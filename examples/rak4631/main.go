//go:build tinygo

// Command rak4631 is an example meshub node for the RAK4631 (nRF52840 +
// SX1262), and a starting point for real ones such as the hydrant monitor.
//
// It pairs with the hub, checks in on an interval set by the hub's config,
// and reports an event when its wake pin goes high (a comparator output,
// say). Between check-ins the radio sleeps and the CPU idles. State lives in
// flash, so it survives power loss.
//
// Settings are compiled in from config.go (gitignored):
//
//	cp examples/rak4631/config.go.example examples/rak4631/config.go
//	tinygo flash -target rak4631 ./examples/rak4631
//
// Config keys the hub can set for it:
//
//	interval_s     seconds between check-ins (default settings.Interval)
//	pin_holdoff_s  seconds to ignore the wake pin after it fired (default 60)
package main

import (
	"context"
	"errors"
	"machine"
	"time"

	meshcore "github.com/gapidobri/meshcore-go"
	"github.com/gapidobri/meshcore-go/sx1262"
	"github.com/gapidobri/meshub/node"
	"github.com/gapidobri/meshub/node/flashstore"
	"github.com/gapidobri/meshub/proto/hubproto"
)

// settings are the per-device build settings; config.go defines the config
// variable holding them.
type settings struct {
	// HubPublicKey is the hub's public key, shown on the backend's web UI.
	HubPublicKey string
	Program      string // what this node runs, shown on the hub
	Claim        string // optional code to tell devices apart when approving

	// Interval is the check-in interval until the hub configures one.
	Interval time.Duration
	// WakePin reports an event when it goes high.
	WakePin machine.Pin

	FrequencyHz  uint32
	BandwidthKHz float32
	SF           uint8
	CR           uint8
	TxPowerDBm   int8
}

const (
	firmwareVersion = "meshub-example-rak4631/0.1.0"
	syncTimeout     = time.Minute
	retryPending    = time.Minute     // check again soon while waiting for approval
	retryNoReply    = 5 * time.Minute // the hub or the route is down
	defaultHoldoff  = time.Minute
	storeBlocks     = 2 // erase blocks (4 KiB each) per flash store region
)

// RAK4631 SX1262 wiring (variants/rak4631 in the MeshCore C++ repo).
const (
	pinLoRaPower = machine.LORA_POWER
	pinLED       = machine.LED1 // green
)

func main() {
	time.Sleep(2 * time.Second) // give the USB console time to attach
	println(firmwareVersion)

	pinLED.Configure(machine.PinConfig{Mode: machine.PinOutput})
	pinLoRaPower.Configure(machine.PinConfig{Mode: machine.PinOutput})
	pinLoRaPower.High()
	time.Sleep(10 * time.Millisecond)

	spi := machine.SPI2
	if err := spi.Configure(machine.SPIConfig{
		Frequency: 8_000_000,
		SCK:       machine.LORA_SCK,
		SDO:       machine.LORA_MOSI,
		SDI:       machine.LORA_MISO,
	}); err != nil {
		fail("spi", err)
	}
	radio := sx1262.New(sx1262.Config{
		SPI:            spi,
		NSS:            machine.LORA_NSS,
		Busy:           machine.LORA_BUSY,
		DIO1:           machine.LORA_DIO1,
		Reset:          machine.LORA_NRESET,
		FrequencyHz:    config.FrequencyHz,
		BandwidthKHz:   config.BandwidthKHz,
		SF:             config.SF,
		CR:             config.CR,
		TxPowerDBm:     config.TxPowerDBm,
		TCXOVoltage:    1.8,
		DIO2AsRFSwitch: true,
		RxBoostedGain:  true,
	})

	store, err := flashstore.Open(machine.Flash, storeBlocks)
	if err != nil {
		fail("flash store", err)
	}
	hub, ok := meshcore.IdentityFromHex(config.HubPublicKey)
	if !ok {
		fail("config", errors.New("HubPublicKey must be 64 hex characters"))
	}
	n, err := node.New(node.Config{
		Radio:    radio,
		Millis:   meshcore.NewSystemMillis(),
		RNG:      hwRNG{},
		RTC:      &wallClock{},
		Storage:  store,
		Hub:      hub,
		Program:  config.Program,
		Firmware: firmwareVersion,
		Claim:    config.Claim,
	})
	if err != nil {
		fail("node", err)
	}
	println("node", n.PublicKey())

	wake := newWakePin(config.WakePin)
	bat := newBattery()
	reason := uint8(hubproto.WakePowerOn)
	var holdoffUntil time.Time
	for {
		pinLED.High()
		mv := bat.millivolts()
		println("checking in, battery", mv, "mV")
		n.SetWakeReason(reason)
		n.SetBattery(mv)
		ctx, cancel := context.WithTimeout(context.Background(), syncTimeout)
		err := n.Sync(ctx)
		cancel()
		pinLED.Low()
		radio.Sleep()

		interval := checkInInterval(n, err)
		holdoff := time.Duration(n.Config().Get("pin_holdoff_s").Int(int64(defaultHoldoff/time.Second))) * time.Second
		println("sync:", status(err), "config v", n.ConfigVersion(), "pending events", n.Pending(), "next check-in in", interval.String())

		// Sleep until the check-in is due or the pin fires. For the hold-off
		// after the pin last fired, it is ignored; anything latched while
		// awake still counts.
		due := time.Now().Add(interval)
		reason = hubproto.WakeTimer
		for time.Now().Before(due) {
			if time.Now().Before(holdoffUntil) {
				sleepUntil(time.Until(holdoffUntil), wake, false)
				wake.clear()
				continue
			}
			if sleepUntil(time.Until(due), wake, true) {
				reason = hubproto.WakePin
				holdoffUntil = time.Now().Add(holdoff)
			}
			break
		}
		radio.Begin()

		if reason == hubproto.WakePin {
			println("wake pin fired")
			data := hubproto.Map()
			data.Set("at", hubproto.Int(time.Now().Unix()))
			if err := n.Report("pin", data); err != nil {
				println("report:", err.Error())
			}
		}
	}
}

// checkInInterval returns how long to sleep after a Sync.
func checkInInterval(n *node.Node, err error) time.Duration {
	switch {
	case errors.Is(err, node.ErrPending):
		return retryPending
	case err != nil:
		return retryNoReply
	case n.SleepHint() > 0:
		return n.SleepHint()
	}
	return time.Duration(n.Config().Get("interval_s").Int(int64(config.Interval/time.Second))) * time.Second
}

func status(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}

func fail(what string, err error) {
	for {
		println(what+":", err.Error())
		time.Sleep(5 * time.Second)
	}
}
