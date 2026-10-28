//go:build tinygo

// Command t3s3 is the meshub MQTT gateway firmware for the LilyGo T3S3
// (ESP32-S3 + SX1262). It connects to WiFi, syncs the clock over NTP and
// bridges the radio to an MQTT broker (see package gateway). The OLED shows
// the boot steps and then the gateway's status; it blanks after a few minutes
// and the BOOT button wakes it.
//
// Settings are compiled in from config.go, which is gitignored. Create it
// from the template, fill it in, then flash:
//
//	cp gateway/cmd/t3s3/config.go.example gateway/cmd/t3s3/config.go
//	tinygo flash -target esp32s3-generic -size short -monitor ./gateway/cmd/t3s3
package main

import (
	"context"
	"device/esp"
	"machine"
	"net"
	"runtime"
	"strconv"
	"time"

	meshcore "github.com/gapidobri/meshcore-go"
	"github.com/gapidobri/meshcore-go/sx1262"
	"github.com/gapidobri/meshub/gateway"
	"tinygo.org/x/drivers/netdev"
	nl "tinygo.org/x/drivers/netlink"
	link "tinygo.org/x/espradio/netlink"
)

// settings are the per-gateway build settings; config.go defines the
// config variable holding them.
type settings struct {
	WiFiSSID     string
	WiFiPassword string

	Broker       string // host:port
	MQTTUser     string // empty for anonymous access
	MQTTPassword string

	Name string // human readable gateway name
	IATA string // region code used in topics, e.g. "LJU"

	// IdentitySeed is 64 hex characters. When empty the identity is derived
	// from the WiFi MAC address, which is stable but not secret; set a seed
	// before enabling Repeat, since a repeater's identity signs its adverts.
	IdentitySeed string
	// Repeat makes the gateway forward packets like a MeshCore repeater.
	Repeat bool

	FrequencyHz  uint32
	BandwidthKHz float32
	SF           uint8
	CR           uint8
	TxPowerDBm   int8
	// DutyCycle is the fraction of time the gateway may transmit, as allowed
	// in its band.
	DutyCycle float32
}

// LilyGo T3S3 SX1262 wiring (variants/lilygo_t3s3 in the C++ repo).
const (
	pinLoRaSCK  = machine.GPIO5
	pinLoRaMISO = machine.GPIO3
	pinLoRaMOSI = machine.GPIO6
	pinLoRaNSS  = machine.GPIO7
	pinLoRaRST  = machine.GPIO8
	pinLoRaBusy = machine.GPIO34
	pinLoRaDIO1 = machine.GPIO33
	pinTxLED    = machine.GPIO37
)

const (
	firmwareVersion = "meshub-gw-t3s3/0.1.0"
	poolSize        = 32
	ntpHost         = "pool.ntp.org"
	ntpInterval     = 6 * time.Hour
	offlineReset    = 15 * time.Minute // reboot if the broker stays unreachable this long
)

// hwRNG uses the ESP32-S3 true random number generator.
type hwRNG struct{}

func (hwRNG) Random(dst []byte) {
	for i := 0; i < len(dst); i += 4 {
		v, _ := machine.GetRNG()
		for j := 0; j < 4 && i+j < len(dst); j++ {
			dst[i+j] = byte(v >> (8 * j))
		}
	}
}

// wallClock is an RTCClock backed by the system clock, which NTP keeps set.
type wallClock struct{}

func (wallClock) CurrentTime() uint32 { return uint32(time.Now().Unix()) }

func (wallClock) SetCurrentTime(t uint32) {
	runtime.AdjustTimeOffset(int64(time.Duration(int64(t)-time.Now().Unix()) * time.Second))
}

func (wallClock) Tick() {}

func main() {
	time.Sleep(2 * time.Second) // give the USB console time to attach
	println("meshub gateway", firmwareVersion)

	pinTxLED.Configure(machine.PinConfig{Mode: machine.PinOutput})
	screen.init()
	screen.show(firmwareVersion, config.Name)

	esp := &link.Esplink{}
	netdev.UseNetdev(esp)
	ip := connectWiFi(esp)
	screen.show(firmwareVersion, config.Name, "", "WiFi "+ip, "NTP "+ntpHost)
	syncTime(esp)
	go func() {
		for {
			time.Sleep(ntpInterval)
			syncTime(esp)
		}
	}()

	spi := machine.SPI0
	if err := spi.Configure(machine.SPIConfig{
		Frequency: 8_000_000,
		SCK:       pinLoRaSCK,
		SDO:       pinLoRaMOSI,
		SDI:       pinLoRaMISO,
	}); err != nil {
		println("spi:", err.Error())
	}
	radio := sx1262.New(sx1262.Config{
		SPI:            spi,
		NSS:            pinLoRaNSS,
		Busy:           pinLoRaBusy,
		DIO1:           pinLoRaDIO1,
		Reset:          pinLoRaRST,
		FrequencyHz:    config.FrequencyHz,
		BandwidthKHz:   config.BandwidthKHz,
		SF:             config.SF,
		CR:             config.CR,
		TxPowerDBm:     config.TxPowerDBm,
		TCXOVoltage:    1.8,
		DIO2AsRFSwitch: true,
		RxBoostedGain:  true,
	})

	ms := meshcore.NewSystemMillis()
	gw := gateway.New(gateway.Config{
		Name:            config.Name,
		IATA:            config.IATA,
		Identity:        identity(esp),
		Model:           "LilyGo T3S3",
		FirmwareVersion: firmwareVersion,
		Radio:           radioDesc(),
		Repeat:          config.Repeat,
		DutyCycle:       config.DutyCycle,
		Username:        config.MQTTUser,
		Password:        config.MQTTPassword,
		OnOffline: func(offline time.Duration) {
			if offline > offlineReset {
				println("broker unreachable for", offline.String(), "- resetting")
				reset()
			}
		},
	}, meshcore.Config{
		Radio:   radio,
		Millis:  ms,
		RNG:     hwRNG{},
		RTC:     wallClock{},
		Packets: meshcore.NewStaticPoolPacketManager(poolSize),
		Tables:  &meshcore.SimpleMeshTables{},
	}, poolSize)
	println("gateway", gw.PublicKey())

	gw.Begin()
	go screen.run(gw, ip)
	go gw.RunMQTT(context.Background(), func() (net.Conn, error) {
		return net.Dial("tcp", config.Broker)
	})

	for {
		gw.Loop()
		pinTxLED.Set(!radio.IsInRecvMode())
		time.Sleep(time.Millisecond)
	}
}

// radioDesc describes the modulation like the MeshCore CLI's "get radio".
func radioDesc() string {
	return strconv.FormatFloat(float64(config.FrequencyHz)/1e6, 'f', -1, 64) + "," +
		strconv.FormatFloat(float64(config.BandwidthKHz), 'f', -1, 32) + "," +
		strconv.Itoa(int(config.SF)) + "," + strconv.Itoa(int(config.CR))
}

// reset performs a software system reset; TinyGo has no machine.CPUReset for
// the ESP32-S3.
func reset() {
	esp.RTC_CNTL.SetOPTIONS0_SW_SYS_RST(1)
	for {
	}
}

// connectWiFi blocks until the WiFi connection is up and returns the
// gateway's IP address.
func connectWiFi(esp *link.Esplink) string {
	for backoff := time.Second; ; backoff = min(backoff*2, time.Minute) {
		println("connecting to WiFi", config.WiFiSSID)
		screen.show(firmwareVersion, config.Name, "", "WiFi "+config.WiFiSSID)
		err := esp.NetConnect(&nl.ConnectParams{Ssid: config.WiFiSSID, Passphrase: config.WiFiPassword})
		if err == nil {
			addr, _ := esp.Addr()
			println("WiFi connected,", addr.String())
			return addr.String()
		}
		println("WiFi:", err.Error())
		screen.show(firmwareVersion, config.Name, "", "WiFi "+config.WiFiSSID, err.Error())
		time.Sleep(backoff)
	}
}

func syncTime(esp *link.Esplink) {
	offset, err := esp.NTP(ntpHost, 5*time.Second, 3)
	if err != nil {
		println("NTP:", err.Error())
		return
	}
	runtime.AdjustTimeOffset(int64(offset))
	println("time", time.Now().UTC().String())
}

// identity returns the configured identity, or one derived from the WiFi MAC.
func identity(esp *link.Esplink) meshcore.LocalIdentity {
	var seed [32]byte
	if config.IdentitySeed != "" {
		if !meshcore.FromHex(seed[:], config.IdentitySeed) {
			println("IdentitySeed must be 64 hex characters")
			for {
				time.Sleep(time.Second)
			}
		}
		return meshcore.LocalIdentityFromSeed(seed[:])
	}
	mac, err := esp.GetHardwareAddr()
	if err != nil {
		println("MAC:", err.Error())
	}
	meshcore.SHA256(seed[:], []byte("meshub-gateway"), mac)
	return meshcore.LocalIdentityFromSeed(seed[:])
}
