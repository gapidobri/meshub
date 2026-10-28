//go:build tinygo

package main

import (
	"device/nrf"
	"machine"
	"time"
)

// wakePin latches a rising edge on a GPIO using the nRF52's SENSE and LATCH
// registers. Unlike a GPIOTE interrupt, this draws no current while the CPU
// sleeps, and a pulse shorter than the polling interval is still caught.
// The same SENSE setting also wakes the chip from System OFF.
type wakePin struct {
	port *nrf.GPIO_Type
	bit  uint32
	n    uint8
}

func newWakePin(p machine.Pin) wakePin {
	p.Configure(machine.PinConfig{Mode: machine.PinInputPulldown})
	w := wakePin{port: nrf.P0, n: uint8(p) % 32}
	if p >= 32 {
		w.port = nrf.P1
	}
	w.bit = 1 << w.n
	cnf := &w.port.PIN_CNF[w.n]
	cnf.Set(cnf.Get()&^nrf.GPIO_PIN_CNF_SENSE_Msk | nrf.GPIO_PIN_CNF_SENSE_High<<nrf.GPIO_PIN_CNF_SENSE_Pos)
	w.clear()
	return w
}

// latched reports whether the pin went high since the last clear.
func (w wakePin) latched() bool { return w.port.LATCH.Get()&w.bit != 0 }

func (w wakePin) clear() { w.port.LATCH.Set(w.bit) } // write 1 to clear

// sleepUntil sleeps for d, or until the pin latches if it is armed. It
// returns whether the pin woke it. The CPU idles in System ON between
// checks, with the RTC running for the timer.
func sleepUntil(d time.Duration, w wakePin, armed bool) bool {
	const poll = time.Second
	deadline := time.Now().Add(d)
	for {
		if armed && w.latched() {
			return true
		}
		left := time.Until(deadline)
		if left <= 0 {
			return false
		}
		time.Sleep(min(left, poll))
	}
}

// battery reads the battery voltage through the RAK4631's divider on P0.05
// (AIN3). The ratio is the one the Meshtastic and MeshCore RAK4631 variants
// use; check it against a multimeter.
type battery struct{ adc machine.ADC }

// newBattery enables the ADC, without which reads wait forever, and sets up
// the battery input.
func newBattery() *battery {
	machine.InitADC()
	b := &battery{adc: machine.ADC{Pin: machine.P0_05}}
	b.adc.Configure(machine.ADCConfig{Reference: 3000, Resolution: 12, Samples: 8})
	return b
}

func (b *battery) millivolts() int {
	const divider = 1.73
	raw := b.adc.Get() // scaled to 16 bits
	return int(float32(raw) / 65535 * 3000 * divider)
}

// hwRNG uses the nRF52 true random number generator.
type hwRNG struct{}

func (hwRNG) Random(dst []byte) {
	for i := 0; i < len(dst); i += 4 {
		v, _ := machine.GetRNG()
		for j := 0; j < 4 && i+j < len(dst); j++ {
			dst[i+j] = byte(v >> (8 * j))
		}
	}
}

// wallClock is the RTCClock: the system clock, which keeps running through
// System ON sleep, corrected by the hub's time on every check-in.
type wallClock struct{ offset int64 }

func (c *wallClock) CurrentTime() uint32 { return uint32(time.Now().Unix() + c.offset) }

func (c *wallClock) SetCurrentTime(t uint32) { c.offset = int64(t) - time.Now().Unix() }

func (c *wallClock) Tick() {}
