//go:build tinygo

package main

import (
	"image/color"
	"machine"
	"strconv"
	"time"

	"github.com/gapidobri/meshub/gateway"
	"tinygo.org/x/drivers/ssd1306"
	"tinygo.org/x/espradio"
	"tinygo.org/x/tinyfont"
	"tinygo.org/x/tinyfont/proggy"
)

// The T3S3's 128x64 SSD1306 OLED and BOOT button.
const (
	pinOLEDSDA = machine.GPIO18
	pinOLEDSCL = machine.GPIO17
	pinButton  = machine.GPIO0 // BOOT, low while pressed
	oledAddr   = 0x3C
)

const (
	displayRefresh = time.Second
	buttonPoll     = 50 * time.Millisecond
	buttonDebounce = 200 * time.Millisecond
	// displayTimeout blanks the panel this long after it was woken, so a
	// gateway left running for months does not burn its status into the
	// OLED. It does not time out while the broker is unreachable.
	displayTimeout = 5 * time.Minute

	// Proggy Tiny is monospaced, 6 pixels wide; its glyphs span 7 pixels above
	// the baseline and 2 below, so 9 pixel rows fit 7 lines on the panel.
	charWidth = 6
	rowHeight = 9
	baseline  = 6
	rowChars  = 128 / charWidth
	rows      = 64 / rowHeight
)

var white = color.RGBA{255, 255, 255, 255}

// display shows the gateway's state on the OLED. Its methods do nothing when
// no panel answered at startup, so the firmware runs on boards without one.
type display struct {
	dev  *ssd1306.Device
	ok   bool
	l, r []byte // line scratch, so redraws do not allocate
}

// screen is the board's display, set up by init.
var screen display

func (d *display) init() {
	i2c := machine.I2C0
	if err := i2c.Configure(machine.I2CConfig{SDA: pinOLEDSDA, SCL: pinOLEDSCL, Frequency: 400 * machine.KHz}); err != nil {
		println("display:", err.Error())
		return
	}
	// The driver ignores bus errors, so probe the panel first.
	if err := i2c.Tx(oledAddr, []byte{0x00, ssd1306.DISPLAYOFF}, nil); err != nil {
		println("display: no panel:", err.Error())
		return
	}
	d.dev = ssd1306.NewI2C(i2c)
	d.dev.Configure(ssd1306.Config{Width: 128, Height: 64, Address: oledAddr})
	pinButton.Configure(machine.PinConfig{Mode: machine.PinInputPullup})
	d.l = make([]byte, 0, rowChars)
	d.r = make([]byte, 0, rowChars)
	d.ok = true
}

// show replaces the screen with lines, one per row, wrapping long ones. It is
// for the boot steps, before run takes over the panel.
func (d *display) show(lines ...string) {
	if !d.ok {
		return
	}
	d.dev.ClearBuffer()
	n := 0
	for _, s := range lines {
		for first := true; (first || s != "") && n < rows; first = false {
			k := min(len(s), rowChars)
			drawText(d, n, 0, k, s)
			s = s[k:]
			n++
		}
	}
	d.dev.Display()
}

// run keeps the status screen up to date and blanks it when idle. The BOOT
// button toggles the screen; losing the broker wakes it. It never returns;
// run it on its own goroutine once the gateway has started.
func (d *display) run(gw *gateway.Gateway, ip string) {
	if !d.ok {
		return
	}
	on := true
	woken := time.Now()
	offlineSince := time.Now() // like RunMQTT, count from the start
	var nextDraw, lastEdge time.Time
	wasPressed := false
	for {
		now := time.Now()
		want := on

		pressed := !pinButton.Get()
		if pressed && !wasPressed && now.Sub(lastEdge) > buttonDebounce {
			want = !on
			lastEdge = now
		}
		wasPressed = pressed

		if gw.Connected() {
			offlineSince = time.Time{}
		} else if offlineSince.IsZero() {
			offlineSince = now
			want = true
		}
		if on && offlineSince.IsZero() && now.Sub(woken) >= displayTimeout {
			want = false
		}

		if want != on {
			d.dev.Sleep(!want)
			on = want
			if on {
				woken, nextDraw = now, now
			}
		}
		if on && !now.Before(nextDraw) {
			d.drawStatus(gw, ip, offlineSince, now)
			nextDraw = now.Add(displayRefresh)
		}
		time.Sleep(buttonPoll)
	}
}

// drawStatus draws the status screen:
//
//	name            uptime
//	IP           WiFi RSSI
//	MQTT ok | down, reset in
//	radio settings  repeat
//	RX/TX counts   dropped
//	last RSSI SNR      age
//	noise floor  TX airtime
func (d *display) drawStatus(gw *gateway.Gateway, ip string, offlineSince, now time.Time) {
	st, act := gw.Stats(), gw.Activity()
	d.dev.ClearBuffer()
	l, r := d.l[:0], d.r[:0]

	row(d, 0, config.Name, appendDuration(r, time.Duration(st.UptimeSecs)*time.Second))

	if rssi, err := espradio.RSSI(); err == nil {
		r = append(strconv.AppendInt(d.r[:0], int64(rssi), 10), "dBm"...)
	} else {
		r = append(d.r[:0], "no WiFi"...)
	}
	row(d, 1, ip, r)

	if offlineSince.IsZero() {
		r = d.r[:0]
		if st.MQTTReconnects > 0 {
			r = strconv.AppendUint(append(r, "rc "...), uint64(st.MQTTReconnects), 10)
		}
		row(d, 2, "MQTT ok", r)
	} else {
		left := max(offlineReset-now.Sub(offlineSince), 0)
		row(d, 2, "MQTT down", appendDuration(append(d.r[:0], "reset "...), left))
	}

	r = d.r[:0]
	if config.Repeat {
		r = append(r, "rpt"...)
	}
	l = strconv.AppendFloat(d.l[:0], float64(config.FrequencyHz)/1e6, 'f', -1, 64)
	l = strconv.AppendUint(append(l, " SF"...), uint64(config.SF), 10)
	l = strconv.AppendFloat(append(l, " BW"...), float64(config.BandwidthKHz), 'f', -1, 32)
	row(d, 3, l, r)

	l = appendCount(append(d.l[:0], "RX "...), act.RxPackets)
	l = appendCount(append(l, " TX "...), act.TxPackets)
	r = d.r[:0]
	if st.UplinkDropped > 0 {
		r = appendCount(append(r, "drop "...), st.UplinkDropped)
	}
	row(d, 4, l, r)

	if act.LastRx.IsZero() {
		row(d, 5, "no packets yet", "")
	} else {
		l = strconv.AppendInt(d.l[:0], int64(act.LastRSSI), 10)
		l = strconv.AppendFloat(append(l, "dBm "...), float64(act.LastSNR), 'f', 1, 32)
		row(d, 5, append(l, "dB"...), appendDuration(d.r[:0], now.Sub(act.LastRx)))
	}

	l = strconv.AppendInt(append(d.l[:0], "NF "...), int64(st.NoiseFloor), 10)
	r = d.r[:0]
	if st.UptimeSecs > 0 {
		r = strconv.AppendFloat(append(r, "tx "...), float64(st.TxAirSecs)*100/float64(st.UptimeSecs), 'f', 1, 64)
		if config.DutyCycle > 0 {
			r = strconv.AppendFloat(append(r, "%/"...), float64(config.DutyCycle)*100, 'f', -1, 32)
		}
		r = append(r, '%')
	}
	row(d, 6, l, r)

	if err := d.dev.Display(); err != nil {
		println("display:", err.Error())
	}
}

// row draws left at the start of text row n and right against its end,
// cutting left short if both do not fit.
func row[L, R string | []byte](d *display, n int, left L, right R) {
	col := rowChars - len(right)
	drawText(d, n, col, len(right), right)
	if len(right) > 0 {
		col-- // keep a space between them
	}
	drawText(d, n, 0, min(len(left), col), left)
}

// drawText draws the first k characters of s at column col of text row n.
func drawText[T string | []byte](d *display, n, col, k int, s T) {
	x, y := int16(col*charWidth), int16(baseline+n*rowHeight)
	for i := 0; i < k; i++ {
		tinyfont.DrawChar(d.dev, &proggy.TinySZ8pt7b, x, y, rune(s[i]), white)
		x += charWidth
	}
}

// appendCount appends n, in thousands once it needs more than four digits.
func appendCount(b []byte, n uint32) []byte {
	if n < 10000 {
		return strconv.AppendUint(b, uint64(n), 10)
	}
	return append(strconv.AppendUint(b, uint64(n/1000), 10), 'k')
}

// appendDuration appends d in at most two units: 42s, 5m, 3h07m, 2d04h.
func appendDuration(b []byte, d time.Duration) []byte {
	s := int64(d / time.Second)
	switch {
	case s < 60:
		return append(strconv.AppendInt(b, s, 10), 's')
	case s < 3600:
		return append(strconv.AppendInt(b, s/60, 10), 'm')
	case s < 86400:
		b = append(strconv.AppendInt(b, s/3600, 10), 'h')
		return append(appendTwoDigits(b, s/60%60), 'm')
	default:
		b = append(strconv.AppendInt(b, s/86400, 10), 'd')
		return append(appendTwoDigits(b, s/3600%24), 'h')
	}
}

func appendTwoDigits(b []byte, v int64) []byte {
	if v < 10 {
		b = append(b, '0')
	}
	return strconv.AppendInt(b, v, 10)
}
