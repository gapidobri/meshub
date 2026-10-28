package node

import (
	"bytes"
	"strconv"
	"strings"
	"time"
)

// MaxLogSize is the largest log buffer: its contents, with the command
// result around them, fit hubproto.MaxMessage.
const MaxLogSize = 2000

// logBuffer keeps the most recent log lines, dropping whole lines from the
// front when full. It lives in RAM: it survives sleep but not power loss.
type logBuffer struct {
	buf  []byte
	size int
}

func (l *logBuffer) add(line string) {
	if l.size <= 0 {
		return
	}
	line = strings.TrimRight(line, "\n")
	if len(line) >= l.size {
		line = line[:l.size-1]
	}
	l.buf = append(l.buf, line...)
	l.buf = append(l.buf, '\n')
	if over := len(l.buf) - l.size; over > 0 {
		// Drop the oldest lines, through the newline that ends the overflow.
		// There is one: the buffer ends with a newline.
		i := bytes.IndexByte(l.buf[over-1:], '\n')
		l.buf = append(l.buf[:0], l.buf[over+i:]...)
	}
}

func (l *logBuffer) String() string { return string(l.buf) }

func (l *logBuffer) clear() { l.buf = l.buf[:0] }

// Log adds a line to the node's log buffer, which the hub can fetch with
// the "logs" command. Lines are stamped with the hub's time once a check-in
// has set the clock, and with seconds since boot before that.
func (n *Node) Log(msg string) {
	var stamp string
	if n.clockSet {
		stamp = time.Unix(int64(n.cfg.RTC.CurrentTime()), 0).UTC().Format("01-02 15:04:05")
	} else {
		stamp = "+" + strconv.Itoa(int(time.Since(n.start)/time.Second)) + "s"
	}
	n.logs.add(stamp + " " + msg)
}

// Logs returns the log buffer's contents, oldest line first.
func (n *Node) Logs() string { return n.logs.String() }
