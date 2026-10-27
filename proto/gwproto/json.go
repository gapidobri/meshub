package gwproto

import (
	"strconv"
	"time"
)

const hexChars = "0123456789ABCDEF"

func appendHex(dst, src []byte) []byte {
	for _, b := range src {
		dst = append(dst, hexChars[b>>4], hexChars[b&0x0F])
	}
	return dst
}

// appendString appends s as a JSON string literal.
func appendString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"' || c == '\\':
			dst = append(dst, '\\', c)
		case c == '\n':
			dst = append(dst, '\\', 'n')
		case c == '\r':
			dst = append(dst, '\\', 'r')
		case c == '\t':
			dst = append(dst, '\\', 't')
		case c < 0x20:
			dst = append(dst, '\\', 'u', '0', '0', hexChars[c>>4], hexChars[c&0x0F])
		default:
			dst = append(dst, c)
		}
	}
	return append(dst, '"')
}

// appendKey appends `,"key":` (without the comma for the first key).
func appendKey(dst []byte, key string) []byte {
	if len(dst) > 0 && dst[len(dst)-1] != '{' {
		dst = append(dst, ',')
	}
	dst = append(dst, '"')
	dst = append(dst, key...)
	return append(dst, '"', ':')
}

func appendStrField(dst []byte, key, val string) []byte {
	return appendString(appendKey(dst, key), val)
}

// appendIntStrField appends an integer as a JSON string, which is how
// meshcoretomqtt encodes the numeric packet fields.
func appendIntStrField(dst []byte, key string, v int64) []byte {
	dst = append(appendKey(dst, key), '"')
	dst = strconv.AppendInt(dst, v, 10)
	return append(dst, '"')
}

func appendIntField(dst []byte, key string, v int64) []byte {
	return strconv.AppendInt(appendKey(dst, key), v, 10)
}

func appendUintField(dst []byte, key string, v uint64) []byte {
	return strconv.AppendUint(appendKey(dst, key), v, 10)
}

// timestampLayout is ISO 8601 with microseconds, like Python's isoformat().
const timestampLayout = "2006-01-02T15:04:05.000000Z07:00"

func appendTimestamp(dst []byte, key string, t time.Time) []byte {
	dst = append(appendKey(dst, key), '"')
	dst = t.UTC().AppendFormat(dst, timestampLayout)
	return append(dst, '"')
}
