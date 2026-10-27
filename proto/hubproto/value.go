package hubproto

import "strings"

// Kind is the type of a Value.
type Kind uint8

const (
	KindNull Kind = iota
	KindBool
	KindInt
	KindFloat
	KindString
	KindList
	KindMap
)

// Value is a JSON-like value: null, bool, int, float, string, list, or a map
// with string keys. It carries node configuration and event data. The zero
// Value is null.
type Value struct {
	kind  Kind
	b     bool
	i     int64
	f     float64
	s     string
	items []Value  // list items, or map values
	keys  []string // map keys, parallel to items
}

func Null() Value               { return Value{} }
func Bool(b bool) Value         { return Value{kind: KindBool, b: b} }
func Int(i int64) Value         { return Value{kind: KindInt, i: i} }
func Float(f float64) Value     { return Value{kind: KindFloat, f: f} }
func String(s string) Value     { return Value{kind: KindString, s: s} }
func List(items ...Value) Value { return Value{kind: KindList, items: items} }
func Map() Value                { return Value{kind: KindMap} }
func (v Value) Kind() Kind      { return v.kind }
func (v Value) IsNull() bool    { return v.kind == KindNull }

// Set sets key in a map, replacing an existing entry. It turns a non-map
// value into a map first.
func (v *Value) Set(key string, val Value) {
	if v.kind != KindMap {
		*v = Map()
	}
	for i, k := range v.keys {
		if k == key {
			v.items[i] = val
			return
		}
	}
	v.keys = append(v.keys, key)
	v.items = append(v.items, val)
}

// Field returns the value under key in a map.
func (v Value) Field(key string) (Value, bool) {
	if v.kind != KindMap {
		return Value{}, false
	}
	for i, k := range v.keys {
		if k == key {
			return v.items[i], true
		}
	}
	return Value{}, false
}

// Get follows a dot-separated path of map keys, e.g. "audio.threshold". It
// returns null if any part is missing.
func (v Value) Get(path string) Value {
	for path != "" {
		key, rest, _ := strings.Cut(path, ".")
		var ok bool
		if v, ok = v.Field(key); !ok {
			return Value{}
		}
		path = rest
	}
	return v
}

// Len returns the number of list items or map entries.
func (v Value) Len() int { return len(v.items) }

// Index returns a list item or map value by position.
func (v Value) Index(i int) Value { return v.items[i] }

// Key returns a map key by position.
func (v Value) Key(i int) string { return v.keys[i] }

// Int returns v as an integer, or def if it is not a number. Floats are
// truncated.
func (v Value) Int(def int64) int64 {
	switch v.kind {
	case KindInt:
		return v.i
	case KindFloat:
		return int64(v.f)
	}
	return def
}

// Float returns v as a float, or def if it is not a number.
func (v Value) Float(def float64) float64 {
	switch v.kind {
	case KindFloat:
		return v.f
	case KindInt:
		return float64(v.i)
	}
	return def
}

// Bool returns v as a bool, or def if it is not one.
func (v Value) Bool(def bool) bool {
	if v.kind == KindBool {
		return v.b
	}
	return def
}

// String returns v as a string, or def if it is not one.
func (v Value) String(def string) string {
	if v.kind == KindString {
		return v.s
	}
	return def
}

// Equal reports whether v and o hold the same value. Map order does not
// matter, and an int equals a float of the same value.
func (v Value) Equal(o Value) bool {
	if (v.kind == KindInt || v.kind == KindFloat) && (o.kind == KindInt || o.kind == KindFloat) {
		if v.kind == KindInt && o.kind == KindInt {
			return v.i == o.i
		}
		return v.Float(0) == o.Float(0)
	}
	if v.kind != o.kind || len(v.items) != len(o.items) {
		return false
	}
	switch v.kind {
	case KindBool:
		return v.b == o.b
	case KindString:
		return v.s == o.s
	case KindList:
		for i := range v.items {
			if !v.items[i].Equal(o.items[i]) {
				return false
			}
		}
	case KindMap:
		for i, k := range v.keys {
			ov, ok := o.Field(k)
			if !ok || !v.items[i].Equal(ov) {
				return false
			}
		}
	}
	return true
}

// FromAny converts the result of decoding JSON with encoding/json (nil,
// bool, float64, json.Number via its String method, string, []any,
// map[string]any) into a Value. Numbers that are whole become ints.
func FromAny(x any) (Value, bool) {
	switch x := x.(type) {
	case nil:
		return Null(), true
	case bool:
		return Bool(x), true
	case float64:
		if x == float64(int64(x)) && x >= -1<<53 && x <= 1<<53 {
			return Int(int64(x)), true
		}
		return Float(x), true
	case int:
		return Int(int64(x)), true
	case int64:
		return Int(x), true
	case interface{ Int64() (int64, error) }: // json.Number
		if i, err := x.Int64(); err == nil {
			return Int(i), true
		}
		if f, ok := x.(interface{ Float64() (float64, error) }); ok {
			if v, err := f.Float64(); err == nil {
				return Float(v), true
			}
		}
		return Value{}, false
	case string:
		return String(x), true
	case []any:
		items := make([]Value, len(x))
		for i, e := range x {
			var ok bool
			if items[i], ok = FromAny(e); !ok {
				return Value{}, false
			}
		}
		return List(items...), true
	case map[string]any:
		m := Map()
		for _, k := range sortedKeys(x) {
			e, ok := FromAny(x[k])
			if !ok {
				return Value{}, false
			}
			m.Set(k, e)
		}
		return m, true
	}
	return Value{}, false
}

// Any converts v to nil, bool, int64, float64, string, []any or
// map[string]any, for encoding/json.
func (v Value) Any() any {
	switch v.kind {
	case KindBool:
		return v.b
	case KindInt:
		return v.i
	case KindFloat:
		return v.f
	case KindString:
		return v.s
	case KindList:
		out := make([]any, len(v.items))
		for i, e := range v.items {
			out[i] = e.Any()
		}
		return out
	case KindMap:
		out := make(map[string]any, len(v.items))
		for i, k := range v.keys {
			out[k] = v.items[i].Any()
		}
		return out
	}
	return nil
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sortStrings(keys)
	return keys
}

// sortStrings is an insertion sort; maps here are small, and it avoids
// pulling package sort into firmware.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
