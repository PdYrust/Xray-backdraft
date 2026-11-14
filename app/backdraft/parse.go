package backdraft

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/xtls/xray-core/common/errors"
)

var volumeUnits = map[byte]uint64{
	'b': 1,
	'k': 1 << 10,
	'm': 1 << 20,
	'g': 1 << 30,
	't': 1 << 40,
	'p': 1 << 50,
}

// ParseVolume converts a human readable byte size such as "500GB", "512KiB" or
// "1024" into a number of bytes. All unit prefixes are binary (KB == KiB).
func ParseVolume(v string) (int64, error) {
	s := strings.TrimSpace(strings.ToLower(v))
	if s == "" {
		return 0, errors.New("empty volume")
	}

	split := len(s)
	for split > 0 && unicode.IsLetter(rune(s[split-1])) {
		split--
	}
	number, unit := strings.TrimSpace(s[:split]), strings.TrimSpace(s[split:])
	if number == "" {
		return 0, errors.New("no number in volume: ", v)
	}

	// Accept, but ignore, the "b"/"i" of "KiB"/"MiB"/... notation.
	if strings.HasSuffix(unit, "b") && len(unit) > 1 {
		unit = unit[:len(unit)-1]
	}
	if strings.HasSuffix(unit, "i") && len(unit) > 1 {
		unit = unit[:len(unit)-1]
	}
	if len(unit) > 1 {
		return 0, errors.New("unknown unit in volume: ", v)
	}

	n, err := strconv.ParseInt(number, 10, 64)
	if err != nil {
		return 0, errors.New("invalid number in volume: ", v).Base(err)
	}
	if n < 0 {
		return 0, errors.New("negative volume: ", v)
	}

	multiplier := uint64(1)
	if len(unit) == 1 {
		multiplier = volumeUnits[unit[0]]
		if multiplier == 0 {
			return 0, errors.New("unknown unit in volume: ", v)
		}
	}
	bytes := uint64(n) * multiplier
	if bytes > uint64(1)<<62 {
		return 0, errors.New("volume out of range: ", v)
	}
	return int64(bytes), nil
}

// formatVolume renders a byte count with a binary unit, for logs and reports.
func formatVolume(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1fGB", float64(n)/float64(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(n)/float64(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fKB", float64(n)/float64(1<<10))
	default:
		return strconv.FormatInt(n, 10) + "B"
	}
}
