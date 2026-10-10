package rds

import (
	"regexp"
	"strconv"
)

// RDS MySQL/MariaDB rotate the general, slow-query and error-running logs
// hourly by renaming the live file to "<base>.YYYY-MM-DD.H" and starting a
// fresh one under the base name. Markers for these files look like
// "YYYY-MM-DD.H:<byte offset>", where the hour ID names the hour file the
// offset belongs to. Behaviour verified against live RDS MySQL 8.4:
//
//   - On the base name, the marker's hour ID selects the file: a marker for a
//     just-rotated hour keeps reading the rotated file, then chains to the live
//     file — but only across ONE rotation. After two or more rotations the
//     marker sticks at the end of the old hour (pending=false, no data) forever.
//   - A marker whose hour file has been purged silently reads the live file at
//     that byte offset.
//   - On a rotated name, data always comes from that file; the hour ID in the
//     marker is ignored and the offset honoured.
//
// So rotation is handled explicitly (see pipeline.InstanceWorker) rather than
// by trusting the marker chain.

var (
	rotatedName = regexp.MustCompile(`^(.+\.log)\.(\d{4}-\d{2}-\d{2})\.(\d{1,2})$`)
	hourMarker  = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2})\.(\d{1,2}):(\d+)$`)
)

// HourID identifies one hourly MySQL log file: a date plus RDS's hour index.
type HourID struct {
	Date string // YYYY-MM-DD
	Hour int
}

func (h HourID) String() string { return h.Date + "." + strconv.Itoa(h.Hour) }

// Compare orders hour IDs chronologically: -1, 0 or +1.
func (h HourID) Compare(o HourID) int {
	switch {
	case h.Date < o.Date:
		return -1
	case h.Date > o.Date:
		return 1
	case h.Hour < o.Hour:
		return -1
	case h.Hour > o.Hour:
		return 1
	}
	return 0
}

// SplitRotated reports whether name is an hourly-rotated MySQL log file
// ("general/mysql-general.log.2026-10-10.7") and returns its base name and hour.
func SplitRotated(name string) (base string, id HourID, ok bool) {
	m := rotatedName.FindStringSubmatch(name)
	if m == nil {
		return "", HourID{}, false
	}
	h, err := strconv.Atoi(m[3])
	if err != nil {
		return "", HourID{}, false
	}
	return m[1], HourID{Date: m[2], Hour: h}, true
}

// ParseHourMarker decodes a MySQL-style "YYYY-MM-DD.H:offset" marker.
func ParseHourMarker(marker string) (id HourID, offset int64, ok bool) {
	m := hourMarker.FindStringSubmatch(marker)
	if m == nil {
		return HourID{}, 0, false
	}
	h, err1 := strconv.Atoi(m[2])
	off, err2 := strconv.ParseInt(m[3], 10, 64)
	if err1 != nil || err2 != nil {
		return HourID{}, 0, false
	}
	return HourID{Date: m[1], Hour: h}, off, true
}
