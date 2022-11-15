package tables

import (
	"fmt"
	"strings"
)

// BaseDate is TableExpirationを設定する時に何を基準にして経過日数を出すのかの指定する
//
//go:generate stringer -type=BaseDate
type BaseDate int

const (
	CreationTime BaseDate = iota
	LastModifiedTime
	TableSuffix
)

func ParseBaseDate(v string) (BaseDate, error) {
	switch strings.ToLower(v) {
	case "creationtime":
		return CreationTime, nil
	case "lastmodifiedtime":
		return LastModifiedTime, nil
	case "tablesuffix":
		return TableSuffix, nil
	}
	return 0, fmt.Errorf("invalid values")
}
