package repmeta

import (
	"fmt"
	"time"

	"github.com/jackc/pgtype"
)

type DataValPtr interface{}
type DataValText string
type DataValType int

const (
	DVNone DataValType = iota
	DVText
	DVInt
	DVFloat
	DVCurrency
	DVBoolean
	DVDate
)

type DataVal struct {
	Typ DataValType
	Ptr DataValPtr
}

func NewDVNone() *DataVal {
	dv := DataVal{Typ: DVNone}
	return &dv
}

func NewDVBoolean(v ...bool) *DataVal {
	var val bool
	if len(v) > 0 {
		val = v[0]
	}
	dv := DataVal{Typ: DVBoolean, Ptr: &val}
	return &dv
}

func NewDVText(v ...string) *DataVal {
	var val string
	if len(v) > 0 {
		val = v[0]
	}
	dv := DataVal{Typ: DVText, Ptr: &val}
	return &dv
}

func NewDVInt(v ...int64) *DataVal {
	var val int64
	if len(v) > 0 {
		val = v[0]
	}
	dv := DataVal{Typ: DVInt, Ptr: &val}
	return &dv
}

func NewDVCurrency(v ...int64) *DataVal {
	var val int64
	if len(v) > 0 {
		val = v[0]
	}
	dv := DataVal{Typ: DVInt, Ptr: &val}
	return &dv
}

func NewDVFloat(v ...float64) *DataVal {
	var val float64
	if len(v) > 0 {
		val = v[0]
	}
	dv := DataVal{Typ: DVFloat, Ptr: &val}
	return &dv
}

func NewDVDate(v ...time.Time) *DataVal {
	val := time.Now()
	if len(v) > 0 {
		val = v[0]
	}
	pgVal := new(pgtype.Date)
	pgVal.Time = val
	dv := DataVal{Typ: DVDate, Ptr: pgVal}
	return &dv
}

func (dv *DataVal) ResetAll() {
	switch dv.Typ {
	case DVNone:
		return
	case DVDate:
		dv.ToDate()
	case DVCurrency:
		dv.ToCurrency()
	case DVText:
		dv.ToText()
	case DVInt:
		dv.ToInt()
	case DVFloat:
		dv.ToFloat()
	case DVBoolean:
		dv.ToBool()
	}
	return
}

func (dv *DataVal) ResetNumerics() {
	switch dv.Typ {
	case DVNone:
		return
	case DVDate:
		return
	case DVCurrency:
		dv.ToCurrency()
	case DVText:
		return
	case DVInt:
		dv.ToInt()
	case DVFloat:
		dv.ToFloat()
	case DVBoolean:
		dv.ToBool()
	}
	return
}

func (dv *DataVal) ToNone() {
	dv.Typ = DVNone
	dv.Ptr = nil
}

func (dv *DataVal) ToText(v ...*string) {
	dv.ToNone()
	dv.Typ = DVText
	if len(v) == 0 {
		dv.Ptr = new(string)
	} else {
		dv.Ptr = v[0]
	}
}

func (dv *DataVal) ToInt(v ...*int64) {
	dv.ToNone()
	dv.Typ = DVInt
	if len(v) == 0 {
		dv.Ptr = new(int64)
	} else {
		dv.Ptr = v[0]
	}
}

func (dv *DataVal) ToCurrency(v ...*int64) {
	dv.ToNone()
	dv.Typ = DVCurrency
	if len(v) == 0 {
		dv.Ptr = new(int64)
	} else {
		dv.Ptr = v[0]
	}
}

func (dv *DataVal) ToFloat(v ...*float64) {
	dv.ToNone()
	dv.Typ = DVFloat
	if len(v) == 0 {
		dv.Ptr = new(float64)
	} else {
		dv.Ptr = v[0]
	}
}

func (dv *DataVal) ToBool(v ...*bool) {
	dv.ToNone()
	dv.Typ = DVBoolean
	if len(v) == 0 {
		dv.Ptr = new(bool)
	} else {
		dv.Ptr = v[0]
	}
}

func (dv *DataVal) ToDate(v ...*time.Time) {
	dv.ToNone()
	dv.Typ = DVDate
	pDt := new(pgtype.Date)
	dv.Ptr = pDt
	if len(v) == 0 {
		pDt.Time = time.Time{}
	} else {
		pTime := v[0]
		pDt.Time = *pTime
	}
}

func (dv *DataVal) String() string {
	switch dv.Typ {
	case DVNone:
		return ""
	case DVDate:
		pDt := dv.Ptr.(*pgtype.Date)
		pTime := pDt.Time
		if pTime.IsZero() {
			return ""
		}
		return pTime.Format("2006-01-02")
	case DVCurrency:
		pennies := *dv.Ptr.(*int64)
		dollars := pennies / 100
		cents := pennies % 100
		return fmt.Sprintf("%d.%2.2d", dollars, cents)
	case DVText:
		return *dv.Ptr.(*string)
	case DVInt:
		return fmt.Sprintf("%d", *dv.Ptr.(*int64))
	case DVFloat:
		return fmt.Sprintf("%.2f", *dv.Ptr.(*float64))
	case DVBoolean:
		return fmt.Sprintf("%t", *dv.Ptr.(*bool))
	}
	return ""
}

func ToDataValType(s string) DataValType {
	switch s {
	case "text":
		return DVText
	case "int":
		return DVInt
	case "date":
		return DVDate
	case "currency":
		return DVCurrency
	case "float":
		return DVFloat
	case "boolean":
		return DVBoolean
	}
	return DVNone
}

func (dv *DataVal) FromDataVal() string {
	switch dv.Typ {
	case DVNone:
		return "DVNone"
	case DVDate:
		return "DVDate"
	case DVCurrency:
		return "DVCurrency"
	case DVText:
		return "DVText"
	case DVInt:
		return "DVInt"
	case DVFloat:
		return "DVFloat"
	case DVBoolean:
		return "DVBoolean"
	}
	return ""
}

func (dv *DataVal) GetPointer() DataValPtr {
	return dv.Ptr
}

func (dv *DataVal) DidAccumulate(other *DataVal) bool {
	thisType := dv.Typ
	if thisType != other.Typ {
		return false
	}
	didAccumulate := true
	switch thisType {
	case DVInt, DVCurrency:
		thisVal := *dv.Ptr.(*int64)
		otherVal := *other.Ptr.(*int64)
		*dv.Ptr.(*int64) = thisVal + otherVal
	case DVFloat:
		thisVal := *dv.Ptr.(*float64)
		otherVal := *other.Ptr.(*float64)
		*dv.Ptr.(*float64) = thisVal + otherVal
	default:
		didAccumulate = false
	}
	return didAccumulate
}
