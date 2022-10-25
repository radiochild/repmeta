package repmeta

import (
	"fmt"
	"strconv"
	"strings"
)

type DataRow []*DataVal

func NewDataRow(spec *ReportSpec) (*DataRow, error) {
	var dbRow DataRow
	colNames := ColSpecFldNames(spec.Columns)
	allCols := append(spec.ExtraColumns, colNames...)
	for _, column := range allCols {
		_, pFld := spec.ColumnNamed(column)
		if pFld == nil {
			erx := fmt.Errorf("Unable to scan column named %q", column)
			return nil, erx
		}
		colType := ToDataValType(pFld.FldType)
		dv := NewDVNone()
		switch colType {
		case DVText:
			dv.ToText()
		case DVBoolean:
			dv.ToBool()
		case DVInt:
			dv.ToInt()
		case DVFloat:
			dv.ToFloat()
		case DVCurrency:
			dv.ToCurrency()
		case DVDate:
			dv.ToDate()
		case DVNone:
		} // switch
		dbRow = append(dbRow, dv)
	} // for column

	return &dbRow, nil
}

// reset all values in the row
func (dR *DataRow) ResetNumerics() {
	for _, ptr := range *dR {
		ptr.ResetNumerics()
	}
}

// reset all values in the row
func (dR *DataRow) ResetAll() {
	for _, ptr := range *dR {
		ptr.ResetAll()
	}
}

// need the index within dR to fetch.
func (dR DataRow) ValueAtIndex(fldIdx int) string {
	if fldIdx < 0 {
		return ""
	}
	return dR[fldIdx].String()
}

func (dR DataRow) GetPointers() []interface{} {
	var allPtrs []interface{}
	for _, ptr := range dR {
		allPtrs = append(allPtrs, ptr.GetPointer())
	}
	return allPtrs
}

func (dR DataRow) TabString() string {
	var sb strings.Builder
	for idx, ptr := range dR {
		if idx > 0 {
			sb.WriteString("\t")
		}
		s := ptr.String()
		sb.WriteString(s)
	}
	return sb.String()
}

func (dR DataRow) String() string {
	var sb strings.Builder
  var s string
	for idx, ptr := range dR {
		if idx > 0 {
			sb.WriteString(", ")
		}
		s = ptr.String()
		if ptr.Typ == DVText || ptr.Typ ==DVDate {
			sb.WriteString(strconv.Quote(s))
    } else {
      sb.WriteString(s)
    }
	}
	return sb.String()
}

func (dR DataRow) ShowAll(hdr string) {
	fmt.Println(hdr)
	for _, ptr := range dR {
		fmt.Printf("%s %q %p\n", ptr.FromDataVal(), ptr.String(), &ptr.Ptr)
	}
}

func (dR DataRow) AllValues() []string {
	var allVals []string
	for _, pV := range dR {
		allVals = append(allVals, pV.String())
	}

	return allVals
}
