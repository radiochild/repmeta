package repmeta

import (
	"fmt"
	"strings"
)

type ReportLevel struct {
	Totals    *DataRow
	FldName   string
	FldSpec   *FieldSpec
	FldIdx    int
	TotCount  int64
	PrevValue string
}

type NumericSet map[DataValType]bool

func Numerics() NumericSet {
	return NumericSet{
		DVInt:      true,
		DVFloat:    true,
		DVCurrency: true,
	}
}

func NewReportLevel(spec *ReportSpec, groupName string) (*ReportLevel, error) {
	// hasField := true
	fldIdx, fldSpec := spec.ColumnNamed(groupName)
	// if fldSpec == nil {
	// 	hasField = false
	// }
	rL := new(ReportLevel)
	totals, err := NewDataRow(spec)
	if err != nil {
		return nil, err
	}
	rL.Totals = totals
	rL.FldName = groupName
	rL.FldIdx = fldIdx
	// if hasField {
	rL.FldSpec = fldSpec
	// }
	return rL, nil
}

func (lvl *ReportLevel) AsText() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("FldName: %q  FldIdx: %d  PrevValue: %q\n", lvl.FldName, lvl.FldIdx, lvl.PrevValue))
	sb.WriteString(lvl.FldSpec.String())
	return sb.String()
}

func (lvl *ReportLevel) DidAccumulate(row *DataRow) bool {
	isNumeric := Numerics()
	didSucceed := true
	for idx, pTotFld := range *lvl.Totals {
		fldType := pTotFld.Typ
		if _, ok := isNumeric[fldType]; ok {
			if !pTotFld.DidAccumulate((*row)[idx]) {
				didSucceed = false
			}
		}
	}
	if didSucceed {
		lvl.TotCount++
	}
	return didSucceed
}

func (lvl *ReportLevel) TabString() string {
	return fmt.Sprintf("%s", lvl.Totals.TabString())
}

func (lvl *ReportLevel) AllTotals() []string {
	allTotals := []string{}
	for _, total := range *lvl.Totals {
		allTotals = append(allTotals, total.String())
	}
	return allTotals
}

func (lvl *ReportLevel) String() string {
	levelName := lvl.PrevValue
	if len(levelName) == 0 {
		levelName = "Grand"
	}
	return fmt.Sprintf("%s Totals: %s", levelName, lvl.Totals)
}

func (lvl *ReportLevel) ResetNumerics() {
	lvl.Totals.ResetNumerics()
	return
}
