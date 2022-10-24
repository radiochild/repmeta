package repmeta

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	reptext "github.com/radiochild/utils/text"
	"go.uber.org/zap"
)

// ------------------------------------------------------------
// export type NewReportTemplateType = {
//   templateName?: string; // not needed
//   templateDescription?: string; // not needed
//   dataSetObjectId?: string; // not needed
//   dataSetId?: number;  // not needed
//   isStandard?: boolean; // not needed
//   ++ Dataset: {DataSetName, DatasetDesc, ViewName, Fields}
//   columns?: FieldMetaDataDomainModel[]; // prefer a list of FldNames
//   groups?: string[]; // list of FldNames
//   filters?: RowFilterDomainModel[]; // list of filters
// };
// ------------------------------------------------------------

type ColumnSpec struct {
	FldName  string
	CalcType string
}

type ReportSpec struct {
	Dataset      DatasetSpec
	Columns      []ColumnSpec
	ExtraColumns []string
	Groups       []string
	Filters      []FilterSpec
}

func (cs ColumnSpec) String() string {
	fldName := cs.FldName
	if cs.CalcType == "none" {
		return fldName
	}
	return fmt.Sprintf("%s(%s)", fldName, cs.CalcType)
}

func ColSpecFldNames(allColumns []ColumnSpec) []string {
	fldNames := []string{}
	for _, cs := range allColumns {
		fldNames = append(fldNames, cs.FldName)
	}
	return fldNames
}

func ReadReportSpec(filename string) (*ReportSpec, error) {
	file, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var spec ReportSpec

	err2 := json.Unmarshal([]byte(file), &spec)
	if err2 != nil {
		return nil, err2
	}

	// Derive ExtaColumns
	allFldNames := ColSpecFldNames(spec.Columns)
	allCols := reptext.FromStrings(allFldNames)
	extraColumns := []string{}
	for _, group := range spec.Groups {
		if !allCols.Contains(group) {
			extraColumns = append(extraColumns, group)
		}
	}
	spec.ExtraColumns = extraColumns

	return &spec, err2
}

func ShowReportSpec(spec *ReportSpec, logger *zap.SugaredLogger) {
	specLines := strings.Split(spec.Dataset.String(), "\n")
	for _, lx := range specLines {
		logger.Infof("%s", lx)
	}

	logger.Infof("")
	logger.Infof("Extra Columns:")
	logger.Infof("%v", spec.ExtraColumns)

	logger.Infof("")
	logger.Infof("Columns:")
	logger.Infof("%v", spec.Columns)

	logger.Infof("")
	logger.Infof("Groups:")
	logger.Infof("%v", spec.Groups)

	logger.Infof("")
	logger.Infof("Filters:")
	logger.Infof("%s", spec.Filters)
}

func (spec *ReportSpec) ColumnIndex(fld *FieldSpec) int {
	colNames := ColSpecFldNames(spec.Columns)
	allCols := append(spec.ExtraColumns, colNames...)
	for colIdx, colName := range allCols {
		if colName == fld.FldName {
			return colIdx
		}
	}
	return -1
}

// Return the Column number, and the FieldSpec
func (spec *ReportSpec) ColumnNamed(name string) (int, *FieldSpec) {
	fldIdx, pFld := spec.Dataset.FieldNamed(name)
	if fldIdx == -1 {
		return -1, nil
	}
	colIdx := spec.ColumnIndex(pFld)
	if colIdx == -1 {
		return -1, nil
	}
	return colIdx, pFld
}
