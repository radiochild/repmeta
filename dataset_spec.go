package repmeta

import (
	"fmt"
	"strings"
)

type DatasetSpec struct {
	DatasetName string
	DatasetDesc string
	ViewName    string
	Fields      []FieldSpec
}

func (ds *DatasetSpec) String() string {
	var allFields []string
	for _, fld := range ds.Fields {
		allFields = append(allFields, fld.String())
	}

	formattedFields := strings.Join(allFields, "\n")
	return fmt.Sprintf("Dataset %q\n%s\nView: %q\n\nFields:\n%s", ds.DatasetName, ds.DatasetDesc, ds.ViewName, formattedFields)
}

func (ds *DatasetSpec) FieldNamed(name string) (int, *FieldSpec) {
	for idx, fld := range ds.Fields {
		if fld.FldName == name {
			return idx, &fld
		}
	}
	return -1, nil
}
