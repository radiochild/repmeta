package repmeta

import (
	"fmt"
)

// ------------------------------------------------------------
// export type FieldMetaDataDomainModel = {
//   id?: number;
//   dataSetRef?: number;
//   name?: string;
//   colName?: string;
//   description?: string;
//   fldType?: string;
//   colType?: string;
//   canGroup?: boolean;
//   canFilter?: boolean;
//   canCalc?: boolean;
//   defaultHidden?: boolean;
//   isMultiSelect?: boolean;
//   selectValue?: string;
//   selectName?: string;
// };
// ------------------------------------------------------------

type FieldSpec struct {
	FldName       string
	FldType       string
	ColName       string
	CanGroup      bool
	CanCalc       bool
	DefaultHidden bool
	ColType       string
	Description   string
	CanFilter     bool
}

func (fld FieldSpec) String() string {
	return fmt.Sprintf("%s(%s) %q Grp: %t Hidden: %t Calc: %t  Filter: %t", fld.FldName, fld.FldType, fld.ColName, fld.CanGroup, fld.DefaultHidden, fld.CanCalc, fld.CanFilter)
}
