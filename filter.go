package repmeta

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
)

// ------------------------------------------------------------
// export type RowFilterDomainModel = {
//   fldName: string; // DataSet fldName
//   op: string; // 'lt' | 'le' | 'gt' | 'ge' 'eq' | 'ne' | 'prefix' | 'suffix' | 'contains' | 'exists'
//   values: string[];
//   options: string[];
// };
// ------------------------------------------------------------

type FilterSpec struct {
	FldName string
	Op      string
	Values  []string
	Options []string
}

func (fs *FilterSpec) String() string {
	allValues := strings.Join(fs.Values, ", ")
	return fmt.Sprintf("%s %q %s", fs.FldName, fs.Op, allValues)
}

func SingleQuote(s string) string {
	escapedStr := strings.ReplaceAll(s, "'", "\\'")
	parts := []string{"'", "'"}
	return strings.Join(parts, escapedStr)
}

func (fs FilterSpec) WhereTerm(ds *DatasetSpec, logger *zap.SugaredLogger) (string, error) {
	if ds == nil {
		return "", fmt.Errorf("Dataset not provided for filter named %q", fs.FldName)
	}
	idx, pFld := ds.FieldNamed(fs.FldName)
	if idx == -1 {
		return "", fmt.Errorf("Filter field named %q not found in dataset %q", fs.FldName, ds.DatasetName)
	}

	parts := []string{}
	shouldNegate := fs.HasOption("not")

	// is there a value expected for this term?
	expectsValue := fs.Op != "exists"

	parts = append(parts, fs.FldName)
	opcode, valFormat := OpCodeSQL(fs.Op, shouldNegate)
	parts = append(parts, opcode)

	compValue := ComparisonVal(fs.Values, valFormat, pFld.FldType, opcode)
	hasValue := compValue != ""
	if hasValue && expectsValue {
		parts = append(parts, compValue)
	}

	if hasValue != expectsValue {
		return "", fmt.Errorf("Value expected for opcode %q: %t  Value provided %t", fs.Op, expectsValue, hasValue)
	}

	term := strings.Join(parts, " ")
	return term, nil
}

func ComparisonVal(values []string, valFormat string, typ string, opcode string) string {
	if strings.HasSuffix(opcode, " null") {
		return ""
	}
	if len(values) < 1 {
		return ""
	}
	needsQuotes := typ == "text" || typ == "date"
	if strings.HasSuffix(opcode, "ilike") {
		return SingleQuote(fmt.Sprintf(valFormat, values[0]))
	}
	if strings.HasSuffix(opcode, "in") {
		allVals := []string{}
		for _, value := range values {
			val := FormatValue(value, needsQuotes)
			allVals = append(allVals, val)
		}
		return fmt.Sprintf("(%s)", strings.Join(allVals, ", "))
	}
	if strings.HasSuffix(opcode, "between") {
		if len(values) != 2 {
			return ""
		}
		allVals := []interface{}{}
		for idx := 0; idx < 2; idx++ {
			val := FormatValue(values[idx], needsQuotes)
			allVals = append(allVals, val)
		}
		return fmt.Sprintf(valFormat, allVals...)
	}
	return FormatValue(values[0], needsQuotes)
}

func FormatValue(v string, needsQuotes bool) string {
	if needsQuotes {
		return SingleQuote(v)
	}
	return v
}

func OpCodeSQL(op string, shouldNegate bool) (string, string) {
	sql := ""
	valFormat := "%s"
	switch op {
	case "lt":
		sql = "<"
		if shouldNegate {
			sql = ">="
		}
	case "gt":
		sql = ">"
		if shouldNegate {
			sql = "<="
		}
	case "le":
		sql = "<="
		if shouldNegate {
			sql = ">"
		}
	case "ge":
		sql = ">="
		if shouldNegate {
			sql = "<"
		}
	case "eq":
		sql = "="
		if shouldNegate {
			sql = "<>"
		}
	case "ne":
		sql = "<>"
		if shouldNegate {
			sql = "="
		}
	case "prefix":
		sql = "ilike"
		if shouldNegate {
			sql = "not ilike"
		}
		valFormat = "%s%%"
	case "suffix":
		sql = "ilike"
		if shouldNegate {
			sql = "not ilike"
		}
		valFormat = "%%%s"
	case "contains":
		sql = "ilike"
		if shouldNegate {
			sql = "not ilike"
		}
		valFormat = "%%%s%%"
	case "exists":
		sql = "is null"
		if shouldNegate {
			sql = "is not null"
		}
	case "range":
		sql = "between"
		if shouldNegate {
			sql = "not between"
		}
		valFormat = "%s and %s"
	case "in":
		sql = "in"
		if shouldNegate {
			sql = "not in"
		}
	}
	return sql, valFormat
}

func (fs *FilterSpec) HasOption(s string) bool {
	for _, opt := range fs.Options {
		if strings.EqualFold(s, opt) {
			return true
		}
	}
	return false
}
