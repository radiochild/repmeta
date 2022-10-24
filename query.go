package repmeta

import (
	"fmt"
	"strings"

	reptext "github.com/radiochild/utils/text"

	"go.uber.org/zap"
)

func formatWhere(ds *DatasetSpec, filters []FilterSpec, logger *zap.SugaredLogger) string {
	numFilters := len(filters)
	if numFilters < 1 {
		return ""
	}
	if numFilters == 1 {
		term, err := filters[0].WhereTerm(ds, logger)
		if err != nil {
			logger.Warnf("%s", err.Error())
			return ""
		}
		return fmt.Sprintf("where %s", term)
	}

	wrappedFilters := []string{}
	for _, filter := range filters {
		term, err := filter.WhereTerm(ds, logger)
		if err != nil {
			logger.Warnf("%s", err.Error())
			continue
		}
		wrappedFilter := fmt.Sprintf("(%s)", term)
		wrappedFilters = append(wrappedFilters, wrappedFilter)
	}
	numFilters = len(wrappedFilters)
	if numFilters == 0 {
		return ""
	}
	if numFilters == 1 {
		return fmt.Sprintf("where %s", wrappedFilters[0])
	}
	allFilters := strings.Join(wrappedFilters, " and ")
	return fmt.Sprintf("where (%s)", allFilters)
}

func formatOrder(groups []string) string {
	if len(groups) < 1 {
		return ""
	}
	allGroups := strings.Join(groups, ", ")
	return fmt.Sprintf("order by %s", allGroups)
}

func formatOffset(page, maxRecs int) string {
	if page < 0 {
		return ""
	}
	startRec := page * maxRecs
	return fmt.Sprintf("offset %d limit %d", startRec, maxRecs)
}

func FormatQuery(spec *ReportSpec, maxRecs int, logger *zap.SugaredLogger) string {
	page := 0
	if maxRecs < 0 {
		page = -1
	}

	table := spec.Dataset.ViewName
	colNames := ColSpecFldNames(spec.Columns)
	allCols := append(spec.ExtraColumns, colNames...)
	fldList := strings.Join(allCols, ", ")
	where := formatWhere(&spec.Dataset, spec.Filters, logger)
	order := formatOrder(spec.Groups)
	paging := formatOffset(page, maxRecs)
	suffix := reptext.AppendText(where, order, paging)
	qry := fmt.Sprintf("select %s from %s %s", fldList, table, suffix)
	return qry
}
