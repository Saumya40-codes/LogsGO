package logsgoql

import (
	"strings"

	"github.com/Saumya40-codes/LogsGO/pkg/store"
)

func ParseQuery(query string) (store.LogFilter, error) {
	// This is a very basic parser, it only supports simple queries like "level=error | service=auth"
	filter := store.LogFilter{}
	query = strings.Replace(query, " ", "", -1)

	if !strings.Contains(query, "|") && !strings.Contains(query, "&") {
		// this means it should just have service= or level= in prefix
		if strings.HasPrefix(query, "service=") {
			filter.Service = strings.TrimPrefix(query, "service=")
		} else if strings.HasPrefix(query, "level=") {
			filter.Level = strings.TrimPrefix(query, "level=")
		} else {
			if !strings.Contains(query, "=") {
				return store.LogFilter{}, store.ErrInvalidQuery
			}
			return store.LogFilter{}, store.ErrInvalidLabel
		}

		Filter(&filter)
		return filter, nil
	}

	// plan -> itterate from left, if "|", "&" is found check if LHS is nil then assign to it, recurse for LHS

	for i := range query {
		if query[i] == '|' || query[i] == '&' {
			if query[i] == '|' {
				filter.Or = true
			}
			filter.LHS = &store.LogFilter{}
			currentOp := query[0:i]
			if strings.HasPrefix(currentOp, "service=") {
				filter.LHS.Service = strings.TrimPrefix(currentOp, "service=") // we do check above
			} else if strings.HasPrefix(currentOp, "level=") {
				filter.LHS.Level = strings.TrimPrefix(currentOp, "level=")
			} else {
				return store.LogFilter{}, store.ErrInvalidLabel
			}
			if i+1 < len(query) {
				rhs, err := ParseQuery(query[i+1:])
				if err != nil {
					return store.LogFilter{}, err
				}
				filter.RHS = &rhs
			}

			break
		}
	}

	Filter(&filter)
	return filter, nil
}

func Filter(filter *store.LogFilter) {
	if filter.Service != "" {
		filter.Service = FilterQuotes(filter.Service)
	}
	if filter.Level != "" {
		filter.Level = FilterQuotes(filter.Level)
	}

	if filter.LHS != nil {
		Filter(filter.LHS)
	}
	if filter.RHS != nil {
		Filter(filter.RHS)
	}
}

func FilterQuotes(s string) string {
	s = strings.Trim(s, `"`)
	return s
}
