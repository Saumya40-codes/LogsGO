package logql

import (
	"strings"

	"github.com/Saumya40-codes/LogsGO/pkg/store"
)

func ParseQuery(query string) (store.LogFilter, error) {
	// This is a very basic parser, it only supports simple queries like "level=error | service=auth"
	filter := store.LogFilter{}
	query = strings.Replace(query, " ", "", -1)

	if strings.Contains(query, "|") {
		parts := strings.Split(query, "|")

		if len(parts) > 2 {
			return filter, store.ErrNotImplemented
		}
		if len(parts) <= 1 {
			return filter, store.ErrInvalidQuery
		}
		for _, part := range parts {
			if strings.HasPrefix(part, "level=") {
				if filter.LHS == nil {
					filter.LHS = &store.LogFilter{}
					filter.LHS.Level = strings.TrimPrefix(part, "level=")
				} else if filter.RHS == nil {
					filter.RHS = &store.LogFilter{}
					filter.RHS.Level = strings.TrimPrefix(part, "level=")
				} else {
					return filter, store.ErrNotImplemented // More than two filters in OR condition not supported
				}
			} else if strings.HasPrefix(part, "service=") {
				if filter.LHS == nil {
					filter.LHS = &store.LogFilter{}
					filter.LHS.Service = strings.TrimPrefix(part, "service=")
				} else if filter.RHS == nil {
					filter.RHS = &store.LogFilter{}
					filter.RHS.Service = strings.TrimPrefix(part, "service=")
				} else {
					return filter, store.ErrNotImplemented // More than two filters in OR condition not supported
				}
			} else {
				return filter, store.ErrInvalidQuery // Invalid part of the query
			}
		}
		filter.Or = true
	} else if strings.Contains(query, "&") {
		parts := strings.Split(query, "&")
		if len(parts) > 2 {
			return filter, store.ErrNotImplemented
		}
		if len(parts) <= 1 {
			return filter, store.ErrInvalidQuery
		}

		for _, part := range parts {
			if strings.HasPrefix(part, "level=") {
				if filter.LHS == nil {
					filter.LHS = &store.LogFilter{}
					filter.LHS.Level = strings.TrimPrefix(part, "level=")
				} else if filter.RHS == nil {
					filter.RHS = &store.LogFilter{}
					filter.RHS.Level = strings.TrimPrefix(part, "level=")
				} else {
					return filter, store.ErrNotImplemented // More than two filters in AND condition not supported
				}
			} else if strings.HasPrefix(part, "service=") {
				if filter.LHS == nil {
					filter.LHS = &store.LogFilter{}
					filter.LHS.Service = strings.TrimPrefix(part, "service=")
				} else if filter.RHS == nil {
					filter.RHS = &store.LogFilter{}
					filter.RHS.Service = strings.TrimPrefix(part, "service=")
				} else {
					return filter, store.ErrNotImplemented // More than two filters in AND condition not supported
				}
			} else {
				return filter, store.ErrInvalidQuery // Invalid part of the query
			}
		}
		filter.Or = false
	} else if strings.HasPrefix(query, "level=") {
		filter.Level = strings.TrimPrefix(query, "level=")
	} else if strings.HasPrefix(query, "service=") {
		filter.Service = strings.TrimPrefix(query, "service=")
	}
	Filter(&filter) // Normalize the filter by removing quotes and trimming spaces
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
