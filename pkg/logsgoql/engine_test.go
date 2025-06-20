package logsgoql

import (
	"fmt"
	"testing"

	"github.com/Saumya40-codes/LogsGO/pkg/store"
	"github.com/efficientgo/core/testutil"
	"github.com/google/go-cmp/cmp"
)

func TestParseQuery(t *testing.T) {
	tests := []struct {
		query          string
		expectedFilter store.LogFilter
		expectError    bool
	}{
		{
			query: "level=error | service=auth",
			expectedFilter: store.LogFilter{
				Or: true,
				LHS: &store.LogFilter{
					Level: "error",
				},
				RHS: &store.LogFilter{
					Service: "auth",
				},
			},
			expectError: false,
		},
		{
			query: "level=info & service=auth",
			expectedFilter: store.LogFilter{
				Or: false,
				LHS: &store.LogFilter{
					Level: "info",
				},
				RHS: &store.LogFilter{
					Service: "auth",
				},
			},
			expectError: false,
		},
		{
			query: "level=\"error\" | service=\"auth\"",
			expectedFilter: store.LogFilter{
				Or: true,
				LHS: &store.LogFilter{
					Level: "error",
				},
				RHS: &store.LogFilter{
					Service: "auth",
				},
			},
			expectError: false,
		},
		{
			query:          "level=error | service=auth | extra=field",
			expectedFilter: store.LogFilter{},
			expectError:    true, // More than two filters in OR condition not supported
		},
		{
			query:          "level=error & service=auth & extra=field",
			expectedFilter: store.LogFilter{},
			expectError:    true, // More than two filters in AND condition not supported
		},
		{
			query: `service="auth"`,
			expectedFilter: store.LogFilter{
				Or:      false,
				Service: "auth",
			},
			expectError: false,
		},
		{
			query: `level="warn"`,
			expectedFilter: store.LogFilter{
				Or:    false,
				Level: "warn",
			},
			expectError: false,
		},
		{
			query: `level       ="info"`,
			expectedFilter: store.LogFilter{
				Or:    false,
				Level: "info",
			},
			expectError: false,
		},
		{
			query: `service=cart&service=auth`,
			expectedFilter: store.LogFilter{
				Or: false,
				LHS: &store.LogFilter{
					Service: "cart",
				},
				RHS: &store.LogFilter{
					Service: "auth",
				},
			},
			expectError: false,
		},
		{
			query: `level=info|service=auth&service=auth`,
			expectedFilter: store.LogFilter{
				Or: true,
				LHS: &store.LogFilter{
					Level: "info",
				},
				RHS: &store.LogFilter{
					LHS: &store.LogFilter{
						Service: "auth",
					},
					RHS: &store.LogFilter{
						Service: "auth",
					},
					Or: false,
				},
			},
			expectError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			op, err := ParseQuery(test.query)
			fmt.Println("Parsed filter:", op)
			fmt.Println("Expected filter:", test.expectedFilter)
			testutil.Assert(t, err != nil == test.expectError, "expected error: %v, got: %v", test.expectError, err)

			if diff := cmp.Diff(test.expectedFilter, op); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
