package logsgoql

import (
	"fmt"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/google/go-cmp/cmp"
)

func TestParseQuery(t *testing.T) {
	tests := []struct {
		query          string
		expectedFilter LogFilter
		expectError    bool
	}{
		{
			query: "level=error | service=auth",
			expectedFilter: LogFilter{
				Or: true,
				LHS: &LogFilter{
					Level: "error",
				},
				RHS: &LogFilter{
					Service: "auth",
				},
			},
			expectError: false,
		},
		{
			query: "level=info & service=auth",
			expectedFilter: LogFilter{
				Or: false,
				LHS: &LogFilter{
					Level: "info",
				},
				RHS: &LogFilter{
					Service: "auth",
				},
			},
			expectError: false,
		},
		{
			query: "level=\"error\" | service=\"auth\"",
			expectedFilter: LogFilter{
				Or: true,
				LHS: &LogFilter{
					Level: "error",
				},
				RHS: &LogFilter{
					Service: "auth",
				},
			},
			expectError: false,
		},
		{
			query:          "level=error | service=auth | extra=field",
			expectedFilter: LogFilter{},
			expectError:    true, // More than two filters in OR condition not supported
		},
		{
			query:          "level=error & service=auth & extra=field",
			expectedFilter: LogFilter{},
			expectError:    true, // More than two filters in AND condition not supported
		},
		{
			query: `service="auth"`,
			expectedFilter: LogFilter{
				Or:      false,
				Service: "auth",
			},
			expectError: false,
		},
		{
			query: `level="warn"`,
			expectedFilter: LogFilter{
				Or:    false,
				Level: "warn",
			},
			expectError: false,
		},
		{
			query: `level       ="info"`,
			expectedFilter: LogFilter{
				Or:    false,
				Level: "info",
			},
			expectError: false,
		},
		{
			query: `service=cart&service=auth`,
			expectedFilter: LogFilter{
				Or: false,
				LHS: &LogFilter{
					Service: "cart",
				},
				RHS: &LogFilter{
					Service: "auth",
				},
			},
			expectError: false,
		},
		{
			query: `level=info|service=auth&service=auth`,
			expectedFilter: LogFilter{
				Or: true,
				LHS: &LogFilter{
					Level: "info",
				},
				RHS: &LogFilter{
					LHS: &LogFilter{
						Service: "auth",
					},
					RHS: &LogFilter{
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
