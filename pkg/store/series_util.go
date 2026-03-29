package store

import (
	"fmt"

	"github.com/Saumya40-codes/LogsGO/pkg/logsgoql"
)

func seriesIterWindow(queryCtx logsgoql.QueryContext) (start, end int64) {
	start = queryCtx.StartTs
	end = queryCtx.EndTs

	if queryCtx.Lookback > 0 {
		if queryCtx.EndTs == queryCtx.StartTs {
			start = queryCtx.EndTs - queryCtx.Lookback
		} else {
			start = queryCtx.StartTs - queryCtx.Lookback
		}
	}
	if start < 0 {
		start = 0
	}
	return start, end
}

func tieredSeries(queryCtx logsgoql.QueryContext, plan *logsgoql.Plan, resolution int64, next *Store, localRaw func() ([]logsgoql.Series, error)) ([]logsgoql.Series, error) {
	var nextRes []logsgoql.Series
	var nextErr error

	if next != nil {
		done := make(chan struct{})
		go func() {
			defer close(done)
			if resolution > 0 && queryCtx.EndTs != queryCtx.StartTs {
				nextRes, nextErr = (*next).SeriesRange(queryCtx, plan, resolution)
			} else {
				nextRes, nextErr = (*next).Series(queryCtx, plan)
			}
		}()

		local, err := localRaw()
		if err != nil {
			<-done
			return nil, err
		}

		if resolution > 0 && queryCtx.EndTs != queryCtx.StartTs {
			local = logsgoql.EvalRangeAtResolution(local, queryCtx.StartTs, queryCtx.EndTs, queryCtx.Lookback, resolution)
		}

		<-done
		if nextErr != nil {
			return nil, fmt.Errorf("failed to query next store: %w", nextErr)
		}

		return mergeTierSeriesPreferLeft(local, nextRes), nil
	}

	local, err := localRaw()
	if err != nil {
		return nil, err
	}
	if resolution > 0 && queryCtx.EndTs != queryCtx.StartTs {
		local = logsgoql.EvalRangeAtResolution(local, queryCtx.StartTs, queryCtx.EndTs, queryCtx.Lookback, resolution)
	}
	return local, nil
}

func mergeTierSeriesPreferLeft(left, right []logsgoql.Series) []logsgoql.Series {
	out := make(map[string]logsgoql.Series, len(left)+len(right))

	for _, s := range right {
		key := s.Service + "|" + s.Level + "|" + s.Message
		s.Points = compactSamplesByTimestamp(s.Points)
		out[key] = s
	}

	for _, s := range left {
		key := s.Service + "|" + s.Level + "|" + s.Message
		s.Points = compactSamplesByTimestamp(s.Points)

		if existing, ok := out[key]; ok {
			existing.Points = mergeSamplesPreferLeft(s.Points, existing.Points)
			out[key] = existing
			continue
		}
		out[key] = s
	}

	res := make([]logsgoql.Series, 0, len(out))
	for _, v := range out {
		res = append(res, v)
	}
	return res
}

func mergeSamplesPreferLeft(a, b []logsgoql.Sample) []logsgoql.Sample {
	res := make([]logsgoql.Sample, 0, len(a)+len(b))

	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].Timestamp < b[j].Timestamp {
			res = append(res, a[i])
			i++
			continue
		}
		if a[i].Timestamp > b[j].Timestamp {
			res = append(res, b[j])
			j++
			continue
		}

		// Same timestamp: prefer upstream (left).
		res = append(res, a[i])
		i++
		j++
	}

	res = append(res, a[i:]...)
	res = append(res, b[j:]...)
	return res
}

func compactSamplesByTimestamp(pts []logsgoql.Sample) []logsgoql.Sample {
	if len(pts) <= 1 {
		return pts
	}
	out := pts[:0]
	for _, p := range pts {
		if len(out) == 0 || out[len(out)-1].Timestamp != p.Timestamp {
			out = append(out, p)
		}
	}
	return out
}
