package logsgoql

import "sort"

type Executor struct {
	engine   Engine
	queryCtx QueryContext
}

func NewExecutor(e Engine, queryCtx QueryContext) *Executor {
	return &Executor{
		engine:   e,
		queryCtx: queryCtx,
	}
}

func (e *Executor) Execute(plan *Plan) ([]Series, error) {
	if e.engine.store == nil {
		return nil, ErrInternal
	}

	series, err := e.engine.store.Series(e.queryCtx, plan)
	if err != nil {
		return nil, err
	}

	return series, nil
}

func (e *Executor) ExecuteQuery(plan *Plan) ([]Series, error) {
	series, err := e.Execute(plan)
	if err != nil {
		return nil, err
	}

	if e.queryCtx.EndTs == e.queryCtx.StartTs {
		return e.evalInstant(series)
	}

	return series, nil
}

// ExecuteRangeQuery evaluates a range query at the given resolution.
//
// It fetches raw samples from stores once via Execute(expr), then evaluates an
// "instant-at-step" result for each step in [StartTs, EndTs] using Lookback.
func (e *Executor) ExecuteRangeQuery(plan *Plan, resolution int64) ([]Series, error) {
	if resolution <= 0 || e.queryCtx.EndTs == e.queryCtx.StartTs {
		return e.ExecuteQuery(plan)
	}

	if e.engine.store == nil {
		return nil, ErrInternal
	}

	series, err := e.engine.store.SeriesRange(e.queryCtx, plan, resolution)
	if err != nil {
		return nil, err
	}
	return series, nil
}

func (e *Executor) evalInstant(series []Series) ([]Series, error) {
	instantRes := make([]Series, 0, len(series))

	for _, s := range series {
		res := Series{
			Service: s.Service,
			Level:   s.Level,
			Message: s.Message,
		}
		pts := s.Points
		if len(pts) > 0 {
			last := pts[len(pts)-1]
			if last.Timestamp <= e.queryCtx.EndTs {
				res.Points = []Sample{last}
			} else {
				i := sort.Search(len(pts), func(i int) bool {
					return pts[i].Timestamp > e.queryCtx.EndTs
				})

				if i > 0 {
					res.Points = pts[i-1 : i]
				}
			}
		}

		if len(res.Points) > 0 {
			instantRes = append(instantRes, res)
		}
	}

	return instantRes, nil
}

// TODO: This is a naive implementation that iterates over all raw samples for each step. Need an optimization
func EvalRangeAtResolution(series []Series, startTs, endTs, lookback, resolution int64) []Series {
	if resolution <= 0 {
		return series
	}

	out := make([]Series, 0, len(series))
	for _, s := range series {
		pts := s.Points
		if len(pts) == 0 {
			continue
		}

		j := 0
		evaluated := make([]Sample, 0, 1+((endTs-startTs)/resolution))

		for t := startTs; t <= endTs; t += resolution {
			minT := t - lookback
			if minT < 0 {
				minT = 0
			}

			for j < len(pts) && pts[j].Timestamp <= t {
				j++
			}
			if j > 0 {
				cand := pts[j-1]
				if cand.Timestamp >= minT {
					evaluated = append(evaluated, Sample{
						Timestamp: t,
						Count:     cand.Count,
					})
				}
			}
		}

		if len(evaluated) == 0 {
			continue
		}

		out = append(out, Series{
			Service: s.Service,
			Level:   s.Level,
			Message: s.Message,
			Points:  evaluated,
		})
	}
	return out
}
