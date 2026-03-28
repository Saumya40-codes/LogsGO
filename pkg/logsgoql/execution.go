package logsgoql

import "sort"

type Executor struct {
	engine   Engine
	queryCtx *QueryContext
}

func NewExecutor(e Engine, queryCtx *QueryContext) *Executor {
	return &Executor{
		engine:   e,
		queryCtx: queryCtx,
	}
}

func (e *Executor) Execute(expr Expr) ([]Series, error) {
	var res []Series

	for _, s := range e.engine.stores {
		series, err := s.Series(e.queryCtx, expr)
		if err != nil {
			return nil, err
		}
		res = append(res, series...)
	}

	return e.deduplicateSeries(res), nil
}

func (e *Executor) deduplicateSeries(series []Series) []Series {
	m := make(map[string]Series)

	for _, s := range series {
		key := s.Service + "|" + s.Level + "|" + s.Message

		if val, ok := m[key]; ok {
			val.Points = mergePoints(val.Points, s.Points)
			m[key] = val
		} else {
			m[key] = Series{
				Service: s.Service,
				Level:   s.Level,
				Message: s.Message,
				Points:  s.Points,
			}
		}
	}

	var res []Series
	for _, v := range m {
		res = append(res, v)
	}

	return res
}

func (e *Executor) ExecuteQuery(expr Expr) ([]Series, error) {
	series, err := e.Execute(expr)
	if err != nil {
		return nil, err
	}

	if e.queryCtx.EndTs == e.queryCtx.StartTs {
		return e.evalInstant(series)
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

func mergePoints(a, b []Sample) []Sample {
	res := make([]Sample, 0, len(a)+len(b))

	i, j := 0, 0

	for i < len(a) && j < len(b) {
		if a[i].Timestamp <= b[j].Timestamp {
			res = append(res, a[i])
			i++
		} else {
			res = append(res, b[j])
			j++
		}
	}

	res = append(res, a[i:]...)
	res = append(res, b[j:]...)

	return res
}
