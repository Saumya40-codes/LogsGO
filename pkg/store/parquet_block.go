package store

import (
	"bytes"
	json "encoding/json/v2"
	"fmt"
	"io"
	"math"
	"strings"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/pkg/logsgoql"
	"github.com/parquet-go/parquet-go"
)

const (
	blockObjectExt      = ".parquet"
	parquetRowsPerGroup = 4096
	bloomBitsPerValue   = 10
)

type parquetSeriesRow struct {
	Timestamp int64  `parquet:"timestamp,delta,zstd"`
	Service   string `parquet:"service,dict,zstd"`
	Level     string `parquet:"level,dict,zstd"`
	Message   string `parquet:"message,dict,zstd"`
	Count     uint64 `parquet:"count,delta,zstd"`
	Labels    string `parquet:"labels,dict,zstd"`
}

func encodeSeriesParquet(entries []*logapi.Series) ([]byte, error) {
	rows := make([]parquetSeriesRow, 0, len(entries))
	for _, e := range entries {
		if e == nil || e.Entry == nil {
			continue
		}
		labelsJSON, err := encodeLabelsJSON(e.Entry.Labels)
		if err != nil {
			return nil, err
		}
		rows = append(rows, parquetSeriesRow{
			Timestamp: e.Entry.Timestamp,
			Service:   e.Entry.Service,
			Level:     e.Entry.Level,
			Message:   e.Entry.Message,
			Count:     e.Count,
			Labels:    labelsJSON,
		})
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[parquetSeriesRow](&buf,
		parquet.MaxRowsPerRowGroup(parquetRowsPerGroup),
		parquet.Compression(&parquet.Zstd),
		parquet.SortingWriterConfig(
			parquet.SortingColumns(parquet.Ascending("timestamp")),
		),
		parquet.BloomFilters(
			parquet.SplitBlockFilter(bloomBitsPerValue, "level"),
			parquet.SplitBlockFilter(bloomBitsPerValue, "service"),
		),
	)
	if len(rows) > 0 {
		if _, err := w.Write(rows); err != nil {
			_ = w.Close()
			return nil, fmt.Errorf("write parquet rows: %w", err)
		}
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("close parquet writer: %w", err)
	}
	return buf.Bytes(), nil
}

func encodeLabelsJSON(labels map[string]string) (string, error) {
	if len(labels) == 0 {
		return "", nil
	}
	norm := normalizeCustomLabels(labels)
	b, err := json.Marshal(norm)
	if err != nil {
		return "", fmt.Errorf("marshal labels: %w", err)
	}
	return string(b), nil
}

func decodeLabelsJSON(s string) (map[string]string, error) {
	if s == "" {
		return nil, nil
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, fmt.Errorf("unmarshal labels: %w", err)
	}
	return normalizeCustomLabels(m), nil
}

func seriesFromParquetReaderAt(ra io.ReaderAt, size int64, plan *logsgoql.Plan, iterStart, iterEnd int64) ([]*logapi.Series, error) {
	f, err := parquet.OpenFile(ra, size)
	if err != nil {
		return nil, fmt.Errorf("open parquet: %w", err)
	}

	var levelEq []string
	var levelOK bool
	var serviceEq []string
	var serviceOK bool
	if plan != nil {
		levelEq, levelOK = requiredLevelValues(plan.Root)
		serviceEq, serviceOK = requiredServiceValues(plan.Root)
	}

	out := make([]*logapi.Series, 0)
	for _, rg := range f.RowGroups() {
		if !rowGroupOverlapsTime(rg, iterStart, iterEnd) {
			continue
		}
		if levelOK && !rowGroupMayContainStrings(rg, "level", levelEq) {
			continue
		}
		if serviceOK && !rowGroupMayContainStrings(rg, "service", serviceEq) {
			continue
		}

		reader := parquet.NewGenericRowGroupReader[parquetSeriesRow](rg)
		batch := make([]parquetSeriesRow, 256)
		for {
			n, rerr := reader.Read(batch)
			for i := 0; i < n; i++ {
				row := batch[i]
				if row.Timestamp < iterStart || row.Timestamp > iterEnd {
					continue
				}
				labels, lerr := decodeLabelsJSON(row.Labels)
				if lerr != nil {
					_ = reader.Close()
					return nil, lerr
				}
				if plan != nil {
					matched, merr := plan.Match(logsgoql.EntryLabels{
						Service: row.Service,
						Level:   row.Level,
						Message: row.Message,
						Labels:  labels,
					})
					if merr != nil {
						_ = reader.Close()
						return nil, merr
					}
					if !matched {
						continue
					}
				}
				out = append(out, &logapi.Series{
					Entry: &logapi.LogEntry{
						Timestamp: row.Timestamp,
						Service:   row.Service,
						Level:     row.Level,
						Message:   row.Message,
						Labels:    labels,
					},
					Count: row.Count,
				})
			}
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				_ = reader.Close()
				return nil, fmt.Errorf("read parquet rows: %w", rerr)
			}
		}
		if err := reader.Close(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func loadAllSeriesFromParquet(ra io.ReaderAt, size int64) ([]*logapi.Series, error) {
	return seriesFromParquetReaderAt(ra, size, nil, 0, math.MaxInt64)
}

type columnBounds interface {
	Bounds() (min, max parquet.Value, ok bool)
}

func rowGroupOverlapsTime(rg parquet.RowGroup, start, end int64) bool {
	chunks := rg.ColumnChunks()
	idx := columnIndexByName(rg, "timestamp")
	if idx < 0 || idx >= len(chunks) {
		return true
	}
	b, ok := chunks[idx].(columnBounds)
	if !ok {
		return true
	}
	minV, maxV, has := b.Bounds()
	if !has {
		return true
	}
	return !(maxV.Int64() < start || minV.Int64() > end)
}

func rowGroupMayContainStrings(rg parquet.RowGroup, col string, want []string) bool {
	if len(want) == 0 {
		return false
	}
	chunks := rg.ColumnChunks()
	idx := columnIndexByName(rg, col)
	if idx < 0 || idx >= len(chunks) {
		return true
	}

	if bf := chunks[idx].BloomFilter(); bf != nil {
		anyPossible := false
		for _, v := range want {
			ok, err := bf.Check(parquet.ByteArrayValue([]byte(v)))
			if err != nil {
				return true
			}
			if ok {
				anyPossible = true
				break
			}
		}
		if !anyPossible {
			return false
		}
	}

	if b, ok := chunks[idx].(columnBounds); ok && len(want) == 1 {
		minV, maxV, has := b.Bounds()
		if has {
			v := want[0]
			minS := string(minV.ByteArray())
			maxS := string(maxV.ByteArray())
			if v < minS || v > maxS {
				return false
			}
		}
	}
	return true
}

func columnIndexByName(rg parquet.RowGroup, name string) int {
	schema := rg.Schema()
	if schema == nil {
		return -1
	}
	for i, path := range schema.Columns() {
		if len(path) > 0 && path[len(path)-1] == name {
			return i
		}
		if strings.Join(path, ".") == name {
			return i
		}
	}
	return -1
}

func requiredLevelValues(n logsgoql.Node) ([]string, bool) {
	set, ok := requiredFieldSet(n, logsgoql.FieldLevel)
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	return out, true
}

func requiredFieldSet(n logsgoql.Node, field logsgoql.Field) (map[string]struct{}, bool) {
	switch x := n.(type) {
	case *logsgoql.MatchNode:
		if x.Field == field && x.Op == logsgoql.MatchEq {
			return map[string]struct{}{x.Value: {}}, true
		}
		return nil, false
	case *logsgoql.BinaryNode:
		left, leftOK := requiredFieldSet(x.Left, field)
		right, rightOK := requiredFieldSet(x.Right, field)
		switch x.Op {
		case logsgoql.OpAnd:
			switch {
			case leftOK && rightOK:
				out := make(map[string]struct{})
				for v := range left {
					if _, ok := right[v]; ok {
						out[v] = struct{}{}
					}
				}
				return out, true
			case leftOK:
				return left, true
			case rightOK:
				return right, true
			default:
				return nil, false
			}
		case logsgoql.OpOr:
			if !leftOK || !rightOK {
				return nil, false
			}
			out := make(map[string]struct{}, len(left)+len(right))
			for v := range left {
				out[v] = struct{}{}
			}
			for v := range right {
				out[v] = struct{}{}
			}
			return out, true
		default:
			return nil, false
		}
	default:
		return nil, false
	}
}
