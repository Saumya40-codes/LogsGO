package store

import (
	"fmt"
	"os"
	"time"

	"github.com/Saumya40-codes/LogsGO/pkg"
	"github.com/Saumya40-codes/LogsGO/pkg/logsgoql"
	"gopkg.in/yaml.v3"
)

type cacheConfigFile struct {
	Cache CacheConfig `yaml:"cache"`
}

type CacheConfig struct {
	Enabled    bool     `yaml:"enabled"`
	TTL        string   `yaml:"ttl"`
	MaxEntries int64    `yaml:"max_entries"`
	Rules      []string `yaml:"rules"`
}

// CachePolicy decides which logs the memory cache retains. A nil policy means
// "cache everything" (the degenerate, unconfigured case).
type CachePolicy struct {
	enabled    bool
	ttl        time.Duration
	maxEntries int64
	plan       *logsgoql.Plan // OR of all rule plans; nil ⇒ match everything
}

func LoadCachePolicy(path, inline string) (*CachePolicy, error) {
	var data []byte
	var err error

	switch {
	case path != "":
		data, err = os.ReadFile(path)
		if err != nil {
			return nil, err
		}
	case inline != "":
		data = []byte(inline)
	default:
		return nil, nil
	}

	var cfg cacheConfigFile
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return NewCachePolicy(cfg.Cache)
}

func NewCachePolicy(cfg CacheConfig) (*CachePolicy, error) {
	p := &CachePolicy{
		enabled:    cfg.Enabled,
		maxEntries: cfg.MaxEntries,
	}

	if cfg.TTL != "" {
		if !pkg.ValidateTimeDurations(cfg.TTL) {
			return nil, fmt.Errorf("invalid cache ttl %q", cfg.TTL)
		}
		p.ttl = pkg.GetTimeDuration(cfg.TTL)
	}

	plan, err := combineRules(cfg.Rules)
	if err != nil {
		return nil, err
	}
	p.plan = plan
	return p, nil
}

func combineRules(rules []string) (*logsgoql.Plan, error) {
	var root logsgoql.Expr
	for _, rule := range rules {
		parser := logsgoql.NewParser(logsgoql.NewLexer(rule))
		expr := parser.ParseExpression()
		if errs := parser.Errors(); len(errs) > 0 {
			return nil, fmt.Errorf("invalid cache rule %q: %v", rule, errs)
		}
		if root == nil {
			root = expr
			continue
		}
		root = &logsgoql.BinaryExpr{Left: root, Operator: logsgoql.OR, Right: expr}
	}
	if root == nil {
		return nil, nil
	}
	return logsgoql.BuildPlan(root)
}

func (p *CachePolicy) ShouldCache(labels logsgoql.EntryLabels) bool {
	if p == nil || !p.enabled {
		return false
	}
	if p.plan == nil {
		return true
	}
	ok, err := p.plan.Match(labels)
	return err == nil && ok
}

// Covers reports whether a query restricted to plan is fully answerable from
// the cache alone, so the durable tier can be skipped.
func (p *CachePolicy) Covers(plan *logsgoql.Plan) bool {
	if p == nil || !p.enabled {
		return false
	}
	if p.plan == nil {
		return true
	}
	return plan.SubsetOf(p.plan)
}
