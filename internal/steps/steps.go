package steps

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/rkshvish/vortaraos/pkg/config/v2"
	"github.com/rkshvish/vortaraos/pkg/row"
)

// Processor applies a sequence of transform steps to each row.
type Processor struct {
	steps   []stepFn
	mutates bool
}

// stepFn processes one row. Returns modified row and whether it should be kept.
type stepFn func(r row.Row) (row.Row, bool)

// New builds a Processor from v2 transform step config.
func New(steps []v2.TransformStep) (*Processor, error) {
	fns := make([]stepFn, 0, len(steps))
	mutates := false

	for _, step := range steps {
		if step.Filter != "" {
			expr, err := parseExpr(step.Filter)
			if err != nil {
				return nil, err
			}
			fns = append(fns, func(expr Expr) stepFn {
				return func(r row.Row) (row.Row, bool) {
					return r, evalExpr(expr, r.Data)
				}
			}(expr))
		}

		if len(step.Rename) > 0 {
			renames := step.Rename
			mutates = true
			fns = append(fns, func(r row.Row) (row.Row, bool) {
				for old, newName := range renames {
					if v, ok := r.Data[old]; ok {
						r.Data[newName] = v
						delete(r.Data, old)
					}
				}
				return r, true
			})
		}

		if len(step.Add) > 0 {
			adds := step.Add
			mutates = true
			fns = append(fns, func(r row.Row) (row.Row, bool) {
				for field, expr := range adds {
					r.Data[field] = evalAddExpr(expr, r.Data)
				}
				return r, true
			})
		}

		if len(step.Drop) > 0 {
			drops := step.Drop
			mutates = true
			fns = append(fns, func(r row.Row) (row.Row, bool) {
				for _, field := range drops {
					delete(r.Data, field)
				}
				return r, true
			})
		}

		if len(step.Trim) > 0 {
			trims := step.Trim
			all := len(trims) == 1 && trims[0] == "*"
			mutates = true
			fns = append(fns, func(r row.Row) (row.Row, bool) {
				if all {
					for k, v := range r.Data {
						if s, ok := v.(string); ok {
							r.Data[k] = strings.TrimSpace(s)
						}
					}
					return r, true
				}
				for _, field := range trims {
					if s, ok := r.Data[field].(string); ok {
						r.Data[field] = strings.TrimSpace(s)
					}
				}
				return r, true
			})
		}

		if step.Flatten != "" {
			sep := step.Flatten
			mutates = true
			fns = append(fns, func(r row.Row) (row.Row, bool) {
				flattenInto(r.Data, sep)
				return r, true
			})
		}

		if len(step.Mask) > 0 {
			masks := step.Mask
			mutates = true
			fns = append(fns, func(r row.Row) (row.Row, bool) {
				for _, field := range masks {
					if _, ok := r.Data[field]; ok {
						r.Data[field] = "***"
					}
				}
				return r, true
			})
		}
	}

	return &Processor{steps: fns, mutates: mutates}, nil
}

// Apply runs all configured steps on a row. The input row is never mutated:
// when any step modifies data, the row is cloned exactly once up front and
// the steps operate on the clone in place.
func (p *Processor) Apply(r row.Row) (row.Row, bool) {
	if p == nil || len(p.steps) == 0 {
		return r, true
	}
	if p.mutates {
		r = r.Clone()
	}
	for _, step := range p.steps {
		var keep bool
		r, keep = step(r)
		if !keep {
			return r, false
		}
	}
	return r, true
}

// flattenInto rewrites nested map values into top-level keys joined by sep:
// {"user": {"email": e}} with sep "_" becomes {"user_email": e}.
func flattenInto(data map[string]interface{}, sep string) {
	for {
		expanded := false
		for k, v := range data {
			nested, ok := v.(map[string]interface{})
			if !ok {
				continue
			}
			delete(data, k)
			for nk, nv := range nested {
				data[k+sep+nk] = nv
			}
			expanded = true
		}
		if !expanded {
			return
		}
	}
}

var templatePattern = regexp.MustCompile(`\{\{[^}]*\}\}`)

// evalAddExpr evaluates a step.Add expression string.
func evalAddExpr(expr string, data map[string]interface{}) interface{} {
	expr = strings.TrimSpace(expr)

	// Accept the documented template form: {{ expr }}. A whole-string
	// placeholder preserves the evaluated type; mixed text like
	// "{{ first }} {{ last }}" renders as a concatenated string.
	if strings.HasPrefix(expr, "{{") && strings.HasSuffix(expr, "}}") && len(expr) >= 4 && strings.Count(expr, "{{") == 1 {
		expr = strings.TrimSpace(expr[2 : len(expr)-2])
	} else if strings.Contains(expr, "{{") {
		return templatePattern.ReplaceAllStringFunc(expr, func(match string) string {
			inner := strings.TrimSpace(match[2 : len(match)-2])
			v := evalAddExpr(inner, data)
			if v == nil {
				return ""
			}
			if t, ok := v.(time.Time); ok {
				return t.UTC().Format(time.RFC3339)
			}
			return fmt.Sprintf("%v", v)
		})
	}

	if expr == "now()" {
		return time.Now().UTC()
	}

	if strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'") && len(expr) >= 2 {
		return expr[1 : len(expr)-1]
	}

	if strings.HasPrefix(expr, "if ") {
		return evalIfExpr(expr, data)
	}

	if v, ok := data[expr]; ok {
		return v
	}

	return expr
}

// evalIfExpr handles "if condition then value else value".
func evalIfExpr(expr string, data map[string]interface{}) interface{} {
	parts := strings.SplitN(expr[3:], " then ", 2)
	if len(parts) != 2 {
		return expr
	}
	condition := strings.TrimSpace(parts[0])
	rest := strings.SplitN(parts[1], " else ", 2)
	if len(rest) != 2 {
		return expr
	}

	thenVal := strings.TrimSpace(rest[0])
	elseVal := strings.TrimSpace(rest[1])

	compiled, err := parseExpr(condition)
	if err != nil {
		return expr
	}

	if evalExpr(compiled, data) {
		return evalAddExpr(thenVal, data)
	}
	return evalAddExpr(elseVal, data)
}

func mustParse(expr string) Expr {
	parsed, err := parseExpr(expr)
	if err != nil {
		panic(fmt.Sprintf("invalid expression in tests or step init: %v", err))
	}
	return parsed
}
