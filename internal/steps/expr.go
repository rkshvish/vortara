package steps

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// Expr is a compiled expression.
type Expr interface {
	eval(data map[string]interface{}) bool
}

func parseExpr(condition string) (Expr, error) {
	p := &parser{lex: &lexer{input: condition}}
	p.next()
	expr, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.cur.kind != tokEOF {
		return nil, fmt.Errorf("unexpected token %q", p.cur.val)
	}
	return expr, nil
}

// ParseExpr compiles a condition string for reuse across packages.
func ParseExpr(condition string) (Expr, error) {
	return parseExpr(condition)
}

func evalExpr(expr Expr, data map[string]interface{}) bool {
	if expr == nil {
		return false
	}
	return expr.eval(data)
}

// EvalExpr evaluates a compiled expression against row data.
func EvalExpr(expr Expr, data map[string]interface{}) bool {
	return evalExpr(expr, data)
}

type tokenKind int

const (
	tokField tokenKind = iota
	tokString
	tokNumber
	tokBool
	tokEQ
	tokNE
	tokGT
	tokGTE
	tokLT
	tokLTE
	tokAND
	tokOR
	tokNOT
	tokLParen
	tokRParen
	tokComma
	tokIdent
	tokEOF
)

type token struct {
	kind tokenKind
	val  string
}

type lexer struct {
	input string
	pos   int
}

func (l *lexer) next() token {
	l.skipSpace()
	if l.pos >= len(l.input) {
		return token{kind: tokEOF}
	}

	switch ch := l.input[l.pos]; ch {
	case '(':
		l.pos++
		return token{kind: tokLParen, val: "("}
	case ')':
		l.pos++
		return token{kind: tokRParen, val: ")"}
	case ',':
		l.pos++
		return token{kind: tokComma, val: ","}
	case '=':
		if l.peek('=') {
			l.pos += 2
			return token{kind: tokEQ, val: "=="}
		}
	case '!':
		if l.peek('=') {
			l.pos += 2
			return token{kind: tokNE, val: "!="}
		}
	case '>':
		if l.peek('=') {
			l.pos += 2
			return token{kind: tokGTE, val: ">="}
		}
		l.pos++
		return token{kind: tokGT, val: ">"}
	case '<':
		if l.peek('=') {
			l.pos += 2
			return token{kind: tokLTE, val: "<="}
		}
		l.pos++
		return token{kind: tokLT, val: "<"}
	case '\'':
		return l.scanString()
	}

	if unicode.IsDigit(rune(l.input[l.pos])) {
		return l.scanNumber()
	}

	ident := l.scanIdent()
	if ident == "" {
		ch := l.input[l.pos]
		l.pos++
		return token{kind: tokEOF, val: string(ch)}
	}
	switch strings.ToUpper(ident) {
	case "AND":
		return token{kind: tokAND, val: ident}
	case "OR":
		return token{kind: tokOR, val: ident}
	case "NOT":
		return token{kind: tokNOT, val: ident}
	case "TRUE", "FALSE":
		return token{kind: tokBool, val: strings.ToLower(ident)}
	default:
		return token{kind: tokIdent, val: ident}
	}
}

func (l *lexer) skipSpace() {
	for l.pos < len(l.input) && unicode.IsSpace(rune(l.input[l.pos])) {
		l.pos++
	}
}

func (l *lexer) peek(expected byte) bool {
	return l.pos+1 < len(l.input) && l.input[l.pos+1] == expected
}

func (l *lexer) scanString() token {
	l.pos++
	start := l.pos
	for l.pos < len(l.input) && l.input[l.pos] != '\'' {
		l.pos++
	}
	val := l.input[start:l.pos]
	if l.pos < len(l.input) {
		l.pos++
	}
	return token{kind: tokString, val: val}
}

func (l *lexer) scanNumber() token {
	start := l.pos
	for l.pos < len(l.input) && (unicode.IsDigit(rune(l.input[l.pos])) || l.input[l.pos] == '.') {
		l.pos++
	}
	return token{kind: tokNumber, val: l.input[start:l.pos]}
}

func (l *lexer) scanIdent() string {
	start := l.pos
	for l.pos < len(l.input) {
		ch := rune(l.input[l.pos])
		if unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_' || ch == '.' {
			l.pos++
			continue
		}
		break
	}
	return l.input[start:l.pos]
}

type parser struct {
	lex *lexer
	cur token
}

func (p *parser) next() {
	p.cur = p.lex.next()
}

func (p *parser) expect(kind tokenKind) error {
	if p.cur.kind != kind {
		return fmt.Errorf("unexpected token %q", p.cur.val)
	}
	return nil
}

func (p *parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.cur.kind == tokOR {
		p.next()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = orExpr{left: left, right: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (Expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.cur.kind == tokAND {
		p.next()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = andExpr{left: left, right: right}
	}
	return left, nil
}

func (p *parser) parseNot() (Expr, error) {
	if p.cur.kind == tokNOT {
		p.next()
		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return notExpr{inner: inner}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (Expr, error) {
	switch p.cur.kind {
	case tokLParen:
		p.next()
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokRParen); err != nil {
			return nil, err
		}
		p.next()
		return inner, nil
	case tokIdent:
		name := p.cur.val
		p.next()
		if p.cur.kind == tokLParen {
			return p.parseFunctionCall(name)
		}
		return p.parseComparison(name)
	default:
		return nil, fmt.Errorf("unexpected token %q", p.cur.val)
	}
}

func (p *parser) parseFunctionCall(name string) (Expr, error) {
	p.next() // (
	if p.cur.kind != tokIdent {
		return nil, fmt.Errorf("expected field name in %s()", name)
	}
	field := p.cur.val
	p.next()
	if err := p.expect(tokComma); err != nil {
		return nil, err
	}
	p.next()
	if p.cur.kind != tokString {
		return nil, fmt.Errorf("expected string literal in %s()", name)
	}
	arg := p.cur.val
	p.next()
	if err := p.expect(tokRParen); err != nil {
		return nil, err
	}
	p.next()

	switch strings.ToLower(name) {
	case "contains":
		return containsExpr{field: field, substr: arg}, nil
	case "startswith":
		return startsWithExpr{field: field, prefix: arg}, nil
	default:
		return nil, fmt.Errorf("unknown function %q", name)
	}
}

func (p *parser) parseComparison(field string) (Expr, error) {
	op := p.cur.kind
	if op != tokEQ && op != tokNE && op != tokGT && op != tokGTE && op != tokLT && op != tokLTE {
		return nil, fmt.Errorf("expected comparison operator after %q", field)
	}
	p.next()
	val, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	return cmpExpr{field: field, op: op.String(), value: val}, nil
}

func (p *parser) parseValue() (interface{}, error) {
	switch p.cur.kind {
	case tokString:
		v := p.cur.val
		p.next()
		return v, nil
	case tokNumber:
		v, err := strconv.ParseFloat(p.cur.val, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid number %q", p.cur.val)
		}
		p.next()
		return v, nil
	case tokBool:
		v := p.cur.val == "true"
		p.next()
		return v, nil
	default:
		return nil, fmt.Errorf("expected literal value, got %q", p.cur.val)
	}
}

func (k tokenKind) String() string {
	switch k {
	case tokEQ:
		return "=="
	case tokNE:
		return "!="
	case tokGT:
		return ">"
	case tokGTE:
		return ">="
	case tokLT:
		return "<"
	case tokLTE:
		return "<="
	default:
		return ""
	}
}

type cmpExpr struct {
	field string
	op    string
	value interface{}
}

func (e cmpExpr) eval(data map[string]interface{}) bool {
	left, ok := data[e.field]
	if !ok {
		return false
	}
	switch rv := e.value.(type) {
	case string:
		ls, ok := left.(string)
		if !ok {
			return false
		}
		switch e.op {
		case "==":
			return ls == rv
		case "!=":
			return ls != rv
		case ">":
			return ls > rv
		case ">=":
			return ls >= rv
		case "<":
			return ls < rv
		case "<=":
			return ls <= rv
		default:
			return false
		}
	case float64:
		lf, ok := toFloat64(left)
		if !ok {
			return false
		}
		switch e.op {
		case "==":
			return lf == rv
		case "!=":
			return lf != rv
		case ">":
			return lf > rv
		case ">=":
			return lf >= rv
		case "<":
			return lf < rv
		case "<=":
			return lf <= rv
		default:
			return false
		}
	case bool:
		lb, ok := left.(bool)
		if !ok {
			return false
		}
		switch e.op {
		case "==":
			return lb == rv
		case "!=":
			return lb != rv
		default:
			return false
		}
	default:
		return false
	}
}

type andExpr struct {
	left, right Expr
}

func (e andExpr) eval(data map[string]interface{}) bool {
	return e.left.eval(data) && e.right.eval(data)
}

type orExpr struct {
	left, right Expr
}

func (e orExpr) eval(data map[string]interface{}) bool {
	return e.left.eval(data) || e.right.eval(data)
}

type notExpr struct {
	inner Expr
}

func (e notExpr) eval(data map[string]interface{}) bool {
	return !e.inner.eval(data)
}

type containsExpr struct {
	field  string
	substr string
}

func (e containsExpr) eval(data map[string]interface{}) bool {
	v, ok := data[e.field]
	if !ok {
		return false
	}
	s, ok := v.(string)
	if !ok {
		return false
	}
	return strings.Contains(s, e.substr)
}

type startsWithExpr struct {
	field  string
	prefix string
}

func (e startsWithExpr) eval(data map[string]interface{}) bool {
	v, ok := data[e.field]
	if !ok {
		return false
	}
	s, ok := v.(string)
	if !ok {
		return false
	}
	return strings.HasPrefix(s, e.prefix)
}

func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	default:
		return 0, false
	}
}
