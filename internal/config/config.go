package config

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type ValueType int

const (
	Float ValueType = iota
	String
	Bool
	Object
	Array
)

type Value struct {
	Type ValueType
	F    float64
	S    string
	B    bool
	O    map[string]any
	A    []Value
}

func (v Value) MarshalJSON() ([]byte, error) {
	switch v.Type {
	case Float:
		return json.Marshal(v.F)
	case String:
		return json.Marshal(v.S)
	case Bool:
		return json.Marshal(v.B)
	case Object:
		return json.Marshal(v.O)
	case Array:
		return json.Marshal(v.A)
	default:
		return nil, fmt.Errorf("unknown ValueType %d", v.Type)
	}
}

func (v *Value) UnmarshalJSON(data []byte) error {
	var u any
	err := json.Unmarshal(data, &u)
	if err != nil {
		return err
	}

	switch val := u.(type) {
	case float64:
		v.Type = Float
		v.F = val
		return nil

	case string:
		v.Type = String
		v.S = val
		return nil

	case bool:
		v.Type = Bool
		v.B = val
		return nil

	case map[string]any:
		v.Type = Object
		v.O = val
		return nil

	case []json.RawMessage:
		v.Type = Array
		v.A = make([]Value, len(val))
		for i, item := range val {
			err := v.A[i].UnmarshalJSON(item)
			if err != nil {
				return err
			}
		}

		return nil

	default:
		return fmt.Errorf("could not infer type from %s", data)
	}
}

type Operand string

const (
	Eq Operand = "=="
	Ne Operand = "!="
	Lt Operand = "<"
	Le Operand = "<="
	Gt Operand = ">"
	Ge Operand = ">="
)

func (o Operand) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(o))
}

func (o *Operand) UnmarshalJSON(data []byte) error {
	var s string
	err := json.Unmarshal(data, &s)
	if err != nil {
		return err
	}

	op := Operand(s)
	switch op {
	case Eq, Ne, Lt, Le, Gt, Ge:
		*o = op
		return nil
	}

	return fmt.Errorf("unknown operand value %s", s)
}

type CriteriaNode interface {
	Evaluate(obj map[string]any) bool
}

type Criterion struct {
	Path    string  `json:"path"`
	Operand Operand `json:"operand"`
	Value   Value   `json:"value"`
}

var _ CriteriaNode = Criterion{}

func getKeyValue(key string, obj map[string]any) (any, error) {
	keys := strings.Split(key, ".")
	target := keys[0]

	value, exists := obj[target]
	if !exists {
		return nil, fmt.Errorf("key does not match any element of the object")
	}

	switch typedValue := value.(type) {
	case map[string]any:
		{
			return getKeyValue(strings.Join(keys[1:], "."), typedValue)
		}
	default:
		if len(keys) == 1 {
			return value, nil
		} else {
			return nil, fmt.Errorf("could not match path")
		}
	}
}

func compareFloat(a any, o Operand, v float64) bool {
	switch t := a.(type) {
	case float64:
		switch o {
		case Eq:
			return t == v
		case Ne:
			return t != v
		case Lt:
			return t < v
		case Le:
			return t <= v
		case Gt:
			return t > v
		case Ge:
			return t >= v
		default:
			return false
		}

	default:
		return false
	}
}

func compareString(a any, o Operand, v string) bool {
	switch t := a.(type) {
	case string:
		switch o {
		case Eq:
			return t == v
		case Ne:
			return t != v
		case Lt:
			return t < v
		case Le:
			return t <= v
		case Gt:
			return t > v
		case Ge:
			return t >= v
		default:
			return false
		}

	default:
		return false
	}
}

func compareBool(a any, o Operand, v bool) bool {
	switch t := a.(type) {
	case bool:
		switch o {
		case Eq:
			return t == v
		case Ne:
			return t != v
		case Lt:
			return !t && v
		case Le:
			return !t || t && v
		case Gt:
			return t && !v
		case Ge:
			return t
		default:
			return false
		}

	default:
		return false
	}
}

func (c Criterion) Evaluate(obj map[string]any) bool {
	val, err := getKeyValue(c.Path, obj)
	if err != nil {
		return false
	}

	switch c.Value.Type {
	case Float:
		return compareFloat(val, c.Operand, c.Value.F)
	case String:
		return compareString(val, c.Operand, c.Value.S)
	case Bool:
		return compareBool(val, c.Operand, c.Value.B)
	case Object, Array:
		return false
	default:
		return false
	}
}

type CriteriaGroup struct {
	IsDisjunctive bool           `json:"isDisjunctive,omitempty"`
	IsNegated     bool           `json:"isNegated,omitempty"`
	Children      []CriteriaNode `json:"children"`
}

var _ CriteriaNode = CriteriaGroup{}

func unmarshalNode(data []byte) (CriteriaNode, error) {
	var raw map[string]json.RawMessage
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return nil, err
	}

	_, hasChildrenKey := raw["children"]
	if hasChildrenKey {
		var g CriteriaGroup
		err := json.Unmarshal(data, &g)
		if err != nil {
			return nil, err
		}

		return g, nil
	} else {
		var c Criterion
		err := json.Unmarshal(data, &c)
		if err != nil {
			return nil, err
		}

		return c, nil
	}
}

func (cg *CriteriaGroup) UnmarshalJSON(data []byte) error {
	type alias struct {
		IsDisjunctive bool              `json:"isDisjunctive,omitempty"`
		IsNegated     bool              `json:"isNegated,omitempty"`
		Children      []json.RawMessage `json:"children"`
	}

	var aux alias
	err := json.Unmarshal(data, &aux)
	if err != nil {
		return err
	}

	cg.IsDisjunctive = aux.IsDisjunctive
	cg.IsNegated = aux.IsNegated
	cg.Children = make([]CriteriaNode, len(aux.Children))
	for i, childData := range aux.Children {
		child, err := unmarshalNode(childData)
		if err != nil {
			return err
		}

		cg.Children[i] = child
	}

	return nil
}

func (cg CriteriaGroup) Evaluate(obj map[string]any) bool {
	if len(cg.Children) == 0 {
		return cg.IsNegated
	}

	for _, child := range cg.Children {
		res := false
		switch c := child.(type) {
		case Criterion:
			res = c.Evaluate(obj)
		case CriteriaGroup:
			res = c.Evaluate(obj)
		}

		if res && cg.IsDisjunctive {
			return !cg.IsNegated
		} else if !res {
			return cg.IsNegated
		}
	}

	return !cg.IsNegated
}

type DataSource interface {
	Connect(ctx context.Context, msg chan json.RawMessage) error
}

type Config []DataSource
