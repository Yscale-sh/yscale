package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

func TestStructuredLogsDoNotUseSensitiveKeys(t *testing.T) {
	t.Parallel()

	packages, err := parser.ParseDir(token.NewFileSet(), ".", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	sensitive := map[string]bool{
		"auth_key": true, "authorization": true, "password": true,
		"secret": true, "token": true,
	}
	for _, file := range packages["main"].Files {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || (selector.Sel.Name != "Debug" && selector.Sel.Name != "Info" &&
				selector.Sel.Name != "Warn" && selector.Sel.Name != "Error") {
				return true
			}
			for _, argument := range call.Args {
				literal, ok := argument.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				value, err := strconv.Unquote(literal.Value)
				if err == nil && sensitive[strings.ToLower(value)] {
					t.Errorf("structured log uses forbidden sensitive key %q", value)
				}
			}
			return true
		})
	}
}
