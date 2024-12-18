/*
Copyright 2024 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cmd

import (
	"go/ast"
	"go/token"
)

func createMockASTFileWithFunc(funcName string, found bool) *ast.File {
	if !found {
		return &ast.File{}
	}
	// Create a mock AST with the specified function name
	return &ast.File{
		Decls: []ast.Decl{
			&ast.FuncDecl{
				Name: &ast.Ident{Name: funcName},
				Body: createValidFunctionBody(),
			},
		},
	}
}

func createValidFunctionBody() *ast.BlockStmt {
	return &ast.BlockStmt{
		List: []ast.Stmt{
			&ast.ReturnStmt{
				Results: []ast.Expr{createValidCompositeLit()},
			},
		},
	}
}

func createValidCompositeLit() *ast.CompositeLit {
	return &ast.CompositeLit{
		// Using SelectorExpr for the type field to match 'Binding' type
		Type: &ast.SelectorExpr{
			X:   &ast.Ident{Name: "metadataschema"}, // Package name
			Sel: &ast.Ident{Name: "Binding"},        // Type name
		},
		Elts: []ast.Expr{
			&ast.KeyValueExpr{
				Key:   &ast.Ident{Name: "Input"},
				Value: &ast.Ident{Name: "true"}, // Keep it as an Ident (literal bool type)
			},
			&ast.KeyValueExpr{
				Key:   &ast.Ident{Name: "Output"},
				Value: &ast.Ident{Name: "false"}, // Keep it as an Ident (literal bool type)
			},
			&ast.KeyValueExpr{
				Key: &ast.Ident{Name: "Operations"},
				Value: &ast.CompositeLit{
					Elts: []ast.Expr{
						&ast.CompositeLit{
							Elts: []ast.Expr{
								&ast.KeyValueExpr{
									Key:   &ast.Ident{Name: "Name"},
									Value: &ast.BasicLit{Kind: token.STRING, Value: `"create"`}, // Correct string literal
								},
								&ast.KeyValueExpr{
									Key:   &ast.Ident{Name: "Description"},
									Value: &ast.BasicLit{Kind: token.STRING, Value: `"Create blob"`}, // Correct string literal
								},
							},
						},
						&ast.CompositeLit{
							Elts: []ast.Expr{
								&ast.KeyValueExpr{
									Key:   &ast.Ident{Name: "Name"},
									Value: &ast.BasicLit{Kind: token.STRING, Value: `"get"`}, // Correct string literal
								},
								&ast.KeyValueExpr{
									Key:   &ast.Ident{Name: "Description"},
									Value: &ast.BasicLit{Kind: token.STRING, Value: `"Get blob"`}, // Correct string literal
								},
							},
						},
					},
				},
			},
		},
	}
}

func createInvalidCompositeLit() *ast.CompositeLit {
	return &ast.CompositeLit{
		Type: &ast.Ident{Name: "NotBinding"},
		Elts: []ast.Expr{
			&ast.KeyValueExpr{
				Key:   &ast.Ident{Name: "InvalidInput"},
				Value: &ast.Ident{Name: "true"},
			},
			&ast.KeyValueExpr{
				Key:   &ast.Ident{Name: "InvalidOutput"},
				Value: &ast.Ident{Name: "false"},
			},
		},
	}
}

func createKeyValueExpr(key, value string) *ast.KeyValueExpr {
	return &ast.KeyValueExpr{
		Key:   &ast.Ident{Name: key},
		Value: &ast.BasicLit{Kind: token.STRING, Value: value},
	}
}

func createInvalidKeyValueExpr(value string) *ast.KeyValueExpr {
	return &ast.KeyValueExpr{
		Key:   nil, // this is invalid intentionally and should actually be &ast.Ident{Name: key},
		Value: &ast.Ident{Name: value},
	}
}

func createBodyWithoutBindingReturn() *ast.BlockStmt {
	return &ast.BlockStmt{
		List: []ast.Stmt{
			&ast.ReturnStmt{
				Results: []ast.Expr{
					// createNonBindingReturn(),
				},
			},
		},
	}
}

// Helper function to create a CompositeLit for a single BindingOperation
func createBindingOperationCompositeLit(name, description string) *ast.CompositeLit {
	return &ast.CompositeLit{
		Elts: []ast.Expr{
			createKeyValueExpr("Name", name),
			createKeyValueExpr("Description", description),
		},
	}
}
