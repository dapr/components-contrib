package metadataanalyzer

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	mdutils "github.com/dapr/components-contrib/metadata"
)

func GenerateMetadataAnalyzer(contribRoot string, componentFolders []string, outputfile string) {
	fset := token.NewFileSet()
	pkgs := make(map[string]string)

	err := filepath.WalkDir(contribRoot, func(path string, file fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if file.IsDir() {
			if file.Name() == "vendor" || file.Name() == "tests" || file.Name() == "internal" {
				return fs.SkipDir
			}
			return nil
		}

		if filepath.Ext(path) != ".go" {
			return nil
		}

		componentType := ""
		packageName := ""
		skip := true
		dir := filepath.Dir(path)
		for dir != "." && !strings.HasSuffix(dir, "components-contrib") {
			if !skip {
				packageName = filepath.Base(dir) + "/" + packageName
			} else {
				packageName = filepath.Base(dir)
			}
			skip = false
			dir = filepath.Dir(dir)

			curFolder := filepath.Base(dir)

			for _, val := range componentFolders {
				if curFolder == val {
					componentType = curFolder
				}
			}
		}

		parsedFile, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			log.Printf("could not parse %s: %v", path, err)
			return nil
		}

		var method string
		var methodFinderErr error
		methodFound := false

		switch componentType {
		// Only the component types listed here implement the GetComponentMetadata method today
		case string(mdutils.SecretStoreType):
			method, methodFinderErr = getConstructorMethod("secretstores.SecretStore", parsedFile)
			if methodFinderErr == nil {
				methodFound = true
			}
		case string(mdutils.StateStoreType):
			method, methodFinderErr = getConstructorMethod("state.Store", parsedFile)
			if methodFinderErr == nil {
				methodFound = true
			}
		case string(mdutils.BindingType):
			method, methodFinderErr = getConstructorMethod("bindings.InputOutputBinding", parsedFile)
			if methodFinderErr == nil {
				methodFound = true
			} else {
				method, methodFinderErr = getConstructorMethod("bindings.OutputBinding", parsedFile)
				if methodFinderErr == nil {
					methodFound = true
				} else {
					method, methodFinderErr = getConstructorMethod("bindings.InputBinding", parsedFile)
					if methodFinderErr == nil {
						methodFound = true
					}
				}
			}
		case string(mdutils.LockStoreType):
			method, methodFinderErr = getConstructorMethod("lock.Store", parsedFile)
			if methodFinderErr == nil {
				methodFound = true
			}
		case string(mdutils.WorkflowType):
			method, methodFinderErr = getConstructorMethod("workflows.Workflow", parsedFile)
			if methodFinderErr == nil {
				methodFound = true
			}
		case string(mdutils.ConfigurationStoreType):
			method, methodFinderErr = getConstructorMethod("configuration.Store", parsedFile)
			if methodFinderErr == nil {
				methodFound = true
			}
		case string(mdutils.CryptoType):
			method, methodFinderErr = getConstructorMethod("contribCrypto.SubtleCrypto", parsedFile)
			if methodFinderErr == nil {
				methodFound = true
			}
		case string(mdutils.MiddlewareType):
			method, methodFinderErr = getConstructorMethod("middleware.Middleware", parsedFile)
			if methodFinderErr == nil {
				methodFound = true
			}
		}

		if methodFound {
			pkgs[packageName] = method
		}

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	data := make(map[string][]string)

	for fullpkg, method := range pkgs {
		sanitizedPkg := strings.ReplaceAll(strings.ReplaceAll(fullpkg, "/", "_"), "-", "_")
		data[fullpkg] = []string{sanitizedPkg, method}
	}

	templateData := struct {
		Pkgs map[string][]string
	}{
		Pkgs: data,
	}

	// let's try loading the template
	bytes, err := os.ReadFile(".build-tools/pkg/metadataanalyzer/analyzer.template")
	tmpl := string(bytes)
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Create(outputfile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	t := template.Must(template.New("tmpl").Parse(tmpl))
	err = t.Execute(f, templateData)
	if err != nil {
		log.Fatal(err)
	}
}

func getConstructorMethod(componentType string, file *ast.File) (string, error) {
	typeSplit := strings.Split(componentType, ".")
	if len(typeSplit) != 2 {
		return "", fmt.Errorf("invalid component type: %s", componentType)
	}

	for _, d := range file.Decls {
		if f, ok := d.(*ast.FuncDecl); ok {
			if f.Type.Results != nil && len(f.Type.Results.List) > 0 {
				if selExpr, ok := f.Type.Results.List[0].Type.(*ast.SelectorExpr); ok {
					xIdent, ok := selExpr.X.(*ast.Ident)
					if !ok || xIdent.Name != typeSplit[0] {
						continue
					}
					if selExpr.Sel.Name == typeSplit[1] {
						if len(f.Name.Name) > 3 && f.Name.Name[:3] == "New" {
							return f.Name.Name, nil
						}
					}
				}
			}
		}
	}
	return "", errors.New("could not find constructor method")
}
