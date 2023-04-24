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

	// Import the embed package.
	_ "embed"
)

//go:embed analyzer.template
var tmpl string

type PkgInfo struct {
	Method        string
	ComponentType string
}

func GenerateMetadataAnalyzer(contribRoot string, componentFolders []string, outputfile string) {
	fset := token.NewFileSet()
	pkgs := make(map[string]PkgInfo)

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

		componentTypeFolder := ""
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
					componentTypeFolder = curFolder
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

		switch componentTypeFolder {
		// Only the component types listed here implement the GetComponentMetadata method today
		// Note: these are folder names not the type of components
		case "secretstores":
			method, methodFinderErr = getConstructorMethod("secretstores.SecretStore", parsedFile)
			if methodFinderErr == nil {
				methodFound = true
			}
		case "state":
			method, methodFinderErr = getConstructorMethod("state.Store", parsedFile)
			if methodFinderErr == nil {
				methodFound = true
			}
		case "bindings":
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
		case "lock":
			method, methodFinderErr = getConstructorMethod("lock.Store", parsedFile)
			if methodFinderErr == nil {
				methodFound = true
			}
		case "workflows":
			method, methodFinderErr = getConstructorMethod("workflows.Workflow", parsedFile)
			if methodFinderErr == nil {
				methodFound = true
			}
		case "configuration":
			method, methodFinderErr = getConstructorMethod("configuration.Store", parsedFile)
			if methodFinderErr == nil {
				methodFound = true
			}
		case "crypto":
			method, methodFinderErr = getConstructorMethod("contribCrypto.SubtleCrypto", parsedFile)
			if methodFinderErr == nil {
				methodFound = true
			}
		case "middleware":
			method, methodFinderErr = getConstructorMethod("middleware.Middleware", parsedFile)
			if methodFinderErr == nil {
				methodFound = true
			}
		case "pubsub":
			method, methodFinderErr = getConstructorMethod("pubsub.PubSub", parsedFile)
			if methodFinderErr == nil {
				methodFound = true
			}
		}

		if methodFound {
			pkgs[packageName] = PkgInfo{
				Method:        method,
				ComponentType: componentTypeFolder,
			}
		}

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	data := make(map[string][]string)

	for fullpkg, info := range pkgs {
		sanitizedPkg := strings.ReplaceAll(strings.ReplaceAll(fullpkg, "/", "_"), "-", "_")
		data[fullpkg] = []string{sanitizedPkg, info.Method, info.ComponentType}
	}

	templateData := struct {
		Pkgs map[string][]string
	}{
		Pkgs: data,
	}

	// let's try loading the template
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
