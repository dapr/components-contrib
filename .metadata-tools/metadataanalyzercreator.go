package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"io/ioutil"
	"log"
	"path/filepath"
	"strings"
)

func main() {
	flag.Parse()
	root := flag.Arg(0)

	fset := token.NewFileSet()
	pkgs := make(map[string]string)

	err := filepath.WalkDir(root, func(path string, file fs.DirEntry, err error) error {
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

			switch filepath.Base(dir) {
			case "bindings", "pubsub", "state", "secretstores", "middleware", "workflows", "lock", "configuration", "crypto", "nameresolution":
				componentType = filepath.Base(dir)
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
		}

		if methodFound {
			pkgs[packageName] = method
		}

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// now we have a list of packages that implement the GetComponentMetadata method
	// now write our go program to check the metadata of each component and compare it againts the yaml metadata

	importBlock := ""
	for pkg := range pkgs {
		importBlock += fmt.Sprintf(`    %s "github.com/dapr/components-contrib/%s"`+"\n", strings.ReplaceAll(strings.ReplaceAll(pkg, "/", "_"), "-", "_"), pkg)
	}

	metadataBlock := ""
	for fullpkg, method := range pkgs {
		sanitizedPkg := strings.ReplaceAll(strings.ReplaceAll(fullpkg, "/", "_"), "-", "_")

		metadataBlock += "\n" + fmt.Sprintf(`
    instanceOf_%s := %s.%s(log)
    metadataFor_%s := instanceOf_%s.GetComponentMetadata()
    yamlMetadata = getYamlMetadata(basePath, "%s")
    missing = checkMissingMetadata(yamlMetadata, metadataFor_%s)
    if len(missing) > 0 {
      missingByComponent["%s"] = missing
    }`, sanitizedPkg, sanitizedPkg, method, sanitizedPkg, sanitizedPkg, fullpkg, sanitizedPkg, fullpkg)
	}

	// write go program to file
	code := `package main

import (
    "fmt"
    "os"
    "github.com/dapr/kit/logger"
    "encoding/json"
%s
)

func main() {
    if len(os.Args) < 2 {
      fmt.Println("Please provide the path to the components-contrib root as an argument")
      os.Exit(1)
    }
    basePath := os.Args[1]
    log := logger.NewLogger("metadata")

    var yamlMetadata *map[string]string
    var missing map[string]string
    missingByComponent := make(map[string]map[string]string)
%s

    jsonData, err := json.MarshalIndent(missingByComponent, "", "  ")
    if err != nil {
      fmt.Println(err)
      return
    }
		if len(missingByComponent) > 0 {
			fmt.Println("The following components are missing metadata in their metadata.yaml:\n")
      fmt.Println(string(jsonData))
			os.Exit(1)
		}
}`

	errOut := ioutil.WriteFile(".metadata-tools/metadataanalyzer/metadataanalyzer.go", []byte(fmt.Sprintf(code, importBlock, metadataBlock)), 0o644)
	if err != nil {
		log.Fatal(errOut)
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
						return f.Name.Name, nil
					}
				}
			}
		}
	}
	return "", errors.New("could not find constructor method")
}
