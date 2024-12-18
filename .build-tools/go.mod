module github.com/dapr/components-contrib/build-tools

go 1.23.0

replace github.com/dapr/components-contrib/build-tools => /Users/samcoyle/go/src/github.com/forks/dapr/components-contrib/.build-tools

require (
	github.com/dapr/components-contrib v0.0.0
	github.com/invopop/jsonschema v0.6.0
	github.com/spf13/cobra v1.8.1
	github.com/stretchr/testify v1.9.0
	github.com/xeipuuv/gojsonschema v1.2.1-0.20201027075954-b076d39a02e5
	gopkg.in/yaml.v3 v3.0.1
	sigs.k8s.io/yaml v1.4.0
)

require github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect

require (
	github.com/dapr/kit v0.13.1-0.20240909215017-3823663aa4bb // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/iancoleman/orderedmap v0.0.0-20190318233801-ac98e3ecb4b0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.1-0.20220423185008-bf980b35cac4 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/rogpeppe/go-internal v1.12.0 // indirect
	github.com/spf13/cast v1.5.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonreference v0.0.0-20180127040603-bd5ef7bd5415 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	k8s.io/apimachinery v0.26.10 // indirect
)

replace github.com/dapr/components-contrib => ../
