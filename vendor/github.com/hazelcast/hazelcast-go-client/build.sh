#!/bin/bash

# Following packages will be installed by build.sh
# gocov: go get github.com/axw/gocov/gocov
# go2xunit: go get github.com/tebeka/go2xunit
# gocover-cobertura: go get github.com/t-yuki/gocover-cobertura
# golangci-lint: go get github.com/golangci/golangci-lint/cmd/golangci-lint
# testify : go get github.com/stretchr/testify

# Set up environment
export CLIENT_IMPORT_PATH="github.com/hazelcast/hazelcast-go-client"
export PACKAGE_LIST=$(go list -tags enterprise $CLIENT_IMPORT_PATH/... | grep -vE ".*/test|.*/compatibility|.*/rc|.*/sample" | sed -e 'H;${x;s/\n/,/g;s/^,//;p;};d')

set -ex


if [ -d $GOPATH/src/github.com/apache/thrift/  ]; then
    echo "thrift already exists, not downloading."
else
    go get github.com/apache/thrift/lib/go/thrift
    pushd $GOPATH/src/github.com/apache/thrift
    git fetch --tags --quiet
    git checkout 0.10.0
    popd
fi

pushd $GOPATH/src/$CLIENT_IMPORT_PATH
go build
popd

#run linter
pushd $GOPATH/src/$CLIENT_IMPORT_PATH
bash ./linter.sh

if [ "$?" != "0" ]; then
    exit 1
fi
popd

bash ./start-rc.sh

sleep 10

go get github.com/t-yuki/gocover-cobertura
go get github.com/tebeka/go2xunit
go get github.com/stretchr/testify

# Run tests (JUnit plugin)
echo "mode: atomic" > coverage.out

for pkg in $(go list -tags enterprise $CLIENT_IMPORT_PATH/...);
do
  echo "testing... $pkg"
  if [ -n "${ENTERPRISE_TESTS_ENABLED}" ]; then
    go test -race -tags enterprise -covermode=atomic  -v -coverprofile=tmp.out -coverpkg ${PACKAGE_LIST} $pkg | tee -a test.out
  else
    go test -race -covermode=atomic  -v -coverprofile=tmp.out -coverpkg ${PACKAGE_LIST} $pkg | tee -a test.out
  fi
  if [ -f tmp.out ]; then
     cat tmp.out | grep -v "mode: atomic" >> coverage.out | echo
  fi
done

rm -f ./tmp.out

cat test.out | go2xunit -output tests.xml

# Generate coverage reports (Cobertura plugin)
gocover-cobertura < coverage.out > cobertura-coverage.xml

## Run lint tools (Compiler warning plugin)
#golint $PRJ > lint.txt
