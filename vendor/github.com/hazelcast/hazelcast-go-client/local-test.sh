#!/bin/bash

# Set up environment
export CLIENT_IMPORT_PATH="github.com/hazelcast/hazelcast-go-client"
export PACKAGE_LIST=$(go list $CLIENT_IMPORT_PATH/... | grep -vE ".*/tests|.*/compatibility|.*/rc|.*/samples" | sed -e 'H;${x;s/\n/,/g;s/^,//;p;};d')
echo $PACKAGE_LIST

#run linter
pushd $GOPATH/src/$CLIENT_IMPORT_PATH
bash ./linter.sh

if [ "$?" != "0" ]; then
    exit 1
fi
popd
set -ex

HZ_VERSION="3.11-SNAPSHOT"

HAZELCAST_TEST_VERSION=${HZ_VERSION}
HAZELCAST_VERSION=${HZ_VERSION}
HAZELCAST_ENTERPRISE_VERSION=${HZ_VERSION}
HAZELCAST_RC_VERSION="0.4-SNAPSHOT"

CLASSPATH="hazelcast-remote-controller-${HAZELCAST_RC_VERSION}.jar:hazelcast-${HAZELCAST_TEST_VERSION}-tests.jar"
CLASSPATH="hazelcast-enterprise-${HAZELCAST_ENTERPRISE_VERSION}.jar:"${CLASSPATH}
CLASSPATH="hazelcast-${HAZELCAST_VERSION}.jar:"${CLASSPATH}
echo "Starting Remote Controller ... oss ..."

go build

java -cp ${CLASSPATH} com.hazelcast.remotecontroller.Main&
serverPid=$!
echo ${serverPid}

sleep 10

# Run tests (JUnit plugin)
echo "mode: set" > coverage.out
for pkg in $(go list ./...);
do
    if [[ $pkg != *"vendor"* ]]; then
      echo "testing... $pkg"
      go test -v -race -coverprofile=tmp.out $pkg >> test.out
      if [ -f tmp.out ]; then
         cat tmp.out | grep -v "mode: set" >> coverage.out | echo
      fi
    fi
done
rm -f ./tmp.out

kill -9 ${serverPid}
