package file

import (
	"fmt"
	nr "github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"testing"
)

func TestDefaultDir(t *testing.T) {
	r := NewResolver(logger.NewLogger("test")).(*resolver)
	home, _ := os.UserHomeDir()
	assert.Equal(t, filepath.Join(home, defaultDaprDir, defaultNamingDir), r.dir)
}

func TestPrepareResolverDir(t *testing.T) {
	r := NewResolver(logger.NewLogger("test")).(*resolver)

	t.Run("default dir", func(t *testing.T) {
		t.Logf("base directory is: %s", r.dir)
		defer func() {
			_ = os.RemoveAll(r.dir)
		}()
		resolverMetadata := nr.Metadata{}
		err := r.prepareResolverDir(resolverMetadata)
		assert.Nil(t, err)
		_, err = os.Stat(r.dir)
		assert.Nil(t, err)
	})

	t.Run("customize dir", func(t *testing.T) {
		baseDir := filepath.Join(os.TempDir(), "dapr", "naming")
		t.Logf("base directory is: %s", baseDir)
		defer func() {
			_ = os.RemoveAll(baseDir)
		}()
		resolverMetadata := nr.Metadata{
			Configuration: map[string]interface{}{
				dirKey: baseDir,
			},
		}

		err := r.prepareResolverDir(resolverMetadata)
		assert.Nil(t, err)
		_, err = os.Stat(baseDir)
		assert.Nil(t, err)
		assert.Equal(t, baseDir, r.dir)
	})

	t.Run("empty dir", func(t *testing.T) {
		r.dir = ""
		t.Logf("base directory is: %s", r.dir)
		resolverMetadata := nr.Metadata{}
		err := r.prepareResolverDir(resolverMetadata)
		assert.NotNil(t, err)
	})
}

func TestInitAndResolveID(t *testing.T) {
	r := NewResolver(logger.NewLogger("test"))
	baseDir := filepath.Join(os.TempDir(), "dapr", "naming")
	t.Logf("base directory is: %s", baseDir)

	t.Run("one app instance", func(t *testing.T) {
		defer func() {
			_ = os.RemoveAll(baseDir)
		}()

		resolverMetadata := buildMetadata("helloapp", "192.168.1.127", "3000", "63478", "3500", baseDir)
		err := r.Init(resolverMetadata)
		assert.Nil(t, err)
		infos, err := loadNamingInfo(filepath.Join(baseDir, "helloapp"))
		assert.Nil(t, err)
		assert.Equal(t, 1, len(infos))
		assert.Equal(t, namingInfo{
			AppID:        "helloapp",
			HostAddress:  "192.168.1.127",
			AppPort:      "3000",
			DaprPort:     "63478",
			DaprHTTPPort: "3500",
		}, infos[0])

		request := nr.ResolveRequest{
			ID:   "helloapp",
			Port: 12345,
		}
		result, err := r.ResolveID(request)
		assert.Nil(t, err)
		assert.Equal(t, "192.168.1.127:63478", result)
	})

	t.Run("many app instances", func(t *testing.T) {
		defer func() {
			_ = os.RemoveAll(baseDir)
		}()

		wg := sync.WaitGroup{}
		wg.Add(100)
		for i := 0; i < 100; i++ {
			go func(i int) {
				resolverMetadata := buildMetadata("helloapp",
					fmt.Sprintf("addr-%d", i),
					fmt.Sprintf("app-port-%d", i),
					fmt.Sprintf("grpc-port-%d", i),
					fmt.Sprintf("http-port-%d", i),
					baseDir)
				err := r.Init(resolverMetadata)
				assert.Nil(t, err)
				wg.Done()
			}(i)
		}

		wg.Wait()
		infos, err := loadNamingInfo(filepath.Join(baseDir, "helloapp"))
		assert.Nil(t, err)
		assert.Equal(t, 100, len(infos))

		for i := 0; i < 100; i++ {
			info := infos[i]
			suffix := info.HostAddress[strings.LastIndex(info.HostAddress, "-")+1:]
			assert.Equal(t, "app-port-"+suffix, info.AppPort)
			assert.Equal(t, "grpc-port-"+suffix, info.DaprPort)
			assert.Equal(t, "http-port-"+suffix, info.DaprHTTPPort)
		}

		request := nr.ResolveRequest{
			ID:   "helloapp",
			Port: 12345,
		}

		re := regexp.MustCompile("addr-(\\d+):grpc-port-(\\d+)")
		for i := 0; i < 100; i++ {
			result, err := r.ResolveID(request)
			assert.Nil(t, err)
			ret := re.FindStringSubmatch(result)
			assert.Equal(t, ret[1], ret[2])
		}
	})
}

func buildMetadata(appId string, addr string, appPort string, grpcPort string, httpPort string, baseDir string) nr.Metadata {
	resolverMetadata := nr.Metadata{
		Properties: map[string]string{
			nr.AppID:        appId,
			nr.HostAddress:  addr,
			nr.AppPort:      appPort,
			nr.DaprPort:     grpcPort,
			nr.DaprHTTPPort: httpPort,
		},
		Configuration: map[string]interface{}{
			dirKey: baseDir,
		},
	}
	return resolverMetadata
}
