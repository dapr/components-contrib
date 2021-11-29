package file

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/gofrs/flock"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/config"
	"github.com/dapr/kit/logger"
)

const (
	dirKey           = "dir"
	defaultDaprDir   = ".dapr"
	defaultNamingDir = "naming"
)

type namingInfo struct {
	AppID        string `json:"APP_ID"`
	HostAddress  string `json:"HOST_ADDRESS"`
	AppPort      string `json:"APP_PORT"`
	DaprPort     string `json:"DAPR_PORT"`
	DaprHTTPPort string `json:"DAPR_HTTP_PORT"`
}

func createNamingInfo(metadata nameresolution.Metadata) *namingInfo {
	return &namingInfo{
		AppID:        metadata.Properties[nameresolution.AppID],
		HostAddress:  metadata.Properties[nameresolution.HostAddress],
		AppPort:      metadata.Properties[nameresolution.AppPort],
		DaprPort:     metadata.Properties[nameresolution.DaprPort],
		DaprHTTPPort: metadata.Properties[nameresolution.DaprHTTPPort],
	}
}

type resolver struct {
	logger logger.Logger
	dir    string
}

// NewResolver creates file-based name resolver.
func NewResolver(logger logger.Logger) nameresolution.Resolver {
	if home, err := os.UserHomeDir(); err == nil {
		return &resolver{
			logger: logger,
			dir:    filepath.Join(home, defaultDaprDir, defaultNamingDir),
		}
	}

	return &resolver{
		logger: logger,
	}
}

// Init registers service for file-based name resolver.
func (r *resolver) Init(metadata nameresolution.Metadata) error {
	if err := r.prepareResolverDir(metadata); err != nil {
		return err
	}

	info := createNamingInfo(metadata)

	fileLock := r.newFileLock(info.AppID)
	if err := fileLock.Lock(); err != nil {
		r.logger.Errorf("fail to lock: %s", fileLock.Path())
		return err
	}

	defer func(lck *flock.Flock) {
		if err := lck.Unlock(); err != nil {
			r.logger.Errorf("fail to unlock: %s, with error: %s", lck.Path(), err)
		}
	}(fileLock)

	f := filepath.Join(r.dir, info.AppID)
	namingInfos, err := loadNamingInfo(f)
	if err != nil {
		r.logger.Errorf("fail to load naming info from file: %s", f)
		return err
	}

	index := -1
	for i, ni := range namingInfos {
		if ni.AppID == info.AppID &&
			ni.HostAddress == info.HostAddress &&
			ni.DaprPort == info.DaprPort &&
			ni.DaprHTTPPort == info.DaprHTTPPort {
			index = i
			break
		}
	}

	if index != -1 {
		namingInfos = append(namingInfos[:index], namingInfos[index+1:]...)
	}

	namingInfos = append(namingInfos, *info)
	content, _ := json.MarshalIndent(namingInfos, "", "\t")
	if err := ioutil.WriteFile(f, content, os.ModePerm); err != nil {
		r.logger.Errorf("fail to write naming info into file: %s", f)
		return err
	}

	return nil
}

func loadNamingInfo(filename string) ([]namingInfo, error) {
	if _, err := os.Stat(filename); err == nil {
		if bytes, err := ioutil.ReadFile(filename); err == nil {
			infos := make([]namingInfo, 0)
			if err := json.Unmarshal(bytes, &infos); err == nil {
				return infos, nil
			}
			return nil, err
		}
		return nil, err
	}

	return make([]namingInfo, 0), nil
}

func (r *resolver) prepareResolverDir(metadata nameresolution.Metadata) error {
	configs, err := config.Normalize(metadata.Configuration)
	if err != nil {
		return err
	}

	if conf, ok := configs.(map[string]interface{}); ok {
		if v := conf[dirKey]; v != nil {
			if s, ok := v.(string); ok {
				r.dir = s
			}
		}
	}

	if r.dir == "" {
		return errors.New("file based naming directory is not defined")
	}

	if err = os.MkdirAll(r.dir, os.ModePerm); err != nil {
		return err
	}

	return nil
}

// ResolveID resolves name to address via file.
func (r *resolver) ResolveID(req nameresolution.ResolveRequest) (string, error) {
	lock := r.newFileLock(req.ID)
	if err := lock.RLock(); err != nil {
		r.logger.Errorf("fail to lock file: %s", lock.Path())
		return "", err
	}
	defer func(lock *flock.Flock) {
		if err := lock.Unlock(); err != nil {
			r.logger.Errorf("fail to unlock file: %s with error: %s", lock.Path(), err)
		}
	}(lock)

	fn := filepath.Join(r.dir, req.ID)
	info, err := loadNamingInfo(fn)
	if err != nil {
		return "", err
	}
	if len(info) == 0 {
		return "", fmt.Errorf("there's no naming info for application: %s, pls. check file: %s", req.ID, lock)
	}

	index := rand.Intn(len(info))

	return fmt.Sprintf("%s:%s", info[index].HostAddress, info[index].DaprPort), nil
}

func (r *resolver) newFileLock(id string) *flock.Flock {
	return flock.New(filepath.Join(r.dir, id+".lock"))
}
