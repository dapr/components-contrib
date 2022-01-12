package file

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"path"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
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
	logger      logger.Logger
	dir         string
	namingInfos map[string][]*namingInfo
}

// NewResolver creates file-based name resolver.
func NewResolver(logger logger.Logger) nameresolution.Resolver {
	if home, err := os.UserHomeDir(); err == nil {
		return &resolver{
			logger:      logger,
			dir:         filepath.Join(home, defaultDaprDir, defaultNamingDir),
			namingInfos: make(map[string][]*namingInfo),
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

	namingInfos = r.removeExistingInfoIfNecessary(namingInfos, info)
	namingInfos = append(namingInfos, info)
	content, _ := json.MarshalIndent(namingInfos, "", "\t")
	if err := ioutil.WriteFile(f, content, os.ModePerm); err != nil {
		r.logger.Errorf("fail to write naming info into file: %s", f)
		return err
	}

	r.namingInfos[info.AppID] = namingInfos

	return r.watchNamingInfos()
}

func (r *resolver) watchNamingInfos() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		r.logger.Warnf("file watcher failed to setup, the naming info may not keep to update")
		return err
	}

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}

				if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
					r.logger.Infof("naming info file %s changed, will update", event.Name)
					id := path.Base(event.Name)
					infos, err2 := r.loadNamingInfo(id)
					if err2 != nil {
						r.logger.Warnf("failed to reload naming info for app: %s", id)
					} else {
						r.namingInfos[id] = infos
					}
				}

			case err2, ok := <-watcher.Errors:
				if !ok {
					return
				}

				r.logger.Warnf("file watcher for naming info encounter error: %s", err2)
			}
		}
	}()

	err = watcher.Add(r.dir)
	if err != nil {
		r.logger.Warnf("fail to add %s to naming info watch list", r.dir)
		_ = watcher.Close()

		return err
	}

	return nil
}

func (r *resolver) removeExistingInfoIfNecessary(namingInfos []*namingInfo, info *namingInfo) []*namingInfo {
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

	return namingInfos
}

func (r *resolver) loadNamingInfo(appID string) ([]*namingInfo, error) {
	lock := r.newFileLock(appID)
	if err := lock.RLock(); err != nil {
		r.logger.Errorf("fail to lock file: %s", lock.Path())
		return nil, err
	}
	defer func(lock *flock.Flock) {
		if err := lock.Unlock(); err != nil {
			r.logger.Errorf("fail to unlock file: %s with error: %s", lock.Path(), err)
		}
	}(lock)

	return loadNamingInfo(filepath.Join(r.dir, appID))
}

func loadNamingInfo(fn string) ([]*namingInfo, error) {
	_, err := os.Stat(fn)
	if err != nil && os.IsNotExist(err) {
		return make([]*namingInfo, 0), nil
	}

	bytes, err := ioutil.ReadFile(fn)
	if err != nil {
		return nil, err
	}

	infos := make([]*namingInfo, 0)
	err = json.Unmarshal(bytes, &infos)
	if err != nil {
		return nil, err
	}

	return infos, nil
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
	info := r.namingInfos[req.ID]
	if info == nil {
		var err error
		info, err = r.loadNamingInfo(req.ID)
		if err != nil {
			return "", err
		}
	}

	if len(info) == 0 {
		return "", fmt.Errorf("there's no naming info for application: %s, pls. check dir: %s", req.ID, r.dir)
	}

	rnd, _ := rand.Int(rand.Reader, big.NewInt(int64(len(info))))
	index := rnd.Int64()

	return fmt.Sprintf("%s:%s", info[index].HostAddress, info[index].DaprPort), nil
}

func (r *resolver) newFileLock(id string) *flock.Flock {
	return flock.New(filepath.Join(r.dir, id+".lock"))
}
