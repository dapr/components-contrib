/*
Copyright 2021 The Dapr Authors
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

package nacos

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	defaultGroup           = "DEFAULT_GROUP"
	defaultTimeout         = 10 * time.Second
	metadataConfigID       = "config-id"
	metadataConfigGroup    = "config-group"
	metadataConfigOnchange = "config-onchange"
)

// Config type.
type configParam struct {
	dataID string
	group  string
}

// Nacos allows reading/writing to a Nacos server.
type Nacos struct {
	settings     Settings
	config       configParam
	watchesLock  sync.Mutex
	watches      []configParam
	servers      []constant.ServerConfig
	logger       logger.Logger
	configClient config_client.IConfigClient //nolint:nosnakecase
	readHandler  func(ctx context.Context, response *bindings.ReadResponse) ([]byte, error)
}

// NewNacos returns a new Nacos instance.
func NewNacos(logger logger.Logger) bindings.OutputBinding {
	return &Nacos{
		logger:      logger,
		watchesLock: sync.Mutex{},
	}
}

// Init implements InputBinding/OutputBinding's Init method.
func (n *Nacos) Init(metadata bindings.Metadata) error {
	n.settings = Settings{
		Timeout: defaultTimeout,
	}
	err := n.settings.Decode(metadata.Properties)
	if err != nil {
		return fmt.Errorf("nacos config error: %w", err)
	}

	if err = n.settings.Validate(); err != nil {
		return fmt.Errorf("nacos config error: %w", err)
	}

	if n.settings.Endpoint != "" {
		n.logger.Infof("nacos server url: %s", n.settings.Endpoint)
	} else if n.settings.NameServer != "" {
		n.logger.Infof("nacos nameserver: %s", n.settings.NameServer)
	}

	if n.settings.Config != "" {
		n.config, err = convertConfig(n.settings.Config)
		if err != nil {
			return err
		}
	}
	n.watches, err = convertConfigs(n.settings.Watches)
	if err != nil {
		return err
	}

	n.servers, err = convertServers(n.settings.Endpoint)
	if err != nil {
		return err
	}

	return n.createConfigClient()
}

func (n *Nacos) createConfigClient() error {
	logRollingConfig := constant.ClientLogRollingConfig{
		MaxSize: n.settings.MaxSize,
		MaxAge:  n.settings.MaxAge,
	}

	nacosConfig := map[string]interface{}{}
	nacosConfig["clientConfig"] = constant.ClientConfig{ //nolint:exhaustivestruct
		TimeoutMs:            uint64(n.settings.Timeout),
		NamespaceId:          n.settings.NamespaceID,
		Endpoint:             n.settings.NameServer,
		RegionId:             n.settings.RegionID,
		AccessKey:            n.settings.AccessKey,
		SecretKey:            n.settings.SecretKey,
		OpenKMS:              n.settings.AccessKey != "" && n.settings.SecretKey != "",
		CacheDir:             n.settings.CacheDir,
		UpdateThreadNum:      n.settings.UpdateThreadNum,
		NotLoadCacheAtStart:  n.settings.NotLoadCacheAtStart,
		UpdateCacheWhenEmpty: n.settings.UpdateCacheWhenEmpty,
		Username:             n.settings.Username,
		Password:             n.settings.Password,
		LogDir:               n.settings.LogDir,
		LogRollingConfig:     &logRollingConfig,
		LogLevel:             n.settings.LogLevel,
	}

	if len(n.servers) > 0 {
		nacosConfig["serverConfigs"] = n.servers
	}

	var err error
	n.configClient, err = clients.CreateConfigClient(nacosConfig)
	if err != nil {
		return fmt.Errorf("nacos config error: create config client failed. %w ", err)
	}

	return nil
}

// Read implements InputBinding's Read method.
func (n *Nacos) Read(ctx context.Context, handler bindings.Handler) error {
	n.readHandler = handler

	n.watchesLock.Lock()
	for _, watch := range n.watches {
		go n.startListen(ctx, watch)
	}
	n.watchesLock.Unlock()

	go func() {
		// Cancel all listeners when the context is done
		<-ctx.Done()
		n.cancelAllListeners()
	}()

	return nil
}

// Close implements cancel all listeners, see https://github.com/dapr/components-contrib/issues/779
func (n *Nacos) Close() error {
	n.cancelAllListeners()

	return nil
}

// Invoke implements OutputBinding's Invoke method.
func (n *Nacos) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case bindings.CreateOperation:
		return n.publish(ctx, req)
	case bindings.GetOperation:
		return n.fetch(ctx, req)
	case bindings.DeleteOperation, bindings.ListOperation:
		return nil, fmt.Errorf("nacos error: unsupported operation %s", req.Operation)
	default:
		return nil, fmt.Errorf("nacos error: unsupported operation %s", req.Operation)
	}
}

// Operations implements OutputBinding's Operations method.
func (n *Nacos) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation, bindings.GetOperation}
}

func (n *Nacos) startListen(ctx context.Context, config configParam) {
	n.fetchAndNotify(ctx, config)
	n.addListener(ctx, config)
}

func (n *Nacos) fetchAndNotify(ctx context.Context, config configParam) {
	content, err := n.configClient.GetConfig(vo.ConfigParam{
		DataId:   config.dataID,
		Group:    config.group,
		Content:  "",
		OnChange: nil,
	})
	if err != nil {
		n.logger.Warnf("failed to receive nacos config %s:%s, error: %v", config.dataID, config.group, err)
		return
	}
	n.notifyApp(ctx, config.group, config.dataID, content)
}

func (n *Nacos) addListener(ctx context.Context, config configParam) {
	err := n.configClient.ListenConfig(vo.ConfigParam{
		DataId:   config.dataID,
		Group:    config.group,
		Content:  "",
		OnChange: n.listener(ctx),
	})
	if err != nil {
		n.logger.Warnf("failed to add nacos listener for %s:%s, error: %v", config.dataID, config.group, err)
		return
	}
}

func (n *Nacos) addListenerFoInputBinding(ctx context.Context, config configParam) {
	if n.addToWatches(config) {
		go n.addListener(ctx, config)
	}
}

func (n *Nacos) publish(_ context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	nacosConfigParam, err := n.findConfig(req.Metadata)
	if err != nil {
		return nil, err
	}

	if _, err := n.configClient.PublishConfig(vo.ConfigParam{
		DataId:   nacosConfigParam.dataID,
		Group:    nacosConfigParam.group,
		Content:  string(req.Data),
		OnChange: nil,
	}); err != nil {
		return nil, fmt.Errorf("publish failed. %w", err)
	}

	return nil, nil
}

func (n *Nacos) fetch(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	nacosConfigParam, err := n.findConfig(req.Metadata)
	if err != nil {
		return nil, err
	}

	rst, err := n.configClient.GetConfig(vo.ConfigParam{
		DataId:   nacosConfigParam.dataID,
		Group:    nacosConfigParam.group,
		Content:  "",
		OnChange: nil,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch failed. err:%w", err)
	}

	if onchange := req.Metadata[metadataConfigOnchange]; strings.EqualFold(onchange, "true") {
		n.addListenerFoInputBinding(ctx, *nacosConfigParam)
	}

	return &bindings.InvokeResponse{Data: []byte(rst), Metadata: map[string]string{}}, nil
}

func (n *Nacos) addToWatches(c configParam) bool {
	n.watchesLock.Lock()
	defer n.watchesLock.Unlock()
	if n.watches != nil {
		for _, watch := range n.watches {
			if c.dataID == watch.dataID && c.group == watch.group {
				return false
			}
		}
	}
	n.watches = append(n.watches, c)

	return true
}

func (n *Nacos) findConfig(md map[string]string) (*configParam, error) {
	nacosConfigParam := n.config
	if _, ok := md[metadataConfigID]; ok {
		nacosConfigParam = configParam{
			dataID: md[metadataConfigID],
			group:  md[metadataConfigGroup],
		}
	}

	if nacosConfigParam.dataID == "" {
		return nil, fmt.Errorf("nacos config error: invalid metadata, no dataID found: %v", md)
	}
	if nacosConfigParam.group == "" {
		nacosConfigParam.group = defaultGroup
	}

	return &nacosConfigParam, nil
}

func (n *Nacos) listener(ctx context.Context) func(_, group, dataID, data string) {
	return func(_, group, dataID, data string) {
		n.notifyApp(ctx, group, dataID, data)
	}
}

func (n *Nacos) cancelAllListeners() {
	n.watchesLock.Lock()
	defer n.watchesLock.Unlock()
	for _, configParam := range n.watches {
		if err := n.cancelListener(configParam); err != nil {
			n.logger.Warnf("nacos cancel listener failed err: %v", err)
		}
	}
}

func (n *Nacos) cancelListener(configParam configParam) error {
	return n.configClient.CancelListenConfig(vo.ConfigParam{
		DataId: configParam.dataID,
		Group:  configParam.group,
	})
}

func (n *Nacos) notifyApp(ctx context.Context, group, dataID, content string) {
	metadata := map[string]string{
		metadataConfigID:    dataID,
		metadataConfigGroup: group,
	}
	var err error
	if n.readHandler != nil {
		n.logger.Debugf("binding-nacos read content to app")
		_, err = n.readHandler(ctx, &bindings.ReadResponse{Data: []byte(content), Metadata: metadata})
	} else {
		err = errors.New("nacos error: the InputBinding.Read handler not init")
	}

	if err != nil {
		n.logger.Errorf("nacos config %s:%s failed to notify application, error: %v", dataID, group, err)
	}
}

func convertConfig(s string) (configParam, error) {
	nacosConfigParam := configParam{dataID: "", group: ""}
	pair := strings.Split(s, ":")
	nacosConfigParam.dataID = strings.TrimSpace(pair[0])
	if len(pair) == 2 {
		nacosConfigParam.group = strings.TrimSpace(pair[1])
	}
	if nacosConfigParam.group == "" {
		nacosConfigParam.group = defaultGroup
	}
	if nacosConfigParam.dataID == "" {
		return nacosConfigParam, fmt.Errorf("nacos config error: invalid config keys, no config-id defined: %s", s)
	}

	return nacosConfigParam, nil
}

func convertConfigs(ss string) ([]configParam, error) {
	configs := make([]configParam, 0)
	if ss == "" {
		return configs, nil
	}

	for _, s := range strings.Split(ss, ",") {
		nacosConfigParam, err := convertConfig(s)
		if err != nil {
			return nil, err
		}
		configs = append(configs, nacosConfigParam)
	}

	return configs, nil
}

func convertServers(ss string) ([]constant.ServerConfig, error) {
	serverConfigs := make([]constant.ServerConfig, 0)
	if ss == "" {
		return serverConfigs, nil
	}

	array := strings.Split(ss, ",")
	for _, s := range array {
		cfg, err := parseServerURL(s)
		if err != nil {
			return serverConfigs, fmt.Errorf("parse url:%s error:%w", s, err)
		}
		serverConfigs = append(serverConfigs, *cfg)
	}

	return serverConfigs, nil
}

func parseServerURL(s string) (*constant.ServerConfig, error) {
	if !strings.HasPrefix(s, "http://") {
		s = "http://" + s
	}
	u, err := url.Parse(s)
	if err != nil {
		return nil, fmt.Errorf("nacos config error: server url %s error: %w", s, err)
	}

	port := uint64(80)
	if u.Scheme == "" {
		u.Scheme = "http"
	} else if u.Scheme == "https" {
		port = uint64(443)
	}

	if u.Port() != "" {
		port, err = strconv.ParseUint(u.Port(), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("nacos config error: server port %s err: %w", u.Port(), err)
		}
	}

	if u.Path == "" || u.Path == "/" {
		u.Path = "/nacos"
	}

	return &constant.ServerConfig{
		ContextPath: u.Path,
		IpAddr:      u.Hostname(),
		Port:        port,
		Scheme:      u.Scheme,
	}, nil
}
