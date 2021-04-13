// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package nacos

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/vo"
)

const (
	defaultGroup           = "DEFAULT_GROUP"
	metadataConfigID       = "config-id"
	metadataConfigGroup    = "config-group"
	metadataConfigOnchange = "config-onchange"
)

// Config type
type configParam struct {
	dataID string
	group  string
}

// Nacos allows reading/writing to a Nacos server
type Nacos struct {
	mu           sync.Mutex
	name         string
	init         bool
	metadata     *nacosMetadata
	config       configParam
	watches      []configParam
	servers      []constant.ServerConfig
	logger       logger.Logger
	configClient config_client.IConfigClient
	readHandler  func(response *bindings.ReadResponse) ([]byte, error)
}

// NewNacos returns a new Nacos instance
func NewNacos(logger logger.Logger) *Nacos {
	return &Nacos{logger: logger} //nolint:exhaustivestruct
}

// Init implements InputBinding/OutputBinding's Init method
func (n *Nacos) Init(metadata bindings.Metadata) (err error) {
	if n.init {
		return nil
	}
	n.init = true
	n.name = metadata.Name

	n.metadata, err = parseMetadata(metadata)
	if err != nil {
		err = fmt.Errorf("nacos config error: %w", err)

		return
	}

	if n.metadata.TimeoutMs <= 0 {
		n.metadata.TimeoutMs = 10000
	}

	if n.metadata.Endpoint != "" {
		n.logger.Infof("nacos server's url: %s", n.metadata.Endpoint)
	} else if n.metadata.NameServer != "" {
		n.logger.Infof("nacos nameserver: %s", n.metadata.NameServer)
	} else {
		err = errors.New("nacos config error: must config endpoint or nameserver")

		return
	}

	if n.metadata.Config != "" {
		n.config, err = convertConfig(n.metadata.Config)
		if err != nil {
			return
		}
	}
	n.watches, err = convertConfigs(n.metadata.Watches)
	if err != nil {
		return
	}

	n.servers, err = convertServers(n.metadata.Endpoint)
	if err != nil {
		return
	}

	return n.createConfigClient()
}

func (n *Nacos) createConfigClient() error {
	config := map[string]interface{}{}
	config["clientConfig"] = constant.ClientConfig{ //nolint:exhaustivestruct
		TimeoutMs:            uint64(n.metadata.TimeoutMs),
		NamespaceId:          n.metadata.NamespaceID,
		Endpoint:             n.metadata.NameServer,
		RegionId:             n.metadata.RegionID,
		AccessKey:            n.metadata.AccessKey,
		SecretKey:            n.metadata.SecretKey,
		OpenKMS:              n.metadata.AccessKey != "" && n.metadata.SecretKey != "",
		CacheDir:             n.metadata.CacheDir,
		UpdateThreadNum:      n.metadata.UpdateThreadNum,
		NotLoadCacheAtStart:  n.metadata.NotLoadCacheAtStart,
		UpdateCacheWhenEmpty: n.metadata.UpdateCacheWhenEmpty,
		Username:             n.metadata.Username,
		Password:             n.metadata.Password,
		LogDir:               n.metadata.LogDir,
		RotateTime:           n.metadata.RotateTime,
		MaxAge:               int64(n.metadata.MaxAge),
		LogLevel:             n.metadata.LogLevel,
	}

	if len(n.servers) > 0 {
		config["serverConfigs"] = n.servers
	}

	var err error
	n.configClient, err = clients.CreateConfigClient(config)
	if err != nil {
		return fmt.Errorf("nacos config error: create config client failed. %w ", err)
	}

	return nil
}

// Read implements InputBinding's Read method
func (n *Nacos) Read(handler func(*bindings.ReadResponse) ([]byte, error)) error {
	n.readHandler = handler

	for _, watch := range n.watches {
		go n.startListen(watch)
	}

	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
	<-exitChan
	n.cancelListener()

	return nil
}

// Invoke implements OutputBinding's Invoke method
func (n *Nacos) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case bindings.CreateOperation:
		return n.publish(req)
	case bindings.GetOperation:
		return n.fetch(req)
	case bindings.DeleteOperation, bindings.ListOperation:
		return nil, fmt.Errorf("nacos error: unsupported operation %s", req.Operation)
	default:
		return nil, fmt.Errorf("nacos error: unsupported operation %s", req.Operation)
	}
}

// Operations implements OutputBinding's Operations method
func (n *Nacos) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation, bindings.GetOperation}
}

func (n *Nacos) startListen(config configParam) {
	n.fetchAndNotify(&config)
	n.addListener(&config)
}

func (n *Nacos) fetchAndNotify(config *configParam) {
	content, err := n.configClient.GetConfig(vo.ConfigParam{
		DataId:   config.dataID,
		Group:    config.group,
		Content:  "",
		DatumId:  "",
		OnChange: nil,
	})
	if err != nil {
		n.logger.Warnf("failed to receive nacos config %s:%s, error: %v", config.dataID, config.group, err)
	} else {
		n.notifyApp(config.group, config.dataID, content)
	}
}

func (n *Nacos) addListener(config *configParam) {
	err := n.configClient.ListenConfig(vo.ConfigParam{
		DataId:   config.dataID,
		Group:    config.group,
		Content:  "",
		DatumId:  "",
		OnChange: n.listener,
	})
	if err != nil {
		n.logger.Warnf("failed to add nacos listener for %s:%s, error: %v", config.dataID, config.group, err)
	}
}

func (n *Nacos) addListener4InputBinding(config *configParam) {
	if n.addToWatches(config) {
		n.addListener(config)
	}
}

func (n *Nacos) publish(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	config, err := n.findConfig(req.Metadata)
	if err != nil {
		return nil, err
	}

	if _, err := n.configClient.PublishConfig(vo.ConfigParam{
		DataId:   config.dataID,
		Group:    config.group,
		Content:  string(req.Data),
		DatumId:  "",
		OnChange: nil,
	}); err != nil {
		return nil, fmt.Errorf("publish failed. %w", err)
	}

	return nil, nil
}

func (n *Nacos) fetch(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	config, err := n.findConfig(req.Metadata)
	if err != nil {
		return nil, err
	}

	rst, err := n.configClient.GetConfig(vo.ConfigParam{
		DataId:   config.dataID,
		Group:    config.group,
		Content:  "",
		DatumId:  "",
		OnChange: nil,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch failed. err:%w", err)
	}

	if onchange := req.Metadata[metadataConfigOnchange]; onchange == "true" || onchange == "TRUE" {
		go n.addListener4InputBinding(config)
	}

	return &bindings.InvokeResponse{Data: []byte(rst), Metadata: map[string]string{}}, nil
}

func (n *Nacos) addToWatches(c *configParam) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.watches != nil {
		for _, watch := range n.watches {
			if c.dataID == watch.dataID && c.group == watch.group {
				return false
			}
		}
	}
	n.watches = append(n.watches, *c)

	return true
}

func (n *Nacos) findConfig(md map[string]string) (*configParam, error) {
	config := n.config
	if _, ok := md[metadataConfigID]; ok {
		config = configParam{
			dataID: md[metadataConfigID],
			group:  md[metadataConfigGroup],
		}
	}

	if config.dataID == "" {
		return nil, fmt.Errorf("nacos config error: invalid metadata, no dataID found: %v", md)
	}
	if config.group == "" {
		config.group = defaultGroup
	}

	return &config, nil
}

func (n *Nacos) listener(_, group, dataID, data string) {
	n.notifyApp(group, dataID, data)
}

func (n *Nacos) cancelListener() {
	for _, config := range n.watches {
		if err := n.configClient.CancelListenConfig(vo.ConfigParam{ //nolint:exhaustivestruct
			DataId: config.dataID,
			Group:  config.group,
		}); err != nil {
			n.logger.Warnf("nacos cancel listener failed err: %v", err)
		}
	}
}

func (n *Nacos) notifyApp(group, dataID, content string) {
	metadata := make(map[string]string, 2)
	metadata[metadataConfigID] = dataID
	metadata[metadataConfigGroup] = group
	var err error
	if n.readHandler != nil {
		n.logger.Debugf("binding-nacos read content to app")
		_, err = n.readHandler(&bindings.ReadResponse{Data: []byte(content), Metadata: metadata})
	} else {
		err = errors.New("nacos error: the InputBinding.Read handler not init")
	}

	if err != nil {
		n.logger.Errorf("nacos config %s:%s failed to notify application, error: %v", dataID, group, err)
	}
}

func parseMetadata(metadata bindings.Metadata) (*nacosMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, fmt.Errorf("parse error:%w", err)
	}

	var md nacosMetadata
	err = json.Unmarshal(b, &md)
	if err != nil {
		return nil, fmt.Errorf("parse error:%w", err)
	}

	return &md, nil
}

func convertConfig(s string) (configParam, error) {
	config := configParam{dataID: "", group: ""}
	pair := strings.Split(s, ":")
	config.dataID = strings.TrimSpace(pair[0])
	if len(pair) == 2 {
		config.group = strings.TrimSpace(pair[1])
	}
	if config.group == "" {
		config.group = defaultGroup
	}
	if config.dataID == "" || config.group == "" {
		return config, fmt.Errorf("nacos config error: invalid config keys, no config-id defined: %s", s)
	}

	return config, nil
}

func convertConfigs(ss string) ([]configParam, error) {
	configs := make([]configParam, 0)
	if ss == "" {
		return configs, nil
	}

	for _, s := range strings.Split(ss, ",") {
		config, err := convertConfig(s)
		if err != nil {
			return nil, err
		}
		configs = append(configs, config)
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
			return serverConfigs, fmt.Errorf("%w", err)
		}
		serverConfigs = append(serverConfigs, *cfg)
	}

	return serverConfigs, nil
}

func parseServerURL(s string) (*constant.ServerConfig, error) {
	if !strings.HasPrefix(s, "http") {
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
