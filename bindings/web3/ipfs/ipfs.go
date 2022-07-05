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

package ipfs

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	ipfs_httpclient "github.com/ipfs/go-ipfs-http-client"
	ipfs_core "github.com/ipfs/go-ipfs/core"
	ipfs_coreapi "github.com/ipfs/go-ipfs/core/coreapi"
	ipfs_libp2p "github.com/ipfs/go-ipfs/core/node/libp2p"
	ipfs_loader "github.com/ipfs/go-ipfs/plugin/loader"
	ipfs_repo "github.com/ipfs/go-ipfs/repo"
	ipfs_fsrepo "github.com/ipfs/go-ipfs/repo/fsrepo"
	ipfs_icore "github.com/ipfs/interface-go-ipfs-core"
	"github.com/multiformats/go-multiaddr"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const swarmKeyFile = "swarm.key"

var (
	loadPluginsOnce sync.Once
	httpClient      *http.Client
)

func init() {
	httpClient = &http.Client{
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 5 * time.Second,
			}).Dial,
		},
	}
}

// IPFSBinding is a binding for interacting with an IPFS network.
type IPFSBinding struct {
	metadata ipfsMetadata
	ipfsAPI  ipfs_icore.CoreAPI
	ipfsNode *ipfs_core.IpfsNode
	ipfsRepo ipfs_repo.Repo
	ctx      context.Context
	cancel   context.CancelFunc
	logger   logger.Logger
}

// NewIPFSBinding returns a new IPFSBinding.
func NewIPFSBinding(logger logger.Logger) *IPFSBinding {
	return &IPFSBinding{
		logger: logger,
	}
}

// Init the binding.
func (b *IPFSBinding) Init(metadata bindings.Metadata) (err error) {
	b.ctx, b.cancel = context.WithCancel(context.Background())

	err = b.metadata.FromMap(metadata.Properties)
	if err != nil {
		return err
	}

	if b.metadata.ExternalAPI == "" {
		var onceErr error
		loadPluginsOnce.Do(func() {
			onceErr = setupPlugins("")
		})
		if onceErr != nil {
			return onceErr
		}

		err = b.createNode()
		if err != nil {
			return fmt.Errorf("failed to start IPFS node: %v", err)
		}
	} else {
		if b.metadata.ExternalAPI[0] == '/' {
			var maddr multiaddr.Multiaddr
			maddr, err = multiaddr.NewMultiaddr(b.metadata.ExternalAPI)
			if err != nil {
				return fmt.Errorf("failed to parse external API multiaddr: %v", err)
			}
			b.ipfsAPI, err = ipfs_httpclient.NewApiWithClient(maddr, httpClient)
		} else {
			b.ipfsAPI, err = ipfs_httpclient.NewURLApiWithClient(b.metadata.ExternalAPI, httpClient)
		}
		if err != nil {
			return fmt.Errorf("failed to initialize external IPFS API: %v", err)
		}
		b.logger.Infof("Using IPFS APIs at %s", b.metadata.ExternalAPI)
	}

	return nil
}

func (b *IPFSBinding) Close() (err error) {
	if b.cancel != nil {
		b.cancel()
		b.cancel = nil
	}
	if b.ipfsNode != nil {
		err = b.ipfsNode.Close()
		if err != nil {
			b.logger.Errorf("Error while closing IPFS node: %v", err)
		}
		b.ipfsNode = nil
	}
	if b.ipfsRepo != nil {
		err = b.ipfsRepo.Close()
		if err != nil {
			b.logger.Errorf("Error while closing IPFS repo: %v", err)
		}
		b.ipfsRepo = nil
	}

	return nil
}

func (b *IPFSBinding) createNode() error {
	// Init the repo if needed
	if !ipfs_fsrepo.IsInitialized(b.metadata.RepoPath) {
		cfg, err := b.metadata.IPFSConfig()
		if err != nil {
			return err
		}
		err = ipfs_fsrepo.Init(b.metadata.RepoPath, cfg)
		if err != nil {
			return err
		}
		if b.metadata.SwarmKey != "" {
			skPath := filepath.Join(b.metadata.RepoPath, swarmKeyFile)
			err = os.WriteFile(skPath, []byte(b.metadata.SwarmKey), 0600)
			if err != nil {
				return fmt.Errorf("error writing swarm key to file '%s': %v", skPath, err)
			}
		}
		b.logger.Infof("Initialized a new IPFS repo at path %s", b.metadata.RepoPath)
	}

	// Open the repo
	repo, err := ipfs_fsrepo.Open(b.metadata.RepoPath)
	if err != nil {
		return err
	}
	b.logger.Infof("Opened IPFS repo at path %s", b.metadata.RepoPath)

	// Create the node
	nodeOptions := &ipfs_core.BuildCfg{
		Online: true,
		Repo:   repo,
	}
	r := strings.ToLower(b.metadata.Routing)
	switch r {
	case "", "dht":
		nodeOptions.Routing = ipfs_libp2p.DHTOption
	case "dhtclient":
		nodeOptions.Routing = ipfs_libp2p.DHTClientOption
	case "dhtserver":
		nodeOptions.Routing = ipfs_libp2p.DHTServerOption
	case "none":
		nodeOptions.Routing = ipfs_libp2p.NilRouterOption
	default:
		return fmt.Errorf("invalid value for metadata property 'routing'")
	}
	b.ipfsNode, err = ipfs_core.NewNode(b.ctx, nodeOptions)
	if err != nil {
		return err
	}

	b.logger.Infof("Started IPFS node %s", b.ipfsNode.Identity)

	// Init API
	b.ipfsAPI, err = ipfs_coreapi.NewCoreAPI(b.ipfsNode)
	if err != nil {
		return err
	}

	return nil
}

// Operations returns the supported operations for this binding.
func (b *IPFSBinding) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.GetOperation,
		bindings.CreateOperation, // alias for "add"
		bindings.DeleteOperation, // alias for "pin-rm"
		"add",
		"pin-add",
		"pin-rm",
		"pin-ls",
	}
}

// Invoke performs an HTTP request to the configured HTTP endpoint.
func (b *IPFSBinding) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case bindings.GetOperation:
		return b.getOperation(ctx, req)
	case "add", bindings.CreateOperation:
		return b.addOperation(ctx, req)
	case "pin-add":
		return b.pinAddOperation(ctx, req)
	case "pin-ls":
		return b.pinLsOperation(ctx, req)
	case "pin-rm", bindings.DeleteOperation:
		return b.pinRmOperation(ctx, req)
	}
	return &bindings.InvokeResponse{
		Data:     nil,
		Metadata: nil,
	}, nil
}

func setupPlugins(externalPluginsPath string) error {
	plugins, err := ipfs_loader.NewPluginLoader("")
	if err != nil {
		return fmt.Errorf("error loading plugins: %s", err)
	}
	if err := plugins.Initialize(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}
	if err := plugins.Inject(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}
	return nil
}
