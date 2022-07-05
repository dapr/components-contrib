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
	"fmt"
	"io"
	"strings"

	ipfs_config "github.com/ipfs/go-ipfs/config"
	ipfs_fsrepo "github.com/ipfs/go-ipfs/repo/fsrepo"
	ipfs_options "github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/dapr/kit/config"
)

type ipfsMetadata struct {
	// Path where to store the IPFS repository
	// It will be initialized automatically if needed
	// Defaults to the "best known path" set by IPFS
	RepoPath string `mapstructure:"repoPath"`

	// If set, uses an external IPFS daemon, connecting to its APIs
	// Can be a HTTP(S) address or a multi-address
	// If set, a local node will not be initialized
	ExternalAPI string `mapstructure:"externalAPI"`

	// The options below can only be set when a new local repo is being initialized

	// List of bootstrap nodes, as a comma-separated string
	// If empty, defaults to the official bootstrap nodes provided by the IPFS project
	// Users should not modify this unless they're using a private cluster
	BootstrapNodes string `mapstructure:"bootstrapNodes"`
	// Swarm key to use for connecting to private IPFS networks
	// If empty, the node will connect to the default, public IPFS network
	// Generate with https://github.com/Kubuxu/go-ipfs-swarm-key-gen
	// When using a swarm key, users should also configure the bootstrap nodes
	SwarmKey string `mapstructure:"swarmKey"`
	// Routing mode: "dht" (default) or "dhtclient"
	Routing string `mapstructure:"routing"`
	// Max local storage used
	// Default: the default value used by go-ipfs (currently, "10GB")
	StorageMax string `mapstructure:"storageMax"`
	// Watermark for running garbage collection, 0-100 (as a percentage)
	// Default: the default value used by go-ipfs (currently, 90)
	StorageGCWatermark int64 `mapstructure:"storageGCWatermark"`
	// Interval for running garbage collection
	// Default: the default value used by go-ipfs (currently, "1h")
	StorageGCPeriod string `mapstructure:"storageGCPeriod"`
}

// FromMap initializes the metadata object from a map.
func (m *ipfsMetadata) FromMap(mp map[string]string) (err error) {
	if len(mp) > 0 {
		err = config.Decode(mp, m)
		if err != nil {
			return err
		}
	}

	if m.RepoPath == "" {
		m.RepoPath, err = ipfs_fsrepo.BestKnownPath()
		if err != nil {
			return fmt.Errorf("error determining the best known repo path: %v", err)
		}
	}

	return nil
}

// IPFSConfig returns the configuration object for using with the go-ipfs library.
// This is executed only when initializing a new repository.
func (m *ipfsMetadata) IPFSConfig() (*ipfs_config.Config, error) {
	identity, err := ipfs_config.CreateIdentity(io.Discard, []ipfs_options.KeyGenerateOption{
		ipfs_options.Key.Type(ipfs_options.Ed25519Key),
	})

	cfg, err := ipfs_config.InitWithIdentity(identity)
	if err != nil {
		return nil, err
	}

	if m.BootstrapNodes != "" {
		var peers []peer.AddrInfo
		peers, err = ipfs_config.ParseBootstrapPeers(
			strings.Split(m.BootstrapNodes, ","),
		)
		cfg.SetBootstrapPeers(peers)
	}

	r := strings.ToLower(m.Routing)
	switch r {
	case "dht", "dhtclient", "dhtserver", "none":
		cfg.Routing.Type = r
	case "":
		cfg.Routing.Type = "dht"
	default:
		return nil, fmt.Errorf("invalid value for metadata property 'routing'")
	}

	if m.StorageMax != "" {
		cfg.Datastore.StorageMax = m.StorageMax
	}
	if m.StorageGCWatermark != 0 {
		cfg.Datastore.StorageGCWatermark = m.StorageGCWatermark
	}
	if m.StorageGCPeriod != "" {
		cfg.Datastore.GCPeriod = m.StorageGCPeriod
	}

	return cfg, nil
}
