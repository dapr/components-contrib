// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

// Nacos is an easy-to-use dynamic service discovery, configuration and service management platform
//
// See https://github.com/nacos-group/nacos-sdk-go/

package nacos

type nacosMetadata struct {
	NameServer           string `json:"nameServer"`
	Endpoint             string `json:"endpoint"`
	RegionID             string `json:"region"`
	NamespaceID          string `json:"namespace"`
	AccessKey            string `json:"accessKey"`
	SecretKey            string `json:"secretKey"`
	TimeoutMs            int    `json:"timeout,string"`
	CacheDir             string `json:"cacheDir"`
	UpdateThreadNum      int    `json:"updateThreadNum,string"`
	NotLoadCacheAtStart  bool   `json:"notLoadCacheAtStart,string"`
	UpdateCacheWhenEmpty bool   `json:"updateCacheWhenEmpty,string"`
	Username             string `json:"username"`
	Password             string `json:"password"`
	LogDir               string `json:"logDir"`
	RotateTime           string `json:"rotateTime"`
	MaxAge               int    `json:"maxAge,string"`
	LogLevel             string `json:"logLevel"`
	Config               string `json:"config"`
	Watches              string `json:"watches"`
}
