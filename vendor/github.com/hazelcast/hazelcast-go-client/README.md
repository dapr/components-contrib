# Table of Contents

* [Introduction](#introduction)
* [1. Getting Started](#1-getting-started)
  * [1.1. Requirements](#11-requirements)
  * [1.2. Working with Hazelcast IMDG Clusters](#12-working-with-hazelcast-imdg-clusters)
     * [1.2.1. Setting Up a Hazelcast IMDG Cluster](#121-setting-up-a-hazelcast-imdg-cluster)
       * [1.2.1.1. Running Standalone JARs](#1211-running-standalone-jars)
       * [1.2.1.2. Adding User Library to CLASSPATH](#1212-adding-user-library-to-classpath)
  * [1.3. Downloading and Installing](#13-downloading-and-installing)
  * [1.4. Basic Configuration](#14-basic-configuration)
    * [1.4.1. Configuring Hazelcast IMDG](#141-configuring-hazelcast-imdg)
    * [1.4.2. Configuring Hazelcast Go Client](#142-configuring-hazelcast-go-client)
      * [1.4.2.1. Group Settings](#1421-group-settings)
      * [1.4.2.2. Network Settings](#1422-network-settings)
    * [1.4.3. Client System Properties](#143-client-system-properties)
  * [1.5. Basic Usage](#15-basic-usage)
  * [1.6. Code Samples](#16-code-samples)
* [2. Features](#2-features)
* [3. Configuration Overview](#3-configuration-overview)
* [4. Serialization](#4-serialization)
  * [4.1. IdentifiedDataSerializable Serialization](#41-identifieddataserializable-serialization)
  * [4.2. Portable Serialization](#42-portable-serialization)
  * [4.3. Custom Serialization](#43-custom-serialization)
  * [4.4. JSON Serialization](#44-json-serialization)
  * [4.5. Global Serialization](#45-global-serialization)
* [5. Setting Up Client Network](#5-setting-up-client-network)
  * [5.1. Providing the Member Addresses](#51-providing-the-member-addresses)
  * [5.2. Setting Smart Routing](#52-setting-smart-routing)
  * [5.3. Enabling Redo Operation](#53-enabling-redo-operation)
  * [5.4. Setting Connection Timeout](#54-setting-connection-timeout)
  * [5.5. Setting Connection Attempt Limit](#55-setting-connection-attempt-limit)
  * [5.6. Setting Connection Attempt Period](#56-setting-connection-attempt-period)
  * [5.7. Enabling Client TLS/SSL](#57-enabling-client-tlsssl)
  * [5.8. Enabling Hazelcast Cloud Discovery](#58-enabling-hazelcast-cloud-discovery)
* [6. Securing Client Connection](#6-securing-client-connection)
  * [6.1. TLS/SSL](#61-tlsssl)
    * [6.1.1. TLS/SSL for Hazelcast Members](#611-tlsssl-for-hazelcast-members)
    * [6.1.2. TLS/SSL for Hazelcast Go Clients](#612-tlsssl-for-hazelcast-go-clients)
    * [6.1.3. Mutual Authentication](#613-mutual-authentication)
* [7. Using Go Client with Hazelcast IMDG](#7-using-go-client-with-hazelcast-imdg)
  * [7.1. Go Client API Overview](#71-go-client-api-overview)
  * [7.2. Go Client Operation Modes](#72-go-client-operation-modes)
      * [7.2.1. Smart Client](#721-smart-client)
      * [7.2.2. Unisocket Client](#722-unisocket-client)
  * [7.3. Handling Failures](#73-handling-failures)
    * [7.3.1. Handling Client Connection Failure](#731-handling-client-connection-failure)
    * [7.3.2. Handling Retry-able Operation Failure](#732-handling-retry-able-operation-failure)
  * [7.4. Using Distributed Data Structures](#74-using-distributed-data-structures)
    * [7.4.1. Using Map](#741-using-map)
    * [7.4.2. Using MultiMap](#742-using-multimap)
    * [7.4.3. Using Replicated Map](#743-using-replicated-map)
    * [7.4.4. Using Queue](#744-using-queue)
    * [7.4.5. Using Set](#745-using-set)
    * [7.4.6. Using List](#746-using-list)
    * [7.4.7. Using Ringbuffer](#747-using-ringbuffer)
    * [7.4.8. Using Reliable Topic](#748-using-reliable-topic)
    * [7.4.9. Using PN Counter](#749-using-pn-counter)
    * [7.4.10. Using Flake ID Generator](#7410-using-flake-id-generator)
  * [7.5. Distributed Events](#75-distributed-events)
    * [7.5.1. Cluster Events](#751-cluster-events)
      * [7.5.1.1. Listening for Member Events](#7511-listening-for-member-events)
      * [7.5.1.2. Listening for Lifecycle Events](#7512-listening-for-lifecycle-events)
    * [7.5.2. Distributed Data Structure Events](#752-distributed-data-structure-events)
      * [7.5.2.1. Map Listener](#7521-map-listener)
      * [7.5.2.2. Entry Listener](#7522-entry-listener)
      * [7.5.2.3. Item Listener](#7523-item-listener)           
      * [7.5.2.4. Message Listener](#7524-message-listener) 
  * [7.6. Distributed Computing](#76-distributed-computing)
    * [7.6.1. Using EntryProcessor](#761-using-entryprocessor)
        * [Processing Entries](#processing-entries)
  * [7.7. Distributed Query](#77-distributed-query)
    * [7.7.1. How Distributed Query Works](#771-how-distributed-query-works)
      * [7.7.1.1. Employee Map Query Example](#7711-employee-map-query-example)
      * [7.7.1.2. Querying by Combining Predicates with AND, OR, NOT](#7712-querying-by-combining-predicates-with-and-or-not)
      * [7.7.1.3. Querying with SQL](#7713-querying-with-sql)
        * [Supported SQL Syntax](#supported-sql-syntax)
        * [Querying Examples with Predicates](#querying-examples-with-predicates)
      * [7.7.1.4. Querying with JSON Strings](#7714-querying-with-json-strings)  
    * [7.7.2. Fast-Aggregations](#772-fast-aggregations)
  * [7.8. Monitoring and Logging](#78-monitoring-and-logging)  
    * [7.8.1. Enabling Client Statistics](#781-enabling-client-statistics)
    * [7.8.2. Logging Configuration](#782-logging-configuration)
* [8. Development and Testing](#8-development-and-testing)
  * [8.1. Building and Using Client From Sources](#81-building-and-using-client-from-sources)
  * [8.2. Testing](#82-testing)
* [9. Getting Help](#9-getting-help)
* [10. Contributing](#10-contributing)
* [11. License](#11-license)
* [12. Copyright](#12-copyright)


# Introduction

[![GoDoc](https://godoc.org/github.com/hazelcast/hazelcast-go-client?status.svg)](https://godoc.org/github.com/hazelcast/hazelcast-go-client)
[![Go Report Card](https://goreportcard.com/badge/github.com/hazelcast/hazelcast-go-client)](https://goreportcard.com/report/github.com/hazelcast/hazelcast-go-client)
<br />

This document explains Go client for Hazelcast which uses Hazelcast's Open Client Protocol 1.6. This client works with Hazelcast 3.6 and higher.

**Hazelcast** is a clustering and highly scalable data distribution platform. With its various distributed data structures, distributed caching capabilities, elastic nature and more importantly with so many happy users, Hazelcast is a feature-rich, enterprise-ready and developer-friendly in-memory data grid solution.

# 1. Getting Started

This chapter provides information on how to get started with your Hazelcast Go client. It outlines the requirements, 
installation and configuration of the client, setting up a cluster, and provides a simple application that uses a distributed map in Go client.

## 1.1. Requirements

- Windows, Linux or MacOS
- Go 1.9 or newer
- Java 6 or newer
- Hazelcast IMDG 3.6 or newer
- Latest Hazelcast Go client

## 1.2. Working with Hazelcast IMDG Clusters

Hazelcast Go client requires a working Hazelcast IMDG cluster to run. This cluster handles storage and manipulation of the user data.
Clients are a way to connect to the Hazelcast IMDG cluster and access such data.

Hazelcast IMDG cluster consists of one or more cluster members. These members generally run on multiple virtual or physical machines
and are connected to each other via network. Any data put on the cluster is partitioned to multiple members transparent to the user.
It is therefore very easy to scale the system by adding new members as the data grows. Hazelcast IMDG cluster also offers resilience. Should
any hardware or software problem causes a crash to any member, the data on that member is recovered from backups and the cluster
continues to operate without any downtime. Hazelcast clients are an easy way to connect to a Hazelcast IMDG cluster and perform tasks on
distributed data structures that live on the cluster.

In order to use Hazelcast Go client, we first need to setup a Hazelcast IMDG cluster.

### 1.2.1. Setting Up a Hazelcast IMDG Cluster

There are following options to start a Hazelcast IMDG cluster easily:

* You can run standalone members by downloading and running JAR files from the website.
* You can embed members to your Java projects. 

We are going to download JARs from the website and run a standalone member for this guide.


#### 1.2.1.1. Running Standalone JARs

Follow the instructions below to create a Hazelcast IMDG cluster:

1. Go to Hazelcast's download [page](https://hazelcast.org/download/) and download either the `.zip` or `.tar` distribution of Hazelcast IMDG.
2. Decompress the contents into any directory that you
want to run members from.
3. Change into the directory that you decompressed the Hazelcast content and then into the `bin` directory.
4. Use either `start.sh` or `start.bat` depending on your operating system. Once you run the start script, you should see the Hazelcast IMDG logs in the terminal.

 You should see a log similar to the following, which means that your 1-member cluster is ready to be used:
```
INFO: [192.168.0.3]:5701 [dev] [3.10.4]

Members {size:1, ver:1} [
	Member [192.168.0.3]:5701 - 65dac4d1-2559-44bb-ba2e-ca41c56eedd6 this
]

Sep 06, 2018 10:50:23 AM com.hazelcast.core.LifecycleService
INFO: [192.168.0.3]:5701 [dev] [3.10.4] [192.168.0.3]:5701 is STARTED
```

#### 1.2.1.2. Adding User Library to CLASSPATH

When you want to use features such as querying and language interoperability, you might need to add your own Java classes
to the Hazelcast member in order to use them from your Go client. This can be done by adding your own compiled code to the 
`CLASSPATH`. To do this, compile your code with the `CLASSPATH` and add the compiled files to the `user-lib` directory in the extracted `hazelcast-<version>.zip` (or `tar`). 
Then, you can start your Hazelcast member by using the start scripts in the `bin` directory. The start scripts will automatically add your compiled classes to the `CLASSPATH`.

Note that if you are adding an `IdentifiedDataSerializable` or a `Portable` class, you need to add its factory too. Then, you should configure the factory in the `hazelcast.xml` configuration file. This file resides in the `bin` directory where you extracted the `hazelcast-<version>.zip` (or `tar`).

The following is an example configuration when you are adding an `IdentifiedDataSerializable` class:

 ```xml
<hazelcast>
     ...
     <serialization>
        <data-serializable-factories>
            <data-serializable-factory factory-id=<identified-factory-id>>
                IdentifiedFactoryClassName
            </data-serializable-factory>
        </data-serializable-factories>
    </serialization>
    ...
</hazelcast>
```
If you want to add a `Portable` class, you should use `<portable-factories>` instead of `<data-serializable-factories>` in the above configuration.

See the [Hazelcast IMDG Reference Manual](http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#getting-started) for more information on setting up the clusters.

## 1.3. Downloading and Installing

Following command installs Hazelcast Go client:

```
go get github.com/hazelcast/hazelcast-go-client
```

See the Go client's [tutorial](https://github.com/hazelcast/hazelcast-go-client/tree/master/sample/helloworld) for more information on installing and setting up the client.

## 1.4. Basic Configuration

If you are using Hazelcast IMDG and Go client on the same computer, generally the default configuration should be fine. This is great for
trying out the client. However, if you run the client on a different computer than any of the cluster members, you may
need to do some simple configuration such as specifying the member addresses.

The Hazelcast IMDG members and clients have their own configuration options. You may need to reflect some of the member side configurations on the client side to properly connect to the cluster.
This section describes the most common configuration elements to get you started in no time.
It discusses some member side configuration options to ease the understanding of Hazelcast's ecosystem. Then, the client side configuration options
regarding the cluster connection are discussed. The configurations for the Hazelcast IMDG data structures that can be used in the Node.js client are discussed in the following sections.

See the [Hazelcast IMDG Reference Manual](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html) and [Configuration Overview section](#configuration-overview) for more information.

### 1.4.1. Configuring Hazelcast IMDG

Hazelcast IMDG aims to run out of the box for most common scenarios. However if you have limitations on your network such as multicast being disabled,
you may have to configure your Hazelcast IMDG members so that they can find each other on the network. Also, since most of the distributed data structures are configurable, you may want to configure them according to your needs. We will show you the basics about network configuration here.

You can use the following options to configure Hazelcast IMDG:

* Using the `hazelcast.xml` configuration file.
* Programmatically configuring the member before starting it from the Java code.

Since we use standalone servers, we will use the `hazelcast.xml` file to configure our cluster members.

When you download and unzip `hazelcast-<version>.zip` (or `tar`), you see the `hazelcast.xml` in the `bin` directory. When a Hazelcast member starts, it looks for the `hazelcast.xml` file to load the configuration from. A sample `hazelcast.xml` is shown below.

```xml
<hazelcast>
    <group>
        <name>dev</name>
        <password>dev-pass</password>
    </group>
    <network>
        <port auto-increment="true" port-count="100">5701</port>
        <join>
            <multicast enabled="true">
                <multicast-group>224.2.2.3</multicast-group>
                <multicast-port>54327</multicast-port>
            </multicast>
            <tcp-ip enabled="false">
                <interface>127.0.0.1</interface>
                <member-list>
                    <member>127.0.0.1</member>
                </member-list>
            </tcp-ip>
        </join>
        <ssl enabled="false"/>
    </network>
    <partition-group enabled="false"/>
    <map name="default">
        <backup-count>1</backup-count>
    </map>
</hazelcast>
```
We will go over some important configuration elements in the rest of this section.

- `<group>`:  Specifies which cluster this member belongs to. A member connects only to the other members that are in the same group as
              itself. As shown in the above configuration sample, there are `<name>` and `<password>` tags under the `<group>` element with some pre-configured values. You may give your clusters different names so that they can
              live in the same network without disturbing each other. Note that the cluster name should be the same across all members and clients that belong
              to the same cluster. The `<password>` tag is not in use since Hazelcast 3.9. It is there for backward compatibility
              purposes. You can remove or leave it as it is if you use Hazelcast 3.9 or later.
- `<network>`
    - `<port>`: Specifies the port number to be used by the member when it starts. Its default value is 5701. You can specify another port number, and if
     you set `auto-increment` to `true`, then Hazelcast will try the subsequent ports until it finds an available port or the `port-count` is reached.
    - `<join>`: Specifies the strategies to be used by the member to find other cluster members. Choose which strategy you want to
    use by setting its `enabled` attribute to `true` and the others to `false`.
        - `<multicast>`: Members find each other by sending multicast requests to the specified address and port. It is very useful if IP addresses
        of the members are not static.
        - `<tcp>`: This strategy uses a pre-configured list of known members to find an already existing cluster. It is enough for a member to
        find only one cluster member to connect to the cluster. The rest of the member list is automatically retrieved from that member. We recommend
        putting multiple known member addresses there to avoid disconnectivity should one of the members in the list is unavailable at the time
        of connection.

These configuration elements are enough for most connection scenarios. Now we will move onto the configuration of the Go client.

### 1.4.2. Configuring Hazelcast Go Client

This section describes some network configuration settings to cover common use cases in connecting the client to a cluster. Refer to [Configuration Overview](#configuration-overview)
and the following sections for information about detailed network configuration and/or additional features of Hazelcast Go client configuration.

An easy way to configure your Hazelcast Go client is to create a `Config` object and set the appropriate options. Then you can
supply this object to your client at the startup.

**Configuration**

You need to create a `Config` object and adjust its properties. Then you can pass this object to the client when starting it.

```go
package main

import "github.com/hazelcast/hazelcast-go-client"

func main() {

	config := hazelcast.NewConfig()
	client , _ := hazelcast.NewClientWithConfig(config)
}
```
---

If you run the Hazelcast IMDG members in a different server than the client, you most probably have configured the members' ports and cluster
names as explained in the previous section. If you did, then you need to make certain changes to the network settings of your client.

#### 1.4.2.1. Group Settings

You need to provide the group name of the cluster, if it is defined on the server side, to which you want the client to connect.

```go
config := hazelcast.NewConfig()
config.GroupConfig().SetName("GROUP_NAME_OF_YOUR_CLUSTER")
```
> **NOTE: If you have a Hazelcast IMDG release older than 3.11, you need to provide also a group password along with the group name.**

#### 1.4.2.2. Network Settings

You need to provide the IP address and port of at least one member in your cluster so the client can find it.

```go
config := hazelcast.NewConfig()
config.NetworkConfig().AddAddress("some-ip-address:port")
hazelcast.NewClientWithConfig(config)
```

### 1.4.3. Client System Properties

While configuring your Go client, you can use various system properties provided by Hazelcast to tune its clients. These properties can be set programmatically through `config.SetProperty` or by using an environment variable.
The value of this property will be:

* the programmatically configured value, if programmatically set,
* the environment variable value, if the environment variable is set,
* the default value, if none of the above is set.

See the following for an example client system property:

```go
InvocationTimeoutSeconds = NewHazelcastPropertyInt64WithTimeUnit("hazelcast.client.invocation.timeout.seconds",
    120, time.Second)
```


The above property specifies the timeout duration to give up the invocations when a member in the member list is not reachable, and its default value is 120 seconds. You can change this value programmatically or using an environment variable, as shown below.

**Programmatically:**

```go
config.SetProperty(property.InvocationTimeoutSeconds.Name(), "2") // Sets invocation timeout as 2 seconds
```

or 

```go
config.SetProperty("hazelcast.client.invocation.timeout.seconds", "2") // Sets invocation timeout as 2 seconds
```

**By using an environment variable:** 

```go
os.Setenv(property.InvocationTimeoutSeconds.Name(), "2")
```


If you set a property both programmatically and via an environment variable, the programmatically
set value will be used.

See the [complete list](https://github.com/hazelcast/hazelcast-go-client/blob/master/config/property/client_properties.go) of client system properties, along with their descriptions, which can be used to configure your Hazelcast Go client.




## 1.5. Basic Usage

Now that we have a working cluster and we know how to configure both our cluster and client, we can run a simple program to use a distributed map in the Go client.

The following example first creates a programmatic configuration object. Then, it starts a client.

```go

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client"
)

func main() {

	config := hazelcast.NewConfig()  // We create a config for illustrative purposes.
                                    // We do not adjust this config. Therefore it has default settings.

	client, err := hazelcast.NewClientWithConfig(config)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(client.Name()) // Connects and prints the name of the client

}

```
This should print logs about the cluster members and information about the client itself such as the client type, UUID and address.

```
2018/10/24 16:16:16 New State :  STARTING
2018/10/24 16:16:16 

Members {size:2} [
	Member localhost:5701 - 923f0f91-9bc8-432f-9650-fd4a5436e80b
	Member localhost:5702 - c01a31c1-e90d-4a63-a9b1-f323606431ec
]

2018/10/24 16:16:16 Registered membership listener with ID  400022bd-dcbe-4cf5-b2c1-9e41cf6e16d9
2018/10/24 16:16:16 New State :  CONNECTED
2018/10/24 16:16:16 New State :  STARTED


```
Congratulations! You just started a Hazelcast Go client.

**Using a Map**

Let's manipulate a distributed map on a cluster using the client.

**IT.go**

```go
import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
)

func main() {
	config := hazelcast.NewConfig()
	client, err := hazelcast.NewClientWithConfig(config)
	if err != nil {
		fmt.Println(err)
		return
	}
	personnelMap, _ := client.GetMap("personnelMap")
	personnelMap.Put("Alice", "IT")
	personnelMap.Put("Bob", "IT")
	personnelMap.Put("Clark", "IT")
	fmt.Println("Added IT personnel. Logging all known personnel")
	resultPairs, _ := personnelMap.EntrySet()
	for _, pair := range resultPairs {
		fmt.Println(pair.Key(), " is in ", pair.Value(), " department")
	}
}

```
**Output**

```
2018/10/24 16:23:26 New State :  STARTING
2018/10/24 16:23:26 

Members {size:2} [
	Member localhost:5701 - 923f0f91-9bc8-432f-9650-fd4a5436e80b
	Member localhost:5702 - c01a31c1-e90d-4a63-a9b1-f323606431ec
]

2018/10/24 16:23:26 Registered membership listener with ID  199b9d1a-9085-4f1e-b6da-3a15a7757637
2018/10/24 16:23:26 New State :  CONNECTED
2018/10/24 16:23:26 New State :  STARTED
Added IT personnel. Logging all known personnel
Alice  is in  IT  department
Clark  is in  IT  department
Bob  is in  IT  department

```

You see this example puts all IT personnel into a cluster-wide `personnelMap` and then prints all known personnel.

**Sales.go**

```go
import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
)

func main() {
	config := hazelcast.NewConfig()
	client, err := hazelcast.NewClientWithConfig(config)
	if err != nil {
		fmt.Println(err)
		return
	}
	personnelMap, _ := client.GetMap("personnelMap")
	personnelMap.Put("Denise", "Sales")
	personnelMap.Put("Erwin", "Sales")
	personnelMap.Put("Faith", "Sales")
	fmt.Println("Added Sales personnel. Logging all known personnel")
	resultPairs, _ := personnelMap.EntrySet()
	for _, pair := range resultPairs {
		fmt.Println(pair.Key(), " is in ", pair.Value(), " department")
	}
}
```

**Output**

```
2018/10/24 16:25:58 New State :  STARTING
2018/10/24 16:25:58 

Members {size:2} [
	Member localhost:5701 - 923f0f91-9bc8-432f-9650-fd4a5436e80b
	Member localhost:5702 - c01a31c1-e90d-4a63-a9b1-f323606431ec
]

2018/10/24 16:25:58 Registered membership listener with ID  7014c382-182e-4962-94ff-d6094917d864
2018/10/24 16:25:58 New State :  CONNECTED
2018/10/24 16:25:58 New State :  STARTED
Added Sales personnel. Logging all known personnel
Erwin  is in  Sales  department
Alice  is in  IT  department
Clark  is in  IT  department
Bob  is in  IT  department
Denise  is in  Sales  department
Faith  is in  Sales  department

```

You will see this time we add only the sales employees but we get the list all known employees including the ones in IT.
That is because our map lives in the cluster and no matter which client we use, we can access the whole map.

## 1.6. Code Samples

See the Hazelcast Go [code samples](https://github.com/hazelcast/hazelcast-go-client/blob/master/sample) for more examples.

You can also see the Hazelcast Go [API Documentation](https://godoc.org/github.com/hazelcast/hazelcast-go-client).

# 2. Features

Hazelcast Go client supports the following data structures and features:

* Map 
* Multi Map
* List
* Set
* Queue
* Topic
* Reliable Topic
* Replicated Map
* Ringbuffer
* Query (Predicates)
* Built-in Predicates
* API configuration
* Event Listeners
* Entry Processor
* Flake Id Generator
* CRDT PN Counter
* Aggregations 
* Projections
* Lifecycle Service
* Smart Client
* Unisocket Client
* IdentifiedDataSerializable Serialization
* Portable Serialization
* Custom Serialization
* Global Serialization
* JSON Serialization
* SSL Support (requires Enterprise server)
* Mutual Authentication (requires Enterprise server)
* Custom Credentials
* Hazelcast Cloud Discovery
* Statistics

# 3. Configuration Overview

You can configure Hazelcast Go client programmatically (API).

For programmatic configuration of the Hazelcast Go client, just instantiate a `Config` object and configure the
desired aspects. An example is shown below.

```go
config := hazelcast.NewConfig()
config.NetworkConfig().AddAddress("some-ip-address:port")
hazelcast.NewClientWithConfig(config)
```

See the `Config` class documentation at [Hazelcast Go client API Docs](https://godoc.org/github.com/hazelcast/hazelcast-go-client/config#Config) for details.

# 4. Serialization

Serialization is the process of converting an object into a stream of bytes to store the object in the memory, a file or database, or transmit it through the network. Its main purpose is to save the state of an object in order to be able to recreate it when needed. The reverse process is called deserialization. Hazelcast offers you its own native serialization methods. You will see these methods throughout this chapter. For primitive types, it uses Hazelcast native serialization. For other complex types (e.g. Go objects), it uses Gob serialization.

> **NOTE: `int` and `[]int` types in Go Language are serialized as `int64` and `[]int64` respectively by Hazelcast Serialization.**
 
Note that if the object is not one of the above-mentioned types, the Go client uses `Gob serialization` by default.

However, `Gob Serialization` is not the best way of serialization in terms of performance and interoperability between the clients in different languages. If you want the serialization to work faster or you use the clients in different languages, Hazelcast offers its own native serialization types, such as [IdentifiedDataSerializable Serialization](#41-identifieddataserializable-serialization) and [Portable Serialization](#42-portable-serialization).

On top of all, if you want to use your own serialization type, you can use a [Custom Serialization](#43-custom-serialization).

## 4.1. IdentifiedDataSerializable Serialization

For a faster serialization of objects, Hazelcast recommends to implement IdentifiedDataSerializable interface.

The following is an example of an object implementing this interface:

```go
const (
	employeeClassID                 = 100
	sampleDataSerializableFactoryID = 1000
)

type Employee struct {
	id   int32
	name string
}

func (e *Employee) ClassID() int32 {
	return employeeClassID
}

func (e *Employee) FactoryID() int32 {
	return sampleDataSerializableFactoryID
}

func (e *Employee) ReadData(input serialization.DataInput) error {
	e.id = input.ReadInt32()
	e.name = input.ReadUTF()
	return input.Error()
}

func (e *Employee) WriteData(output serialization.DataOutput) (err error) {
	output.WriteInt32(e.id)
	output.WriteUTF(e.name)
	return
}
```

The `IdentifiedDataSerializable` interface uses `ClassID` and `FactoryID` to reconstitute the object. To complete the implementation `IdentifiedDataSerializableFactory` should also be implemented and registered into `SerializationConfig` which can be accessed from `config.SerializationConfig()`. The factory's responsibility is to return an instance of the right `IdentifiedDataSerializable` object, given the classID.

A sample `IdentifiedDataSerializableFactory` could be implemented as follows:

```go

type SampleDataSerializableFactory struct {
}

func (*SampleDataSerializableFactory) Create(classID int32) serialization.IdentifiedDataSerializable {
	if classID == employeeClassID {
		return &Employee{}
	}
	return nil
}
```

The last step is to register the `IdentifiedDataSerializableFactory` to the `SerializationConfig`.

```go
config := hazelcast.NewConfig()
config.SerializationConfig().AddDataSerializableFactory(sampleDataSerializableFactoryID, SampleDataSerializableFactory{})
```

Note that the ID that is passed to the `SerializationConfig` is same as the `FactoryID ` that the `address` object returns.

## 4.2. Portable Serialization

As an alternative to the existing serialization methods, Hazelcast offers portable serialization. To use it, you need to implement the `Portable` interface. Portable serialization has the following advantages:

- Supporting multiversion of the same object type.
- Fetching individual fields without having to rely on the reflection.
- Querying and indexing support without deserialization and/or reflection.

In order to support these features, a serialized `Portable` object contains meta information like the version and the concrete location of the each field in the binary data. This way Hazelcast is able to navigate in the binary data and deserialize only the required field without actually deserializing the whole object which improves the query performance.

With multiversion support, you can have two members where each of them having different versions of the same object, and Hazelcast will store both meta information and use the correct one to serialize and deserialize portable objects depending on the member. This is very helpful when you are doing a rolling upgrade without shutting down the cluster.

Also note that portable serialization is totally language independent and is used as the binary protocol between Hazelcast server and clients.

A sample portable implementation of a `Foo` class looks like the following:

```go
const (
	customerClassID         = 1
	samplePortableFactoryID = 1
)

type Customer struct {
	name      string
	id        int32
	lastOrder time.Time
}

func (c *Customer) FactoryID() int32 {
	return samplePortableFactoryID
}

func (c *Customer) ClassID() int32 {
	return customerClassID
}

func (c *Customer) WritePortable(writer serialization.PortableWriter) (err error) {
	writer.WriteInt32("id", c.id)
	writer.WriteUTF("name", c.name)
	writer.WriteInt64("lastOrder", c.lastOrder.UnixNano()/int64(time.Millisecond))
	return
}

func (c *Customer) ReadPortable(reader serialization.PortableReader) (err error) {
	c.id = reader.ReadInt32("id")
	c.name = reader.ReadUTF("name")
	t := reader.ReadInt64("lastOrder")
	c.lastOrder = time.Unix(0, t*int64(time.Millisecond))
	return reader.Error()
}

```

Similar to `IdentifiedDataSerializable`, a `Portable` object must provide `ClassID ` and `FactoryID `. The factory object will be used to create the `Portable` object given the `classId`.

A sample `PortableFactory` could be implemented as follows:

```go
type SamplePortableFactory struct {
}

func (pf *SamplePortableFactory) Create(classID int32) serialization.Portable {
	if classID == customerClassID {
		return &Customer{}
	}
	return nil
}
```

The last step is to register the `PortableFactory` to the `SerializationConfig`.

```go
config := hazelcast.NewConfig()
config.SerializationConfig().AddPortableFactory(samplePortableFactoryID, &SamplePortableFactory{})
```

Note that the ID that is passed to the `SerializationConfig` is same as the `FactoryID` that `Foo` object returns.

## 4.3. Custom Serialization

Hazelcast lets you plug a custom serializer to be used for serialization of objects.

Let's say you have an object `CustomSerializable` and you would like to customize the serialization, since you may want to use an external serializer for only one object.

```go
type CustomSerializable struct {
	value string
}
```

Let's say your custom `CustomSerializer` will serialize `CustomSerializable`.

```go
type CustomSerializer struct {
}

func (s *CustomSerializer) ID() int32 {
	return 10
}

func (s *CustomSerializer) Read(input serialization.DataInput) (obj interface{}, err error) {
	array = input.ReadByteArray()
	return &CustomSerializable{string(array)}, input.Error()
}

func (s *CustomSerializer) Write(output serialization.DataOutput, obj interface{}) (err error) {
	array := []byte(obj.(CustomSerializable).value)
	output.WriteByteArray(array)
	return
}
```

Note that the serializer `id` must be unique as Hazelcast will use it to lookup the `CustomSerializer` while it deserializes the object. Now the last required step is to register the `MusicianSerializer` to the configuration.

```go
musicianSerializer := &MusicianSerializer{}
config.SerializationConfig().AddCustomSerializer(reflect.TypeOf((*CustomSerializable)(nil)), &CustomSerializer{})
```

From now on, Hazelcast will use `CustomSerializer` to serialize `CustomSerializable` objects.

## 4.4. JSON Serialization

You can use the JSON formatted strings as objects in Hazelcast cluster. Starting with Hazelcast IMDG 3.12, the JSON serialization is one of the formerly supported serialization methods. Creating JSON objects in the cluster does not require any server side coding and hence you can just send a JSON formatted string object to the cluster and query these objects by fields.

In order to use JSON serialization, you should use the `HazelcastJSONValue` object for the key or value.

You can construct a `HazelcastJSONValue` from string or from your go object:

```go
//from string
core.CreateHazelcastJSONValueFromString{"your json string"}

//from go object
core.CreateHazelcastJSONValueFromString{yourObject}
```

No JSON parsing is performed but it is your responsibility to provide correctly formatted JSON strings. The client will not validate the string, and it will send it to the cluster as it is. If you submit incorrectly formatted JSON strings and, later, if you query those objects, it is highly possible that you will get formatting errors since the server will fail to deserialize or find the query fields.

Here is an example of how you can construct a `HazelcastJSONValue` and put to the map:

```go
jsonValue1 , _ := core.CreateHazelcastJSONValueFromString("{ \"age\": 4 }")
mp.Put("item1", jsonValue1)
jsonValue2 , _ := core.CreateHazelcastJSONValueFromString("{ \"age\": 4 }")
mp.Put("item2", jsonValue2)
```

You can query JSON objects in the cluster using the `Predicate`s of your choice. An example JSON query for querying the values whose age is greater than 6 is shown below:

```go
// Get the objects whose age is greater than 6
result, _ := mp.ValuesWithPredicate(predicate.GreaterThan("age", 6))
var person interface{}
result[0].(*core.HazelcastJSONValue).Unmarshal(&person)
log.Println("Retrieved: ", len(result))
log.Println("Entry is: ", person)
```

Note that we have used `var person interface{}`. If we already knew the type of our object we could do the following:

```go
type person struct {
    Age  int
    Name string
}


person1 , _ := core.CreateHazelcastJSONValue(person{Age: 20, Name: "Walter"})
person2 , _ := core.CreateHazelcastJSONValue(person{Age: 5, Name: "Mike"})
mp.Put("item1", person1)
mp.Put("item2", person2)
result, _ := mp.ValuesWithPredicate(predicate.GreaterThan("Age", 6))
var person person
value := result[0].(*core.HazelcastJSONValue)
log.Println(value.ToString()) //{"Age":20,"Name":"Walter"}
value.Unmarshal(&person)
log.Println("Retrieved: ", len(result)) // Retrieved: 1
log.Println("Entry is: ", person) // Entry is: {20 Walter}
```

Note that here we also show an example of how to create the JSON value from a go object.



## 4.5. Global Serialization

The global serializer is identical to custom serializers from the implementation perspective. The global serializer is registered as a fallback serializer to handle all other objects if a serializer cannot be located for them.

By default, Gob serialization is used if the object is not `IdentifiedDataSerializable` or `Portable` or there is no custom serializer for it. When you configure a global serializer, it is used instead of Gob serialization.

**Use cases:**

* Third party serialization frameworks can be integrated using the global serializer.

* For your custom objects, you can implement a single serializer to handle all of them.

A sample global serializer that integrates with a third party serializer is shown below.

```go
type GlobalSerializer struct {
}

func (*GlobalSerializer) ID() int32 {
	return 20
}

func (*GlobalSerializer) Read(input serialization.DataInput) (obj interface{}, err error) {
	// return MyFavoriteSerializer.deserialize(input)
	return
}

func (*GlobalSerializer) Write(output serialization.DataOutput, object interface{}) (err error) {
	// output.write(MyFavoriteSerializer.serialize(object))
	return
}
```

You should register the global serializer in the configuration.

```go
config.SerializationConfig().SetGlobalSerializer(&GlobalSerializer{})
```

# 5. Setting Up Client Network

All network related configuration of Hazelcast Go client is performed via the `NetworkConfig` class when using programmatic configuration. 
Here is an example of configuring network for Go client programmatically.

```go
config := hazelcast.NewConfig()
networkConfig := config.NetworkConfig()
networkConfig.AddAddress("10.1.1.21", "10.1.1.22:5703")
networkConfig.SetSmartRouting(true)
networkConfig.SetRedoOperation(true)
networkConfig.SetConnectionTimeout(6 * time.Second)
networkConfig.SetConnectionAttemptPeriod(5 * time.Second)
networkConfig.SetConnectionAttemptLimit(5)
```

## 5.1. Providing the Member Addresses

Address list is the initial list of cluster addresses which the client will connect to. The client uses this
list to find an alive member. Although it may be enough to give only one address of a member in the cluster
(since all members communicate with each other), it is recommended that you give the addresses for all the members.

```go
config := hazelcast.NewConfig()
networkConfig := config.NetworkConfig()
networkConfig.AddAddress("10.1.1.21", "10.1.1.22:5703")
```

If the port part is omitted, then 5701, 5702 and 5703 will be tried in a random order.

You can specify multiple addresses with or without the port information as seen above. The provided list is shuffled and tried in a random order. Its default value is `localhost`.

## 5.2. Setting Smart Routing

Smart routing defines whether the client mode is smart or unisocket. See the [Go client Operation Modes section](#go-client-operation-modes)
for the description of smart and unisocket modes.

The following are example configurations.

```go
config := hazelcast.NewConfig()
networkConfig := config.NetworkConfig()
networkConfig.SetSmartRouting(true)
```

Its default value is `true` (smart client mode).

## 5.3. Enabling Redo Operation

It enables/disables redo-able operations. While sending the requests to the related members, the operations can fail due to various reasons. Read-only operations are retried by default. If you want to enable retry for the other operations, you can set the `redoOperation` to `true`.

```go
config := hazelcast.NewConfig()
networkConfig := config.NetworkConfig()
networkConfig.SetRedoOperation(true)
```

Its default value is `false` (disabled).

## 5.4. Setting Connection Timeout

Connection timeout is the timeout value in milliseconds for the members to accept the client connection requests.
If the member does not respond within the timeout, the client will retry to connect as many as `NetworkConfig.connectionAttemptLimit` times.

The following are the example configurations.

```go
config := hazelcast.NewConfig()
networkConfig := config.NetworkConfig()
networkConfig.SetConnectionTimeout(6 * time.Second)
```

Its default value is `5000` milliseconds.

## 5.5. Setting Connection Attempt Limit

While the client is trying to connect initially to one of the members in the `NetworkConfig.addresses`, that member might not be available at that moment. Instead of giving up, throwing an error and stopping the client, the client will retry as many as `NetworkConfig.connectionAttemptLimit` times. This is also the case when the previously established connection between the client and that member goes down.

The following are example configurations.

```go
config := hazelcast.NewConfig()
networkConfig := config.NetworkConfig()
networkConfig.SetConnectionAttemptLimit(5)
```

Its default value is `2`.

## 5.6. Setting Connection Attempt Period

Connection attempt period is the duration in milliseconds between the connection attempts.

The following are example configurations.

```go
config := hazelcast.NewConfig()
networkConfig := config.NetworkConfig()
networkConfig.SetConnectionAttemptPeriod(5 * time.Second)
```

Its default value is `3000` milliseconds.

## 5.7. Enabling Client TLS/SSL

You can use TLS/SSL to secure the connection between the clients and members. If you want to enable TLS/SSL
for the client-cluster connection, you should set an SSL configuration. Please see [TLS/SSL section](#61-tlsssl).

As explained in the [TLS/SSL section](#61-tlsssl), Hazelcast members have key stores used to identify themselves (to other members) and Hazelcast Go clients have certificate authorities used to define which members they can trust. Hazelcast has the mutual authentication feature which allows the Go clients also to have their private keys and public certificates and members to have their certificate authorities so that the members can know which clients they can trust. See the [Mutual Authentication section](#13-mutual-authentication).

## 5.8. Enabling Hazelcast Cloud Discovery

The purpose of Hazelcast Cloud Discovery is to provide the clients to use IP addresses provided by `hazelcast orchestrator`. To enable Hazelcast Cloud Discovery, specify a token for the `discoveryToken` field and set the `enabled` field to `true`.

The following are example configurations.

```go
config.GroupConfig().SetName("hazel")
config.GroupConfig().SetPassword("cast")

cloudConfig := config.NetworkConfig().CloudConfig()
cloudConfig.SetDiscoveryToken("EXAMPLE_TOKEN")
cloudConfig.SetEnabled(true)
```

To be able to connect to the provided IP addresses, you should use secure TLS/SSL connection between the client and members. Therefore, you should set an SSL configuration as described in the previous section.

# 6. Securing Client Connection

This chapter describes the security features of Hazelcast Go client. These include using TLS/SSL for connections between members and between clients and members, and mutual authentication. These security features require **Hazelcast IMDG Enterprise** edition.

### 6.1. TLS/SSL

One of the offers of Hazelcast is the TLS/SSL protocol which you can use to establish an encrypted communication across your cluster with key stores and trust stores.

* A Java `keyStore` is a file that includes a private key and a public certificate. The equivalent of a key store is the combination of `key` and `cert` files at the Go client side.

* A Java `trustStore` is a file that includes a list of certificates trusted by your application which is named certificate authority. The equivalent of a trust store is a `ca` file at the Go client side.

You should set `keyStore` and `trustStore` before starting the members. See the next section how to set `keyStore` and `trustStore` on the server side.

#### 6.1.1. TLS/SSL for Hazelcast Members

Hazelcast allows you to encrypt socket level communication between Hazelcast members and between Hazelcast clients and members, for end to end encryption. To use it, see the [TLS/SSL for Hazelcast Members section](http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#tls-ssl-for-hazelcast-members) in the Hazelcast IMDG Reference Manual.

#### 6.1.2. TLS/SSL for Hazelcast Go clients

Hazelcast Go clients which support TLS/SSL should have the following user supplied SSLConfig

```go
config := hazelcast.NewConfig()
sslConfig := config.NetworkConfig().SSLConfig()
sslConfig.SetEnabled(true)
sslConfig.SetCaPath("yourCaPath")
sslConfig.ServerName="serverName"
```

#### 6.1.3. Mutual Authentication

As explained above, Hazelcast members have key stores used to identify themselves (to other members) and Hazelcast clients have trust stores used to define which members they can trust.

Using mutual authentication, the clients also have their key stores and members have their trust stores so that the members can know which clients they can trust to.

To enable mutual authentication, firstly, you need to set the following property on the server side in the `hazelcast.xml`:

```xml
<network>
    <ssl enabled="true">
        <properties>
            <property name="javax.net.ssl.mutualAuthentication">REQUIRED</property>
        </properties>
    </ssl>
</network>
```

You can see the details of setting mutual authentication on the server side in the [Mutual Authentication section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#mutual-authentication) of the Hazelcast IMDG Reference Manual.

Client side config needs to be set as follows:

```go
config := hazelcast.NewConfig()
sslConfig := config.NetworkConfig().SSLConfig()
sslConfig.SetEnabled(true)
sslConfig.SetCaPath("yourCaPath")
sslConfig.AddClientCertAndKeyPath("yourClientCertPath", "yourClientKeyPath")
sslConfig.ServerName = "yourServerName"
```

# 7. Using Go Client with Hazelcast IMDG

This chapter provides information on how you can use Hazelcast IMDG's data structures in the Go client, after giving some basic information including an overview to the client API, operation modes of the client and how it handles the failures.

## 7.1. Go Client API Overview

If you are ready to go, let's start to use Hazelcast Go client!

The first step is configuration. You can configure the Go client programmatically.

```go
config := hazelcast.NewConfig()
config.GroupConfig().SetName("dev")
config.GroupConfig().SetPassword("pass")
config.NetworkConfig().AddAddress("10.1.1.21", "10.1.1.22:5703")
```

The second step is initializing the `HazelcastClient` to be connected to the cluster.

```go
client, err := hazelcast.NewClientWithConfig(config)
```

**This client object is your gateway to access all Hazelcast distributed objects.**

Let’s create a map and populate it with some data, as shown below.

```go
client, err := hazelcast.NewClientWithConfig(config)
if err != nil {
	fmt.Println(err)
	return
}
personnelMap, _ := client.GetMap("personnelMap")
personnelMap.Put("Denise", "Sales")
personnelMap.Put("Erwin", "Sales")
personnelMap.Put("Faith", "Sales")
```

As the final step, if you are done with your client, you can shut it down as shown below. This will release all the used resources and will close connections to the cluster.

```go
client.Shutdown()
```

## 7.2. Go Client Operation Modes

The client has two operation modes because of the distributed nature of the data and cluster: smart and unisocket.

### 7.2.1. Smart Client

In the smart mode, clients connect to each cluster member. Since each data partition uses the well known and consistent hashing algorithm, each client can send an operation to the relevant cluster member, which increases the overall throughput and efficiency. Smart mode is the default mode.


### 7.2.2. Unisocket Client

For some cases, the clients can be required to connect to a single member instead of each member in the cluster. Firewalls, security or some custom networking issues can be the reason for these cases.

In the unisocket client mode, the client will only connect to one of the configured addresses. This single member will behave as a gateway to the other members. For any operation requested from the client, it will redirect the request to the relevant member and return the response back to the client returned from this member.

## 7.3. Handling Failures

There are two main failure cases you should be aware of. Below sections explain these and the configurations you can perform to achieve proper behavior.

### 7.3.1. Handling Client Connection Failure

While the client is trying to connect initially to one of the members in the `NetworkConfig.SetAddresses`, all the members might not be available. Instead of giving up, returning an error and stopping the client, the client will retry as many times as `connectionAttemptLimit`.

You can configure `connectionAttemptLimit` for the number of times you want the client to retry connecting. See the [Setting Connection Attempt Limit section](#55-setting-connection-attempt-limit).

The client executes each operation through the already established connection to the cluster. If this connection(s) disconnects or drops, the client will try to reconnect as configured.

### 7.3.2. Handling Retry-able Operation Failure

While sending the requests to the related members, the operations can fail due to various reasons. Read-only operations are retried by default. If you want to enable retrying for the other operations, you can set the `redoOperation` to `true`. See [Enabling Redo Operation section](#53-enabling-redo-operation).

You can set a timeout for retrying the operations sent to a member. This can be provided by using the property `hazelcast.client.invocation.timeout.seconds` in `config.SetProperty`. The client will retry an operation within this given period, of course, if it is a read-only operation or you enabled the `redoOperation` as stated in the above paragraph. This timeout value is important when there is a failure resulted by either of the following causes:

* Member throws an exception.

* Connection between the client and member is closed.

* Client’s heartbeat requests are timed out.

When a connection problem occurs, an operation is retried if it is certain that it has not run on the member yet or if it is idempotent such as a read-only operation, i.e., retrying does not have a side effect. If it is not certain whether the operation has run on the member, then the non-idempotent operations are not retried. However, as explained in the first paragraph of this section, you can force all client operations to be retried (`redoOperation`) when there is a connection failure between the client and member. But in this case, you should know that some operations may run multiple times causing conflicts. For example, assume that your client sent a `queue.offer` operation to the member and then the connection is lost. Since there will be no response for this operation, you will not know whether it has run on the member or not. If you enabled `redoOperation`, it means this operation may run again, which may cause two instances of the same object in the queue.

When invocation is being retried, the client may wait some time before it retries again. You can configure this duration for waiting using the following property:

```
config.setProperty(“hazelcast.client.invocation.retry.pause.millis”, “500");
```

The default retry wait time is 1 second.

## 7.4. Using Distributed Data Structures

Most of the distributed data structures are supported by the Go client. In this chapter, you will learn how to use these distributed data structures.

### 7.4.1. Using Map

Hazelcast Map (`IMap`) is a distributed map. Through the Go client, you can  perform operations like reading and writing from/to a Hazelcast Map with the well known get and put methods. For details, see the [Map section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#map) in the Hazelcast IMDG Reference Manual.

A Map usage example is shown below.

```go
// Get the Distributed Map from Cluster.
mp, _ := hz.GetMap("myDistributedMap")
//Standard Put and Get.
mp.Put("key", "value")
mp.Get("key")
//Concurrent Map methods, optimistic updating
mp.PutIfAbsent("somekey", "somevalue")
mp.ReplaceIfSame("key", "value", "newvalue")
```

### 7.4.2. Using MultiMap

Hazelcast `MultiMap` is a distributed and specialized map where you can store multiple values under a single key. For details, see the [MultiMap section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#multimap) in the Hazelcast IMDG Reference Manual.

A MultiMap usage example is shown below.

```go
// Get the Distributed MultiMap from Cluster.
multiMap, _ := hz.GetMultiMap("myDistributedMultimap")
// Put values in the map against the same key
multiMap.Put("my-key", "value1")
multiMap.Put("my-key", "value2")
multiMap.Put("my-key", "value3")
// Print out all the values for associated with key called "my-key"
values, _ := multiMap.Get("my-key")
fmt.Println(values)
// remove specific key/value pair
multiMap.Remove("my-key", "value2")
```

### 7.4.3. Using Replicated Map

Hazelcast `ReplicatedMap` is a distributed key-value data structure where the data is replicated to all members in the cluster. It provides full replication of entries to all members for high speed access. For details, see the [Replicated Map section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#replicated-map) in the Hazelcast IMDG Reference Manual.

A Replicated Map usage example is shown below.

```go
// Get a Replicated Map called "my-replicated-map"
mp, _ := hz.GetReplicatedMap("my-replicated-map")
// Put and Get a value from the Replicated Map
replacedValue, _ := mp.Put("key", "value")     // key/value replicated to all members
fmt.Println("replacedValue = ", replacedValue) // Will be null as its first update
value, _ := mp.Get("key")                      // the value is retrieved from a random member in the cluster
fmt.Println("value for key = ", value)
```

### 7.4.4. Using Queue

Hazelcast Queue(`IQueue`) is a distributed queue which enables all cluster members to interact with it. For details, see the [Queue section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#queue) in the Hazelcast IMDG Reference Manual.

A Queue usage example is shown below.

```go
// Get a Blocking Queue called "my-distributed-queue"
queue, _ := hz.GetQueue("my-distributed-queue")
// Offer a String into the Distributed Queue
queue.Offer("item")
// Poll the Distributed Queue and return the String
queue.Poll()
//Timed blocking Operations
queue.OfferWithTimeout("anotheritem", 500*time.Millisecond)
queue.PollWithTimeout(5 * time.Second)
//Indefinitely blocking Operations
queue.Put("yetanotheritem")
fmt.Println(queue.Take())
```

### 7.4.5. Using Set

Hazelcast Set(`ISet`) is a distributed set which does not allow duplicate elements. For details, see the [Set section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#set) in the Hazelcast IMDG Reference Manual.

A Set usage example is shown below.

```go
// Get the distributed set from cluster
set, _ := hz.GetSet("my-distributed-set")
// Add items to the set with duplicates
set.Add("item1")
set.Add("item1")
set.Add("item2")
set.Add("item2")
set.Add("item3")
set.Add("item3")
// Get the items. Note that no duplicates
items, _ := set.ToSlice()
fmt.Println(items)
```

### 7.4.6. Using List

Hazelcast List(`IList`) is distributed list which allows duplicate elements and preserves the order of elements. For details, see the [List section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#list) in the Hazelcast IMDG Reference Manual.

A List usage example is shown below.

```go
// Get the distributed list from cluster
list, _ := hz.GetList("my-distributed-list")
// Add elements to the list
list.Add("item1")
list.Add("item2")
// Remove the first element
removed, _ := list.RemoveAt(0)
fmt.Println("removed: ", removed)
// There is only one element left
size, _ := list.Size()
fmt.Println("current size is: ", size)
```

### 7.4.7. Using Ringbuffer

Hazelcast `Ringbuffer` is a replicated but not partitioned data structure that stores its data in a ring-like structure. You can think of it as a circular array with a given capacity. Each Ringbuffer has a tail and a head. The tail is where the items are added and the head is where the items are overwritten or expired. You can reach each element in a Ringbuffer using a sequence ID, which is mapped to the elements between the head and tail (inclusive) of the Ringbuffer. For details, see the [Ringbuffer section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#ringbuffer) in the Hazelcast IMDG Reference Manual.

A Ringbuffer usage example is shown below.


```go
rb, _ := hz.GetRingbuffer("rb")
// we start from the oldest item.
// if you want to start from the next item, call rb.tailSequence()+1
// add two items into ring buffer
rb.Add(100, core.OverflowPolicyOverwrite)
rb.Add(200, core.OverflowPolicyOverwrite)

// we start from the oldest item.
// if you want to start from the next item, call rb.tailSequence()+1
sequence, _ := rb.HeadSequence()
fmt.Println(rb.ReadOne(sequence))
sequence++
fmt.Println(rb.ReadOne(sequence))
```

### 7.4.8. Using Reliable Topic

Hazelcast `ReliableTopic` is a distributed topic implementation backed up by the `Ringbuffer` data structure. For details, see the [Reliable Topic section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#reliable-topic) in the Hazelcast IMDG Reference Manual.

A Reliable Topic usage example is shown below.

```go
reliableTopic, _ := client.GetReliableTopic("myReliableTopic")
reliableTopic.AddMessageListener(&reliableTopicMessageListener{})

for i := 0; i < 10; i++ {
    reliableTopic.Publish("Message " + strconv.Itoa(i))
}
```

### 7.4.9. Using PN Counter

Hazelcast `PNCounter` (Positive-Negative Counter) is a CRDT positive-negative counter implementation. It is an eventually consistent counter given there is no member failure. For details, see the [PN Counter section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#pn-counter) in the Hazelcast IMDG Reference Manual.

A PN Counter usage example is shown below.

```go
counter, _ := client.GpetPNCounter("myPNCounter")

currentValue, _ := counter.AddAndGet(5)
fmt.Printf("added 5 counter, current value is %d\n", currentValue)

currentValue, _ = counter.DecrementAndGet()
fmt.Printf("decremented counter, current value is %d\n", currentValue)
```

### 7.4.10. Using Flake ID Generator

Hazelcast `FlakeIdGenerator` is used to generate cluster-wide unique identifiers. Generated identifiers are long primitive values and are k-ordered (roughly ordered). IDs are in the range from 0 to `2^63-1 (maximum signed long value)`. For details, see the[FlakeIdGenerator section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#flakeidgenerator) in the Hazelcast IMDG Reference Manual.

A Flake ID Generator usage example is shown below.

```go
flakeIDGenerator, _ := client.GetFlakeIDGenerator("generator")
id, _ := flakeIDGenerator.NewID()
fmt.Printf("new id : %d", id)
```

## 7.5. Distributed Events

This chapter explains when various events are fired and describes how you can add event listeners on a Hazelcast Go client. These events can be categorized as cluster and distributed data structure events.

### 7.5.1. Cluster Events

You can add event listeners to a Hazelcast Go client. You can configure the following listeners to listen to the events on the client side.

* Membership Listener: Notifies when a member joins to/leaves the cluster, or when an attribute is changed in a member.

* Distributed Object Listener: Notifies when a distributed object is created or destroyed throughout the cluster.

* Lifecycle Listener: Notifies when the client is starting, started, shutting down, and shutdown.

#### 7.5.1.1. Listening for Member Events

You can add the following types of member events to the `ClusterService`.

* `memberAdded`: A new member is added to the cluster.
* `memberRemoved`: An existing member leaves the cluster.

The following is a membership listener registration by using `client.Cluster().AddMembershipListener(&membershipListener{})` function.

```go
type membershipListener struct {
}

func (l *membershipListener) MemberAdded(member core.Member) {
	fmt.Println("New member joined: ", member)
}

func (l *membershipListener) MemberRemoved(member core.Member) {
	fmt.Println("Member left: ", member)
}
```

#### 7.5.1.2. Listening for Lifecycle Events

The Lifecycle Listener notifies for the following events:

* `STARTING`: The client is starting.
* `STARTED`: The client has started.
* `SHUTTING_DOWN`: The client is shutting down.
* `SHUTDOWN`: The client’s shutdown has completed.
* `CONNECTED`: The client is connected to cluster
* `DISCONNECTED`: The client is disconnected from cluster note that this does not imply shutdown

The following is an example of Lifecycle Listener that is added to config and its output.

```go
type lifecycleListener struct {
}

func (l *lifecycleListener) LifecycleStateChanged(newState string) {
	fmt.Println("Lifecycle Event >>> ", newState)
}

config.AddLifecycleListener(&lifecycleListener{})
```

Or it can be added later after client has started
```go
registrationID := client.LifecycleService().AddLifecycleListener(&lifecycleListener{})

// Unregister it when you want to stop listening
client.LifecycleService().RemoveLifecycleListener(registrationID)
```


**Output:**

```
2018/10/26 16:16:51 New State :  STARTING
2018/10/26 16:16:51 

Lifecycle Event >>>  CONNECTED
Members {size:1} [
Lifecycle Event >>>  STARTED
	Member localhost:5701 - 936e0450-fc62-4927-9751-07c145f88a6f
]
Lifecycle Event >>>  SHUTTING_DOWN
Lifecycle Event >>>  SHUTDOWN

2018/10/26 16:16:51 Registered membership listener with ID  3e15ce02-4b14-4e4d-afca-bd69ea174498
2018/10/26 16:16:51 New State :  CONNECTED
2018/10/26 16:16:51 New State :  STARTED
2018/10/26 16:16:51 New State :  SHUTTING_DOWN
2018/10/26 16:16:51 New State :  SHUTDOWN
```

### 7.5.2. Distributed Data Structure Events

You can add event listeners to the distributed data structures.

#### 7.5.2.1. Map Listener

The Map Listener is used by the Hazelcast `Map`.

You can listen to map-wide or entry-based events. To listen to these events, you need to implement the relevant interfaces.

An entry-based  event is fired after the operations that affect a specific entry. For example, `Map.Put()`, `Map.Remove()` or `Map.Evict()`. An `EntryEvent` object is passed to the listener function. You can use the following listeners to listen to entry-based events.

- EntryExpiredListener
- EntryMergedListener
- EntryEvictedListener
- EntryUpdatedListener
- EntryRemovedListener
- EntryAddedListener

See the following example.

```go
type entryListener struct {
}

func (l *entryListener) EntryAdded(event core.EntryEvent) {
	fmt.Println("Entry Added: ", event.Key(), " ", event.Value()) // Entry Added: 1 Furkan
}
```
To add listener and fire an event:

```go
m, _ := client.GetMap("m")
m.AddEntryListener(&entryListener{}, true)
m.Put("1", "Furkan")
```


A map-wide event is fired as a result of a map-wide operation. For example, `Map.Clear()` or `Map.EvictAll()`. A `MapEvent` object is passed to the listener function. You can use the following listeners to listen to map-wide events.

* MapEvictedListener
* MapClearedListener

See the following example.

```go
type mapListener struct {
}

func (l *mapListener) MapCleared(event core.MapEvent) {
	fmt.Println("Map Cleared:", event.NumberOfAffectedEntries()) // Map Cleared: 3
}
```
To add listener and fire a related event:
```go
m, _ := client.GetMap("m")
m.AddEntryListener(&mapListener{}, true)
m.Put("1", "Mali")
m.Put("2", "Ahmet")
m.Put("3", "Furkan")

m.Clear()
```
As you see, there is a parameter in the `AddEntryListener` function: `includeValue`. It is a boolean parameter, and if it is `true`, the map event contains the entry value.

#### 7.5.2.2. Entry Listener

The Entry Listener is used by the Hazelcast `MultiMap` and `Replicated Map`.

You can listen to map-wide or entry-based events by implementing the corresponding interface such as `EntryAddedListener`.

An entry-based event is fired after the operations that affect a specific entry. For example, `MultiMap.Put()`, `MultiMap.Remove()`. You should implement the corresponding type to listen to these events such as `EntryAddedListener`. An `EntryEvent` object is passed to the listener function.

```go
type EntryListener struct {
}

func (l *EntryListener) EntryAdded(event core.EntryEvent) {
	log.Println("Entry Added:", event.Key(), event.Value()) // Entry Added: 1 Furkan
}

multiMap.AddEntryListener(&EntryListener{}, true)
multiMap.Put("1", "Furkan")

```

A map-wide event is fired as a result of a map-wide operation. For example, `MultiMap.Clear()`. You should implement the `MapClearedListener` interface to listen to these events. A `MapEvent` object is passed to the listener function.

See the following example.

```go
type EntryListener struct {
}

func (l *EntryListener) MapCleared(event core.MapEvent) {
	log.Println("Map Cleared:", event.NumberOfAffectedEntries()) // Map Cleared: 1
}
multiMap.AddEntryListener(&EntryListener{}, true)
multiMap.Put("1", "Muhammet Ali")
multiMap.Put("1", "Ahmet")
multiMap.Put("1", "Furkan")
multiMap.Clear()
```

See the following headings to see supported listener functions for each data structure.

**Entry Listener Functions Supported by MultiMap**

- `EntryAdded`
- `EntryRemoved`
- `EntryEvicted`
- `MapCleared`

**Entry Listener Functions Supported by Replicated Map**

- `EntryAdded`
- `EntryUpdated`
- `EntryRemoved`
- `EntryEvicted`
- `MapCleared`

As you see, there is a parameter in the `AddEntryListener` function: `includeValue`. It is a boolean parameter, and if it is `true`, the entry event contains the entry value.

#### 7.5.2.3. Item Listener

The Item Listener is used by the Hazelcast `Queue`, `Set` and `List`.

You can listen to item events by implementing the `ItemAddedListener` or `ItemRemovedListener` interface. Their functions are invoked when an item is added or removed.

The following is an example of item listener object and its registration to the `Set`. It also applies to `Queue` and `List`.

```go
type itemListener struct {
}

func (l *itemListener) ItemAdded(event core.ItemEvent) {
	log.Println("Item added:", event.Item()) // Item added: Furkan
}

func (l *itemListener) ItemRemoved(event core.ItemEvent) {
	log.Println("Item removed:", event.Item()) // Item removed: Furkan
}

set.AddItemListener(&itemListener{}, true)
set.Add("Furkan")
set.Remove("Furkan")
```

As you see, there is a parameter in the `AddItemListener` function: `includeValue`. It is a boolean parameter, and if it is `true`, the item event contains the item value.

#### 7.5.2.4. Message Listener

The Message Listener is used by the Hazelcast `Reliable Topic` and `Topic`.

You can listen to message events. To listen to these events, you need to implement the `MessageListener` interface.

See the following example.

```go
type topicMessageListener struct {
}

func (l *topicMessageListener) OnMessage(message core.Message) error {
	log.Println(message.MessageObject()) // furkan
	return nil
}

topic.AddMessageListener(&topicMessageListener{})
topic.Publish("furkan")
```

## 7.6. Distributed Computing

This chapter explains how you can use Hazelcast IMDG's entry processor implementation in the Go client.

### 7.6.1. Using EntryProcessor

Hazelcast supports entry processing. An entry processor is a function that executes your code on a map entry in an atomic way.

An entry processor is a good option if you perform bulk processing on an `Map`. Usually you perform a loop of keys -- executing `Map.get(key)`, mutating the value, and finally putting the entry back in the map using `Map.put(key,value)`. If you perform this process from a client or from a member where the keys do not exist, you effectively perform two network hops for each update: the first to retrieve the data and the second to update the mutated value.

If you are doing the process described above, you should consider using entry processors. An entry processor executes a read and updates upon the member where the data resides. This eliminates the costly network hops described above.

> **NOTE: Entry processor is meant to process a single entry per call. Processing multiple entries and data structures in an entry processor is not supported as it may result in deadlocks on the server side.**

Hazelcast sends the entry processor to each cluster member and these members apply it to the map entries. Therefore, if you add more members, your processing completes faster.

#### Processing Entries

The `Map` interface provides the following functions for entry processing:

* `executeOnKey` processes an entry mapped by a key.

* `executeOnKeys` processes entries mapped by a list of keys.

* `executeOnEntries` can process all entries in a map.

* `executeOnEntriesWithPredicate` can process all entries in a map with a defined predicate. 

In the Go client, an `EntryProcessor` should be `IdentifiedDataSerializable` , `Portable` or `Custom Serializable` because the server should be able to deserialize it to process.

The following is an example for `EntryProcessor` which is `IdentifiedDataSerializable`.

```go
type identifiedEntryProcessor struct {
	value             string
}

func (p *identifiedEntryProcessor) ReadData(input serialization.DataInput) error {
	p.value = input.ReadUTF()
	return input.Error()
}

func (p *identifiedEntryProcessor) WriteData(output serialization.DataOutput) error {
	output.WriteUTF(p.value)
	return nil
}

func (p *identifiedEntryProcessor) FactoryID() int32 {
	return 5
}

func (p *identifiedEntryProcessor) ClassID() int32 {
	return 1
}
```

Now, you need to make sure that the Hazelcast member recognizes the entry processor. For this, you need to implement the Java equivalent of your entry processor and its factory and create your own compiled class or JAR files. For adding your own compiled class or JAR files to the server's `CLASSPATH`, see the [Adding User Library to CLASSPATH section](#1213-adding-user-library-to-classpath).

The following is the Java equivalent of the entry processor in Go client given above:

```java
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import java.io.IOException;
import java.util.Map;

public class IdentifiedEntryProcessor extends AbstractEntryProcessor<String, String> implements IdentifiedDataSerializable {
     static final int CLASS_ID = 1;
     private String value;
     
    public IdentifiedEntryProcessor() {
    }
    
     @Override
    public int getFactoryId() {
        return IdentifiedFactory.FACTORY_ID;
    }
    
     @Override
    public int getId() {
        return CLASS_ID;
    }
    
     @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(value);
    }
    
     @Override
    public void readData(ObjectDataInput in) throws IOException {
        value = in.readUTF();
    }
    
     @Override
    public Object process(Map.Entry<String, String> entry) {
        entry.setValue(value);
        return value;
    }
}
```

You can implement the above processor’s factory as follows:

```java
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class IdentifiedFactory implements DataSerializableFactory {
    public static final int FACTORY_ID = 5;
    
     @Override
    public IdentifiedDataSerializable create(int typeId) {
        if (typeId == IdentifiedEntryProcessor.CLASS_ID) {
            return new IdentifiedEntryProcessor();
        }
        return null;
    }
}
```

Now you need to configure the `hazelcast.xml` to add your factory as shown below.

```xml
<hazelcast>
    <serialization>
        <data-serializable-factories>
            <data-serializable-factory factory-id="5">
                IdentifiedFactory
            </data-serializable-factory>
        </data-serializable-factories>
    </serialization>
</hazelcast>
```

The code that runs on the entries is implemented in Java on the server side. The client side entry processor is used to specify which entry processor should be called. For more details about the Java implementation of the entry processor, see the [Entry Processor section](https://docs.hazelcast.org/docs/latest/manual/html-single/index.html#entry-processor) in the Hazelcast IMDG Reference Manual.

After the above implementations and configuration are done and you start the server where your library is added to its `CLASSPATH`, you can use the entry processor in the `Map` functions. Let's take a look at the following example.

```go
config := hazelcast.NewConfig()
identifiedFactory := &identifiedFactory{}
config.SerializationConfig().AddDataSerializableFactory(5, identifiedFactory)
client, _ := hazelcast.NewClientWithConfig(config)

mp, _ := client.GetMap("my-distributed-map")
mp.Put("key", "not-processed")
processor := &identifiedEntryProcessor{ value: value}
value, _ := mp.ExecuteOnKey("key", processor)

fmt.Println("after processing the new value is ", value)

newValue, _ := mp.Get("key")
fmt.Println("after processing the new value is ", newValue)
```

## 7.7. Distributed Query

Hazelcast partitions your data and spreads it across cluster of members. You can iterate over the map entries and look for certain entries (specified by predicates) you are interested in. However, this is not very efficient because you will have to bring the entire entry set and iterate locally. Instead, Hazelcast allows you to run distributed queries on your distributed map.

### 7.7.1. How Distributed Query Works

1. The requested predicate is sent to each member in the cluster.
2. Each member looks at its own local entries and filters them according to the predicate. At this stage, key-value pairs of the entries are deserialized and then passed to the predicate.
3. The predicate requester merges all the results coming from each member into a single set.

Distributed query is highly scalable. If you add new members to the cluster, the partition count for each member is reduced and thus the time spent by each member on iterating its entries is reduced. In addition, the pool of partition threads evaluates the entries concurrently in each member, and the network traffic is also reduced since only filtered data is sent to the requester.

If queried item is Portable, it can be queried for the fields without deserializing the data at the server side and hence no server side implementation of the queried object class will be needed.

**Predicates Object Operators**

The `predicate` package offered by the client includes many operators for your query requirements. Some of them are described below.

* `equal`: Checks if the result of an expression is equal to a given value.

* `notEqual`: Checks if the result of an expression is not equal to a given value.

* `instanceOf`: Checks if the result of an expression has a certain type.

* `like`: Checks if the result of an expression matches some string pattern. `%` (percentage sign) is the placeholder for many characters, `_` (underscore) is placeholder for only one character.

* `greaterThan`: Checks if the result of an expression is greater than a certain value.

* `greaterEqual`: Checks if the result of an expression is greater than or equal to a certain value.

* `lessThan`: Checks if the result of an expression is less than a certain value.

* `lessEqual`: Checks if the result of an expression is less than or equal to a certain value.

* `between`: Checks if the result of an expression is between two values, inclusively.

* `in`: Checks if the result of an expression is an element of a certain list.

* `not`: Checks if the result of an expression is false.

* `regex`: Checks if the result of an expression matches some regular expression.

Hazelcast offers the following ways for distributed query purposes:

* Combining Predicates with AND, OR, NOT

* Distributed SQL Query

#### 7.7.1.1. Employee Map Query Example

Assume that you have an `employee` map containing the values of `Employee` objects, as coded below. 

```go
type Employee struct {
	name   string
	age    int32
	active bool
	salary int64
}

func (e *Employee) ReadPortable(reader serialization.PortableReader) error {
	e.name = reader.ReadUTF("name")
	e.age = reader.ReadInt32("age")
	e.active = reader.ReadBool("active")
	e.salary = reader.ReadInt64("salary")
	return reader.Error()
}

func (e *Employee) WritePortable(writer serialization.PortableWriter) error {
	writer.WriteUTF("name", e.name)
	writer.WriteInt32("age", e.age)
	writer.WriteBool("active", e.active)
	writer.WriteInt64("salary", e.salary)
	return nil
}

func (e *Employee) FactoryID() int32 {
	return 1
}

func (e *Employee) ClassID() int32 {
	return 1
}

```

Note that `Employee` is implementing `Portable`. As portable types are not deserialized on the server side for querying, you don't need to implement its Java equivalent on the server side.

 For the non-portable types, you need to implement its Java equivalent and its serializable factory on the server side for server to reconstitute the objects from binary formats. 
 In this case before starting the server, you need to compile the `Employee` and related factory classes with server's `CLASSPATH` and add them to the `user-lib` directory in the extracted `hazelcast-<version>.zip` (or `tar`). See the [Adding User Library to CLASSPATH section](#1213-adding-user-library-to-classpath).

 > **NOTE: Querying with `Portable` object is faster as compared to `IdentifiedDataSerializable`.**


#### 7.7.1.2. Querying by Combining Predicates with AND, OR, NOT

You can combine predicates by using the `and`, `or` and `not` operators, as shown in the below example.

```go
mp, _ := client.GetMap("emloyees")
prdct := predicate.And(predicate.Equal("active", true), predicate.LessThan("age", 30))
value, _ := mp.ValuesWithPredicate(prdct)
```

In the above example code, `predicate` verifies whether the entry is active and its `age` value is less than 30. This `predicate` is applied to the `employee` map using the `map.ValuesWithPredicate(predicate)` method. This method sends the predicate to all cluster members and merges the results coming from them. 

> **NOTE: Predicates can also be applied to `keySet` and `entrySet` of the Hazelcast IMDG's distributed map.**

#### 7.7.1.3. Querying with SQL

`predicate.SQL` takes the regular SQL `where` clause. Here is an example:

```go
mp, _ := client.GetMap("employees")
prdct := predicate.And(predicate.SQL("active AND age < 30"))
value, _ := mp.ValuesWithPredicate(prdct)
```

##### Supported SQL Syntax

**AND/OR:** `<expression> AND <expression> AND <expression>…`
   
- `active AND age > 30`
- `active = false OR age = 45 OR name = 'Joe'`
- `active AND ( age > 20 OR salary < 60000 )`

**Equality:** `=, !=, <, ⇐, >, >=`

- `<expression> = value`
- `age <= 30`
- `name = 'Joe'`
- `salary != 50000`

**BETWEEN:** `<attribute> [NOT] BETWEEN <value1> AND <value2>`

- `age BETWEEN 20 AND 33 ( same as age >= 20 AND age ⇐ 33 )`
- `age NOT BETWEEN 30 AND 40 ( same as age < 30 OR age > 40 )`

**IN:** `<attribute> [NOT] IN (val1, val2,…)`

- `age IN ( 20, 30, 40 )`
- `age NOT IN ( 60, 70 )`
- `active AND ( salary >= 50000 OR ( age NOT BETWEEN 20 AND 30 ) )`
- `age IN ( 20, 30, 40 ) AND salary BETWEEN ( 50000, 80000 )`

**LIKE:** `<attribute> [NOT] LIKE 'expression'`

The `%` (percentage sign) is the placeholder for multiple characters, an `_` (underscore) is the placeholder for only one character.

- `name LIKE 'Jo%'` (true for 'Joe', 'Josh', 'Joseph' etc.)
- `name LIKE 'Jo_'` (true for 'Joe'; false for 'Josh')
- `name NOT LIKE 'Jo_'` (true for 'Josh'; false for 'Joe')
- `name LIKE 'J_s%'` (true for 'Josh', 'Joseph'; false 'John', 'Joe')

**ILIKE:** `<attribute> [NOT] ILIKE 'expression'`

ILIKE is similar to the LIKE predicate but in a case-insensitive manner.

- `name ILIKE 'Jo%'` (true for 'Joe', 'joe', 'jOe','Josh','joSH', etc.)
- `name ILIKE 'Jo_'` (true for 'Joe' or 'jOE'; false for 'Josh')

**REGEX:** `<attribute> [NOT] REGEX 'expression'`

- `name REGEX 'abc-.*'` (true for 'abc-123'; false for 'abx-123')

##### Querying Examples with Predicates

You can use the `__key` attribute to perform a predicated search for entry keys. Please see the following example:

```go
personMap, _ := client.GetMap("persons")
personMap.Put("Ahmet", 28)
personMap.Put("Ali", 30)
personMap.Put("Furkan", 23)
value , _ := personMap.ValuesWithPredicate(predicate.SQL("__key like F%"))
fmt.Println(value) //[23]
```

In this example, the code creates a slice with the values whose keys start with the letter "F”.

You can use the `this` attribute to perform a predicated search for entry values. See the following example:

```go
personMap, _ := client.GetMap("persons")
personMap.Put("Ahmet", 28)
personMap.Put("Ali", 30)
personMap.Put("Furkan", 23)
value , _ := personMap.ValuesWithPredicate(predicate.GreaterEqual("this", 27))
fmt.Println(value) //[28 30]
```

In this example, the code creates a slice with the values greater than or equal to "27".

#### 7.7.1.4. Querying with JSON Strings

You can query JSON strings stored inside your Hazelcast clusters. To query the JSON string,
you first need to create a `HazelcastJSONValue` from the JSON string. You can use ``HazelcastJSONValue``s both as keys and values in the distributed data structures. Then, it is
possible to query these objects using the Hazelcast query methods explained in this section.

```go
person1 , _ := core.CreateHazelcastJSONValueFromString{"{ \"name\": \"John\", \"age\": 35 }"}
person2 , _ := core.CreateHazelcastJSONValueFromString{"{ \"name\": \"Jane\", \"age\": 24 }"}
person3 , _ := core.CreateHazelcastJSONValueFromString{"{ \"name\": \"Trey\", \"age\": 17 }"}

mp.Put(1, person1)
mp.Put(2, person2)
mp.Put(3, person3)

peopleUnder21, _ := mp.ValuesWithPredicate(predicate.LessThan("age", 21))
```

When running the queries, Hazelcast treats values extracted from the JSON documents as Java types so they
can be compared with the query attribute. JSON specification defines five primitive types to be used in the JSON
documents: `number`,`string`, `true`, `false` and `nil`. The `string`, `true/false` and `nil` types are treated
as `String`, `boolean` and `null`, respectively. We treat the extracted `number` values as ``long``s if they
can be represented by a `long`. Otherwise, ``number``s are treated as ``double``s.

It is possible to query nested attributes and arrays in the JSON documents. The query syntax is the same
as querying other Hazelcast objects using the ``Predicate``s.

```go
/**
* Sample JSON object
*
* {
*     "departmentId": 1,
*     "room": "alpha",
*     "people": [
*         {
*             "name": "Peter",
*             "age": 26,
*             "salary": 50000
*         },
*         {
*             "name": "Jonah",
*             "age": 50,
*             "salary": 140000
*         }
*     ]
* }
*
*
* The following query finds all the departments that have a person named "Peter" working in them.
*/

departmentWithPeter, _  := departments.values(predicate.Equal("people[any].name", "Peter"))

```

`HazelcastJSONValue` is a lightweight wrapper around your JSON strings. It is used merely as a way to indicate
that the contained string should be treated as a valid JSON value. Hazelcast does not check the validity of JSON
strings put into to the maps. Putting an invalid JSON string into a map is permissible. However, in that case
whether such an entry is going to be returned or not from a query is not defined.

### 7.7.2. Fast-Aggregations

Fast-Aggregations feature provides some aggregate functions, such as `sum`, `average`, `max`, and `min`, on top of Hazelcast `Map` entries. Their performance is perfect since they run in parallel for each partition and are highly optimized for speed and low memory consumption.

The `aggregator` package provides a wide variety of built-in aggregators. The full list is presented below:

- `Count`
- `Float64Average`
- `Float64Sum`
- `FixedPointSum`
- `FloatingPointSum`
- `Max`
- `Min`
- `Int32Average`
- `Int32Sum`
- `Int64Average`
- `Int64Sum`

You can use these aggregators with the `Map.Aggregate()` and `Map.AggregateWithPredicate()` functions.

See the following example.

```go
mp, _ := client.GetMap("brothersMap")
mp.Put("Muhammet Ali", 30)
mp.Put("Ahmet", 27)
mp.Put("Furkan", 23)
agg, _ := aggregator.Count("this")
count, _ := mp.Aggregate(agg)
fmt.Println("There are", count, "brothers.") // There are 3 brothers.
count, _ = mp.AggregateWithPredicate(agg, predicate.GreaterThan("this", 25))
fmt.Println("There are", count, "brothers older than 25.") // There are 2 brothers older than 25.
avg, _ := aggregation.NewInt64Average("this")
avgAge, _ := mp.Aggregate(avg)
fmt.Println("Average age is", avgAge) // Average age is 26.666666666666668
```


## 7.8. Monitoring


 ### 7.8.1. Enabling Client Statistics

You can monitor your clients using Hazelcast Management Center.

 As a prerequisite, you need to enable the client statistics before starting your clients. This can be done by setting the `hazelcast.client.statistics.enabled` system property to `true` on the **member** as the following:

 ```xml
 <hazelcast>
     ...
     <properties>
         <property name="hazelcast.client.statistics.enabled">true</property>
     </properties>
     ...
 </hazelcast>
 ```

 Also, you need to enable the client statistics in the Go client. There are two properties related to client statistics:

 - `hazelcast.client.statistics.enabled`: If set to `true`, it enables collecting the client statistics and sending them to the cluster. When it is `true` you can monitor the clients that are connected to your Hazelcast cluster, using Hazelcast Management Center. Its default value is `false`.

 - `hazelcast.client.statistics.period.seconds`: Period in seconds the client statistics are collected and sent to the cluster. Its default value is `3`.

 You can enable client statistics and set a non-default period in seconds as follows:

 ```go
config := hazelcast.NewConfig()
config.SetProperty(property.StatisticsEnabled.Name(), "true")
config.SetProperty(property.StatisticsPeriodSeconds.Name(), "4")
 ```

 After enabling the client statistics, you can monitor your clients using Hazelcast Management Center. See the [Monitoring Clients section](https://docs.hazelcast.org/docs/management-center/latest/manual/html/index.html#monitoring-clients) in the Hazelcast Management Center Reference Manual for more information on the client statistics.
 
### 7.8.2. Logging Configuration

By default Hazelcast Go client uses DefaultLogger for logging. The default logging level is
`info`. If you want to change the logging level for the client, you should use `LoggingLevel` property:

```go
config := hazelcast.NewConfig()
config.SetProperty(property.LoggingLevel.Name(), logger.ErrorLevel)
``` 

As described in [Client System Properties Section](#143-client-system-properties) you can also 
set the log level via an environment variable with this property:

```go
os.Setenv(property.LoggingLevel.Name(), logger.ErrorLevel)
```

If you are using a custom logger, `LoggingLevel` property will not be used.

Possible log levels are as follows:
```go
// OffLevel disables logging.
OffLevel = "off"
// ErrorLevel level. Logs. Used for errors that should definitely be noted.
// Commonly used for hooks to send errors to an error tracking service.
ErrorLevel = "error"
// WarnLevel level. Non-critical entries that deserve eyes.
WarnLevel = "warn"
// InfoLevel level. General operational entries about what's going on inside the
// application.
InfoLevel = "info"
// DebugLevel level. Usually only enabled when debugging. Very verbose logging.
DebugLevel = "debug"
// TraceLevel level. Designates finer-grained informational events than the Debug.
TraceLevel = "trace"
```

The Default Logger's format is as follows:

```
[Time] [caller method name]
[Log level] [Client Name] [Group Name] [Client Version] [Log Message]
```

An example default log message is as follows:
```
2018/11/30 17:48:52 github.com/hazelcast/hazelcast-go-client/internal.(*lifecycleService).fireLifecycleEvent
INFO:  hz.client_1 [dev] [0.4] New State :  STARTED
```

If you want to modify the Default Logger for your convenience, you can do so by accessing the embedded built-in go logger:
```go
l := logger.New()
l.SetPrefix("myPrefix ")
config := hazelcast.NewConfig()
config.LoggerConfig().SetLogger(l)
``` 

The same log message will now be as follows:

```
myPrefix 2018/11/30 17:55:40 github.com/hazelcast/hazelcast-go-client/internal.(*lifecycleService).fireLifecycleEvent
INFO:  hz.client_1 [dev] [0.4] New State :  CONNECTED
```


If you want to set a custom logger, you can do so by implementing `Logger` interface:

```go
type customLogger struct {
}

func (c *customLogger) Debug(args ...interface{}) {
}

func (c *customLogger) Trace(args ...interface{}) {
}

func (c *customLogger) Info(args ...interface{}) {
	log.Println(args)
}

func (c *customLogger) Warn(args ...interface{}) {
}

func (c *customLogger) Error(args ...interface{}) {
}
```

Note that the `customLogger` only logs `Info` level to console. This way users can
implement only the levels they need in the format they want to.

After implementing the `Logger` interface, you need to set it as the client's logger:

```go
customLogger := &customLogger{}
config := hazelcast.NewConfig()
config.LoggerConfig().SetLogger(customLogger)
```

Note that when you call `SetLogger` method, the hazelcast property `LoggingLevel` will not be used.

You can also integrate any third party logger with `Logger` interface:

#### Logrus

```go
logger := logrus.New()
logger.SetLevel(logrus.DebugLevel)
config := hazelcast.NewConfig()
config.LoggerConfig().SetLogger(logger)
```

The same log message will now be:

```
time="2018-11-30T18:09:52+03:00" level=info msg="New State : CONNECTED"
```

Note that Logrus already implements the `Logger` interface, therefore we did not need to do any extra work.


#### Zap

```go
type zapLogger struct {
	*zap.Logger
}

func (z *zapLogger) Debug(args ...interface{}) {
	message := fmt.Sprintln(args)
	z.Logger.Debug(message)
}

func (z *zapLogger) Trace(args ...interface{}) {
	message := fmt.Sprintln(args)
	z.Logger.Debug(message)
}

func (z *zapLogger) Info(args ...interface{}) {
	message := fmt.Sprintln(args)
	z.Logger.Info(message)
}

func (z *zapLogger) Warn(args ...interface{}) {
	message := fmt.Sprintln(args)
	z.Logger.Warn(message)
}

func (z *zapLogger) Error(args ...interface{}) {
	message := fmt.Sprintln(args)
	z.Logger.Error(message)
}
```

Then, you need to set it as the client's logger:

```go
opt := zap.AddCallerSkip(1)
logger, _ := zap.NewProduction(opt)
zapLogger := &zapLogger{logger}
config := hazelcast.NewConfig()
config.LoggerConfig().SetLogger(zapLogger)
```

Note that we call `zap.AddCallerSkip(1)` to skip our wrapper method.

The same log message will now be:

```
{"level":"info","ts":1543594846.9142249,"caller":"internal/lifecycle.go:89","msg":"[New State :  CONNECTED]\n"}
```



#### Glog

```go
type gLogger struct {
}

func (*gLogger) Debug(args ...interface{}) {
	// NO OP
}

func (*gLogger) Trace(args ...interface{}) {
	// NO OP
}

func (*gLogger) Info(args ...interface{}) {
	glog.Info(args)
}

func (*gLogger) Warn(args ...interface{}) {
	glog.Warning(args)
}

func (*gLogger) Error(args ...interface{}) {
	glog.Error(args)
}
```

Then, you need to set it as the client's logger:

```go
config := hazelcast.NewConfig()
config.LoggerConfig().SetLogger(&gLogger{})
```



# 8. Development and Testing

If you want to help with bug fixes, develop new features or tweak the implementation to your application's needs, 
you can follow the steps in this section.

## 8.1. Building and Using Client From Sources

Follow the below steps to build and install Hazelcast Go client from its source:

- Clone the GitHub repository [https://github.com/hazelcast/hazelcast-go-client.git](https://github.com/hazelcast/hazelcast-go-client.git).
- Run `sh build.sh`.

If you are planning to contribute, please run the style checker, as shown below, and fix the reported issues before sending a pull request.
- `sh linter.sh`

## 8.2. Testing
In order to test Hazelcast Go client locally, you will need the following:
* Java 6 or newer
* Maven

Following command starts the tests:

```
sh local-test.sh
```

Test script automatically downloads `hazelcast-remote-controller` and Hazelcast IMDG. The script uses Maven to download those.

# 9. Getting Help

You can use the following channels for your questions and development/usage issues:

* [This repository](https://github.com/hazelcast/hazelcast-go-client) by opening an issue.
* Our Google Groups directory: https://groups.google.com/forum/#!forum/hazelcast
* Stack Overflow: https://stackoverflow.com/questions/tagged/hazelcast

# 10. Contributing

Besides your development contributions as explained in the [Development and Testing chapter](#8-development-and-testing) above, you can always open a pull request on this repository for your other requests such as documentation changes.

# 11. License

[Apache 2 License](https://github.com/hazelcast/hazelcast-go-client/blob/master/LICENSE).

# 12. Copyright

Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com) for more information.
