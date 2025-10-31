# SFTP with Toxiproxy Setup

This setup includes an SFTP server and Toxiproxy for simulating network conditions during testing.

## Services

- **SFTP**: A simple SFTP server with a test user
- **Toxiproxy**: A TCP proxy that allows simulating network conditions like latency, bandwidth restrictions, and connection failures

## Getting Started

1. Start the services:

```bash
docker-compose up -d
```

2. Connect to SFTP via the Toxiproxy port:

```bash
sftp -P 2222 foo@localhost
# Password: pass
```

3. Control Toxiproxy via its API (port 8474):

```bash
# Add 1000ms latency to SFTP connections
curl -X POST -H "Content-Type: application/json" \
  http://localhost:8474/proxies/sftp/toxics \
  -d '{"type":"latency", "attributes":{"latency":1000, "jitter":0}}'  

# Simulate connection timeout
curl -X POST -H "Content-Type: application/json" \
  http://localhost:8474/proxies/sftp/toxics \
  -d '{"type":"timeout", "attributes":{"timeout":1000}}'

# Remove all toxics
curl -X GET http://localhost:8474/proxies/sftp/toxics
```

See the [Toxiproxy documentation](https://github.com/Shopify/toxiproxy) for more information on available toxics and configuration options.
