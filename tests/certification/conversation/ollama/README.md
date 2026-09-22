# Ollama conversation certification testing

The certification test starts Ollama in Docker, pulls the `qwen2.5:0.5b`
model, starts an embedded Dapr sidecar, and verifies a Conversation Alpha2
request returns a model response.

Run from this directory with:

```bash
go test -v
```
