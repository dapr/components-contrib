package runtime

import "github.com/valyala/fasthttp"

type WASMRuntime interface {
	Run(ctx *fasthttp.RequestCtx) error
}

func NewWASMRuntime(name, path string) WASMRuntime {
	switch name {
	case "wasmedge":
		return NewWASMEdge(path)
	}

	return nil
}
