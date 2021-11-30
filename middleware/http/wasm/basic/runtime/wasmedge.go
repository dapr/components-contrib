package runtime

import (
	"os"
	"strings"

	"github.com/second-state/WasmEdge-go/wasmedge"
	"github.com/valyala/fasthttp"
)

type WASMEdge struct {
	path string
}

func NewWASMEdge(path string) WASMRuntime {
	return &WASMEdge{
		path: path,
	}
}

func (r *WASMEdge) Run(ctx *fasthttp.RequestCtx) error {
	wasmedge.SetLogErrorLevel()
	conf := wasmedge.NewConfigure(wasmedge.WASI)
	store := wasmedge.NewStore()
	vm := wasmedge.NewVMWithConfigAndStore(conf, store)

	wasi := vm.GetImportObject(wasmedge.WASI)
	wasi.InitWasi(
		[]string{},
		os.Environ(),
		[]string{".:."},
	)

	if err := vm.LoadWasmFile(r.path); err != nil {
		return err
	}

	vm.Validate()
	vm.Instantiate()

	uri := string(ctx.RequestURI())
	lengthOfURI := len(uri)

	// Allocate memory for the subject, and get a pointer to it.
	// Include a byte for the NULL terminator we add below.
	allocateResult, _ := vm.Execute("malloc", int32(lengthOfURI+1))
	inputPointer := allocateResult[0].(int32)

	// Write the subject into the memory.
	mem := store.FindMemory("memory")
	memData, _ := mem.GetData(uint(inputPointer), uint(lengthOfURI+1))
	copy(memData, uri)

	// C-string terminates by NULL.
	memData[lengthOfURI] = 0

	// Run the `run` function. Given the pointer to the subject.
	helloResult, err := vm.Execute("run", inputPointer)
	if err != nil {
		return err
	}
	outputPointer := helloResult[0].(int32)

	pageSize := mem.GetPageSize()
	// Read the result of the `hello` function.
	memData, _ = mem.GetData(uint(0), pageSize*65536)
	nth := 0
	var output strings.Builder

	for {
		if memData[int(outputPointer)+nth] == 0 {
			break
		}

		output.WriteByte(memData[int(outputPointer)+nth])
		nth++
	}
	ctx.Request.SetRequestURI(output.String())
	// Deallocate the subject, and the output.
	vm.Execute("free", inputPointer)
	vm.Execute("free", outputPointer)

	vm.Release()
	store.Release()
	conf.Release()
	return nil
}
