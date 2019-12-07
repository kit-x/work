package work

import (
	"runtime"
	"strconv"
	"strings"

	"github.com/go-redis/redis"
)

func newTestClient(namespace ...string) *Client {
	var ns string
	if len(namespace) != 0 {
		ns = namespace[0]
	} else {
		ns = callerName(3)
	}

	return NewClient(ns, &redis.Options{Addr: ":6379"})
}

// callerFuncPos get caller's func name
func callerName(skip int) string {
	_, line, name := caller(skip)
	arr := strings.Split(name, "/")
	arr = append(arr, strconv.Itoa(line))
	if len(arr) >= 3 {
		return strings.Join(arr[2:], ".")
	}
	return strings.Join(arr, ".")
}

// caller file, file line, function name
func caller(skip int) (file string, line int, functionName string) {
	var (
		pc uintptr
		ok bool
	)
	pc, file, line, ok = runtime.Caller(skip)
	if !ok {
		return
	}

	return file, line, runtime.FuncForPC(pc).Name()
}
