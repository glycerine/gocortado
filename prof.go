package gocortado

import (
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" // for web based profiling while running
	"os"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/glycerine/cryrand"
)

func startProfiling(web bool, cpuProfileOutPath, memoryProfileOutPath string) {
	if web {
		AlwaysPrintf("webprofile requested, about to try and bind 127.0.0.1:7070")
		go func() {
			http.ListenAndServe("127.0.0.1:7070", nil)
			// hmm if we get here we couldn't bind 7070.
			startOnlineWebProfiling()
		}()
	}

	if cpuProfileOutPath != "" {
		startProfilingCPU(cpuProfileOutPath)
		//defer pprof.StopCPUProfile() // backup plan if we exit early.
	}

	if memoryProfileOutPath != "" {
		startProfilingMemory(memoryProfileOutPath)
	}

}

func startOnlineWebProfiling() (port int) {

	// To dump goroutine stack from a running program for debugging:
	// Start an HTTP listener if you do not have one already:
	// Then point a browser to http://127.0.0.1:9999/debug/pprof for a menu, or
	// curl http://127.0.0.1:9999/debug/pprof/goroutine?debug=2
	// for a full dump.
	port = GetAvailPort()
	go func() {
		err := http.ListenAndServe(fmt.Sprintf("127.0.0.1:%v", port), nil)
		if err != nil {
			panic(err)
		}
	}()
	fmt.Fprintf(os.Stderr, "\n for stack dump:\n\ncurl http://127.0.0.1:%v/debug/pprof/goroutine?debug=2\n\n for general debugging:\n\nhttp://127.0.0.1:%v/debug/pprof\n\n", port, port)
	return
}

func startProfilingMemory(path string) {
	// add randomness so two tests run at once don't overwrite each other.
	rnd8 := cryrand.RandomStringWithUp(8)
	fn := path + ".memprof." + rnd8
	wait := time.Minute
	AlwaysPrintf("will write mem profile to '%v'; after wait of '%v'", fn, wait)
	go func() {
		time.Sleep(wait)
		WriteMemProfiles(fn)
	}()
}

func WriteMemProfiles(fn string) {
	if !strings.HasSuffix(fn, ".") {
		fn += "."
	}
	h, err := os.Create(fn + "heap")
	panicOn(err)
	defer h.Close()
	a, err := os.Create(fn + "allocs")
	panicOn(err)
	defer a.Close()
	g, err := os.Create(fn + "goroutine")
	panicOn(err)
	defer g.Close()

	hp := pprof.Lookup("heap")
	ap := pprof.Lookup("allocs")
	gp := pprof.Lookup("goroutine")

	panicOn(hp.WriteTo(h, 1))
	panicOn(ap.WriteTo(a, 1))
	panicOn(gp.WriteTo(g, 2))
}

// GetAvailPort asks the OS for an unused port.
// There's a race here, where the port could be grabbed by someone else
// before the caller gets to Listen on it, but in practice such races
// are rare. Uses net.Listen("tcp", ":0") to determine a free port, then
// releases it back to the OS with Listener.Close().
func GetAvailPort() int {
	l, _ := net.Listen("tcp", ":0")
	r := l.Addr()
	l.Close()
	return r.(*net.TCPAddr).Port
}
