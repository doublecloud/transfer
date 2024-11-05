package serverutil

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/doublecloud/transfer/internal/logger"
	"go.ytsaurus.tech/library/go/core/log"
)

func PingFunc(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	w.Header().Set("Content-Type", "Application/json")
	res, _ := json.Marshal(map[string]interface{}{"ping": "pong", "ts": time.Now()})
	if _, err := w.Write(res); err != nil {
		logger.Log.Error("unable to write", log.Error(err))
		return
	}
}

func RunHealthCheck() {
	RunHealthCheckOnPort(80)
}

func RunHealthCheckOnPort(port int) {
	rootMux := http.NewServeMux()
	rootMux.HandleFunc("/ping", PingFunc)
	logger.Log.Infof("healthcheck is upraising on port 80")
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), rootMux); err != nil { // it must be on 80 port - bcs of dataplane instance-group
		logger.Log.Error("failed to serve health check", log.Error(err))
	}
}

func RunPprof() {
	rootMux := http.NewServeMux()
	rootMux.HandleFunc("/debug/pprof/", pprof.Index)
	rootMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	rootMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	rootMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	rootMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	logger.Log.Infof("init pprof on port 8080") // on 8080, bcs YT-vanilla forbid listen 80 port
	if err := http.ListenAndServe(":8080", rootMux); err != nil {
		logger.Log.Info("failed to serve pprof on 8080, try random port", log.Error(err))
		listener, err := net.Listen("tcp", ":0")
		if err != nil {
			logger.Log.Error("failed to add listener for pprof", log.Error(err))
			return
		}
		logger.Log.Infof("pprof listen on: %v", listener.Addr().String())
		if err := http.Serve(listener, rootMux); err != nil {
			logger.Log.Error("failed to serve pprof", log.Error(err))
		}
	}
}
