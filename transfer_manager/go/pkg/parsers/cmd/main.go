package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue"
	"github.com/doublecloud/tross/library/go/core/log"
	"github.com/doublecloud/tross/transfer_manager/go/internal/logger"
	"github.com/doublecloud/tross/transfer_manager/go/internal/metrics"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/abstract"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/format"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsers"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/parsers/registry/logfeller/lib"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/providers/logbroker"
	"github.com/doublecloud/tross/transfer_manager/go/pkg/stats"
	"go.ytsaurus.tech/yt/go/yson"
)

var (
	cpuprofile    = flag.String("cpuprofile", "cpu.prof", "write cpu profile to `file`")
	memprofile    = flag.String("memprofile", "mem.prof", "write memory profile to `file`")
	command       = flag.String("command", "parse", "")
	sampleFormat  = flag.String("sample-format", "yson", "")
	file          = flag.String("file", "./sample", "")
	logFormat     = flag.String("format", "logbroker-ytsender-ng-log", "")
	chunkSplitter = flag.String("splitter", "line-break", "")
	repeats       = flag.Int("repeats", 1000, "")
)

func profile() {
	go func() {
		logger.Log.Error("listen and server", log.Error(http.ListenAndServe("localhost:8181", nil)))
	}()
}

func main() {
	var raw []byte
	flag.Parse()
	profile()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			logger.Log.Error("could not create CPU profile: ", log.Error(err))
		}
		defer func() { _ = f.Close() }()
		if err := pprof.StartCPUProfile(f); err != nil {
			logger.Log.Error("could not start CPU profile: ", log.Error(err))
		}
		defer pprof.StopCPUProfile()
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			logger.Log.Error("could not create memory profile: ", log.Error(err))
		}
		defer func() { _ = f.Close() }()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			logger.Log.Error("could not write memory profile: ", log.Error(err))
		}
	}

	if *sampleFormat == "yson" {
		r, err := ioutil.ReadFile(*file)
		if err != nil {
			logger.Log.Error("Read file error", log.Error(err))
			return
		}

		for _, rw := range strings.Split(string(r), "\n") {
			var row struct {
				Key   string `yson:"key"`
				Value string `yson:"value"`
			}
			decoder := yson.NewDecoderFromBytes([]byte(rw))
			if err := decoder.Decode(&row); err != nil {
				continue
			}

			raw = append(raw, []byte(row.Value)...)
			raw = append(raw, []byte("\n")...)
		}

	} else {
		r, err := ioutil.ReadFile(*file)
		if err != nil {
			logger.Log.Error("Read file error", log.Error(err))
			return
		}

		raw = r
	}

	for i := 0; i < 10; i++ {
		raw = append(raw, raw...)
	}

	if *command == "parse" {
		d := lib.Parse(*logFormat, *chunkSplitter, *command, false, persqueue.ReadMessage{Data: raw})
		reader := yson.NewReaderKindFromBytes([]byte(d), yson.StreamListFragment)
		for {
			ok, err := reader.NextListItem()
			if !ok {
				logger.Log.Info("done")
				break
			}
			if err != nil {
				logger.Log.Infof("err %v", err)
				break
			}
			var res interface{}
			current, err := reader.NextRawValue()
			if err != nil {
				logger.Log.Infof("err %v", err)
				break
			}
			if err := yson.Unmarshal(current, &res); err != nil {
				logger.Log.Infof("err %v", err)
				break
			}
			logger.Log.Debugf("foo: %v", res)
		}

		src := new(logbroker.LfSource)
		parserConfigObj, err := parsers.ParserConfigMapToStruct(src.ParserConfig)
		if err != nil {
			logger.Log.Errorf("unable to convert LfConfig to ParserConfig object, err: %s", err.Error())
			return
		}
		parser, err := parsers.NewParserFromParserConfig(parserConfigObj, false, logger.Log, stats.NewSourceStats(metrics.NewRegistry()))
		if err != nil {
			logger.Log.Errorf("unable to create parser, err: %s", err.Error())
			return
		}
		r := parser.Do(persqueue.ReadMessage{
			WriteTime: time.Now(),
			Data:      raw,
		}, abstract.Partition{Cluster: "", Partition: 0, Topic: ""})
		if len(r) == 0 {
			logger.Log.Error("Parse nothing")
			return
		}

		logger.Log.Info(fmt.Sprintf("Start parse %v", format.SizeInt(len(raw))))
		start := time.Now()
		for i := 0; i < *repeats; i++ {
			r := parser.Do(persqueue.ReadMessage{
				WriteTime: time.Now(),
				Data:      raw,
			}, abstract.Partition{Cluster: "", Partition: 0, Topic: ""})
			logger.Log.Infof("row count %v", len(r))
			time.Sleep(time.Millisecond)
		}
		end := time.Now()
		logger.Log.Info("Done", log.Any("total", end.Sub(start)))
	} else if *command == "schema" {
		schema := lib.Schema(*logFormat, *chunkSplitter)
		logger.Log.Info("Schema", log.Any("schema", schema))
	}
}
