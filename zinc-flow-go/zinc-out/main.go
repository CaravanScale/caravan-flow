package main

import (
	"time"
	"os/signal"
	"syscall"
	"github.com/ZincScale/zinc-stdlib/config"
	"github.com/ZincScale/zinc-stdlib/logging"
	"zinc-flow/core"
	"zinc-flow/fabric/registry"
	"zinc-flow/processors"
	"zinc-flow/fabric/runtime"
	"os"
	"zinc-flow/fabric/source"
	"net/http"
	"zinc-flow/fabric/api"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/main.zn:19
func main() {
	cfg := config.NewConfig()
	cfg.SetDefault("server.port", 9091)
	cfg.SetDefault("logging.level", "INFO")
	cfg.SetDefault("logging.handler", "text")
	cfg.SetDefault("logging.output", "stdout")
	cfg.SetDefault("content.dir", "/tmp/zinc-flow/content")
	cfg.Load("config", "yaml", ".")
	lm := logging.NewLogManager()
	lm.Setup(cfg)
	logging.Info("zinc-flow starting")
	contentDir := cfg.GetString("content.dir")
	store := core.NewFileContentStore(contentDir)
	contentProvider := core.NewContentProvider("content", store)
	contentProvider.Enable()
	configProvider := core.NewConfigProvider(cfg)
	configProvider.Enable()
	loggingProvider := core.NewLoggingProvider(lm)
	loggingProvider.Enable()
	globalCtx := core.NewProcessorContext()
	globalCtx.AddProvider(contentProvider)
	globalCtx.AddProvider(configProvider)
	globalCtx.AddProvider(loggingProvider)
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	logging.Info("registry loaded", "processors", len(reg.List()))
	fab := runtime.NewFabric(reg, globalCtx)
	fab.LoadFlow(cfg)
	fab.StartAsync()
	fab.Status()
	err := os.MkdirAll("/tmp/zinc-flow/output", 0o755)
	if err != nil {
		panic(err)
	}
	httpSource := source.NewHttpSource(fab, store)
	http.HandleFunc("/ingest", httpSource.Handler)
	http.HandleFunc("/health", httpSource.HealthHandler)
	api := api.NewApiHandler(fab)
	http.HandleFunc("/api/flow", api.FlowHandler)
	http.HandleFunc("/api/processors", api.ProcessorsHandler)
	http.HandleFunc("/api/processors/add", api.AddProcessorHandler)
	http.HandleFunc("/api/processors/remove", api.RemoveProcessorHandler)
	http.HandleFunc("/api/registry", api.RegistryHandler)
	http.HandleFunc("/api/stats", api.StatsHandler)
	http.HandleFunc("/api/stats/processors", api.ProcessorStatsHandler)
	http.HandleFunc("/api/dlq", api.DlqHandler)
	http.HandleFunc("/api/dlq/replay", api.DlqReplayHandler)
	http.HandleFunc("/api/dlq/replay-all", api.DlqReplayAllHandler)
	http.HandleFunc("/api/dlq/delete", api.DlqDeleteHandler)
	http.HandleFunc("/api/processors/enable", api.EnableProcessorHandler)
	http.HandleFunc("/api/processors/disable", api.DisableProcessorHandler)
	http.HandleFunc("/api/processors/state", api.ProcessorStateHandler)
	http.HandleFunc("/api/providers", api.ProvidersHandler)
	http.HandleFunc("/api/providers/enable", api.EnableProviderHandler)
	http.HandleFunc("/api/providers/disable", api.DisableProviderHandler)
	go func() {
		for true {
			time.Sleep(30 * time.Second)
			fab.Status()
		}
	}()
	port := cfg.GetString("server.port")
	logging.Info("listening", "port", port)
	go func() {
		_gerr1 := func() error {
			err2 := http.ListenAndServe(":" + port, nil)
			if err2 != nil {
				return err2
			}
			return nil
		}()
		if _gerr1 != nil { panic(_gerr1) }
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit
	logging.Info("shutdown signal received")
	fab.StopAsync()
	time.Sleep(5 * time.Second)
	globalCtx.ShutdownAll()
	logging.Info("shutdown complete")
}

