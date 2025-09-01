package main

import (
	"log/slog"
	"runtime/debug"
)

func logBuildInfo(logger *slog.Logger) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		logger.Error("Build info unavailable")
		return
	}

	settings := map[string]string{}

	for _, s := range info.Settings {
		settings[s.Key] = s.Value
	}

	logger.Info("Build info",
		slog.String("go_version", info.GoVersion),
		slog.String("main.path", info.Main.Path),
		slog.Any("settings", settings),
	)
}
