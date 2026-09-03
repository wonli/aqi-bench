package cmd

import (
	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
	"github.com/wonli/aqi"
	"github.com/wonli/aqi/logger"
	"go.uber.org/zap"

	"aqi-bench/internal/dbc"
	"aqi-bench/internal/router"
)

var quietLog bool

func init() {
	api.Flags().BoolVar(&quietLog, "quiet-log", false, "disable AQI zap output for benchmark A/B profiling")
	rootCmd.AddCommand(api)
}

var api = &cobra.Command{
	Use:   "api",
	Short: "启动API",
	Run: func(cmd *cobra.Command, args []string) {
		app := aqi.Init(
			aqi.ConfigFile(configFile),
			aqi.HttpServer("Api", "port"),
		)

		if quietLog {
			nop := zap.NewNop()
			logger.ZapLog = nop
			logger.RuntimeLog = nop
			logger.SugarLog = nop.Sugar()
		}

		dbc.InitDBC()

		g := gin.Default()

		go router.Api(g)
		go router.Actions(app)

		app.WithHttpServer(g)
		app.Start()
	},
}
