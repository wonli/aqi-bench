package router

import (
	"net/http/pprof"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/wonli/aqi/ws"

	"aqi-bench/internal/middlewares"
)

func Api(g *gin.Engine) {
	g.Use(middlewares.GinCORS())
	g.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"t": time.Now().Unix(),
		})
	})

	g.GET("/ws", func(c *gin.Context) {
		ws.HttpHandler(c.Writer, c.Request)
	})

	g.GET("/debug/pprof/", gin.WrapF(pprof.Index))
	g.GET("/debug/pprof/cmdline", gin.WrapF(pprof.Cmdline))
	g.GET("/debug/pprof/profile", gin.WrapF(pprof.Profile))
	g.POST("/debug/pprof/symbol", gin.WrapF(pprof.Symbol))
	g.GET("/debug/pprof/symbol", gin.WrapF(pprof.Symbol))
	g.GET("/debug/pprof/trace", gin.WrapF(pprof.Trace))
	g.GET("/debug/pprof/allocs", gin.WrapH(pprof.Handler("allocs")))
	g.GET("/debug/pprof/block", gin.WrapH(pprof.Handler("block")))
	g.GET("/debug/pprof/goroutine", gin.WrapH(pprof.Handler("goroutine")))
	g.GET("/debug/pprof/heap", gin.WrapH(pprof.Handler("heap")))
	g.GET("/debug/pprof/mutex", gin.WrapH(pprof.Handler("mutex")))
	g.GET("/debug/pprof/threadcreate", gin.WrapH(pprof.Handler("threadcreate")))
}
