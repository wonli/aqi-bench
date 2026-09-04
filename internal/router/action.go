package router

import (
	"github.com/wonli/aqi"
	"github.com/wonli/aqi/ws"

	"aqi-bench/internal/middlewares"
)

func Actions(e *aqi.AppConfig) {
	app := ws.NewRouter().Use(middlewares.Recovery(), middlewares.App())
	app.Add("hi", func(a *ws.Context) {
		a.Send(a.Params)
	})
	app.Add("bench.echo", func(a *ws.Context) {
		a.SendCode(1001, "benchmark message")
	})
}
