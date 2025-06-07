package ui

import (
	"embed"
	"io/fs"
	"net/http"

	"github.com/gin-gonic/gin"
)

//go:embed dist/*
var embeddedFiles embed.FS

var DistFS http.FileSystem

func init() {
	sub, err := fs.Sub(embeddedFiles, "dist")
	if err != nil {
		panic(err)
	}
	DistFS = http.FS(sub)
}

func Handler() gin.HandlerFunc {
	return func(c *gin.Context) {
		http.FileServer(DistFS).ServeHTTP(c.Writer, c.Request)
	}
}
