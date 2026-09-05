package bootstrap

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func controlPlaneReadinessHandler(runtime *ControlPlaneRuntime) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		if err := runtime.Ready(ctx); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	}
}
