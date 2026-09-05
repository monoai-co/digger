package bootstrap

import (
	"context"
	"net/http"
	"time"

	"github.com/diggerhq/digger/backend/controllers"
	"github.com/gin-gonic/gin"
)

func controlPlaneReadinessHandler(controller controllers.DiggerController, processor *controllers.GithubWebhookProcessor) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		if processor.Enabled() || controller.ExecutionGrantSigningKeyID != "" {
			if err := controller.ExecutionClaimsReady(ctx); err != nil {
				c.JSON(http.StatusServiceUnavailable, gin.H{"status": "execution_keys_not_ready"})
				return
			}
		}
		if err := processor.Ready(ctx); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	}
}
