package bootstrap

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/diggerhq/digger/backend/controllers"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestDurableReadinessRejectsMissingExecutionKeys(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		processor := controllers.NewGithubWebhookProcessor(nil, nil, controllers.GithubWebhookProcessorConfig{Enabled: enabled})
		if !enabled {
			processor.Start()
		}
		router := gin.New()
		router.GET("/ready", controlPlaneReadinessHandler(controllers.DiggerController{}, processor))
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/ready", nil))
		if enabled {
			require.Equal(t, http.StatusServiceUnavailable, response.Code)
			require.Contains(t, response.Body.String(), "execution_keys_not_ready")
		} else {
			require.Equal(t, http.StatusOK, response.Code)
		}
	}
}
