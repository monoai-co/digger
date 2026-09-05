package middleware

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/services"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestJWTActorRequiresVerifiedSignatureAndUsesAuthenticatedToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	database, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "actor.sqlite")), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, database.AutoMigrate(&models.Organisation{}, &models.Token{}))
	previous := models.DB
	models.DB = &models.Database{GormDB: database}
	t.Cleanup(func() {
		models.DB = previous
		sqlDB, err := database.DB()
		require.NoError(t, err)
		require.NoError(t, sqlDB.Close())
	})
	org := models.Organisation{Name: "actor", ExternalId: "actor-tenant", ExternalSource: "test"}
	require.NoError(t, database.Create(&org).Error)
	apiToken := models.Token{Value: "t:actor", OrganisationID: org.ID, Type: models.AdminPolicyType}
	require.NoError(t, database.Create(&apiToken).Error)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	publicDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)
	t.Setenv("JWT_PUBLIC_KEY", string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER})))
	claims := jwt.MapClaims{"sub": "operator", "tenantId": "actor-tenant", "type": "user", "permissions": []string{"digger.all.*"}, "exp": time.Now().Add(time.Hour).Unix()}
	valid, err := jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(key)
	require.NoError(t, err)
	wrongKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	forged, err := jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(wrongKey)
	require.NoError(t, err)
	claims["sub"] = " operator "
	paddedSubject, err := jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(key)
	require.NoError(t, err)
	for _, tc := range []struct {
		token, actor string
		code         int
	}{
		{valid, "jwt-sub:operator", http.StatusOK},
		{paddedSubject, "", http.StatusOK},
		{apiToken.Value, fmt.Sprintf("api-token:%d", apiToken.ID), http.StatusOK},
		{forged, "", http.StatusForbidden},
		{"eyJhbGciOiJub25lIn0.eyJzdWIiOiJhdHRhY2tlciJ9.", "", http.StatusForbidden},
	} {
		router := gin.New()
		router.Use(JWTBearerTokenAuth(services.Auth{}), AccessLevel(models.AdminPolicyType))
		router.GET("/", func(c *gin.Context) {
			require.Equal(t, org.ID, c.GetUint(ORGANISATION_ID_KEY))
			c.String(http.StatusOK, c.GetString(AUTHENTICATED_ACTOR_KEY))
		})
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+tc.token)
		req.Header.Set("DIGGER_USER_ID", "spoofed-actor")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		require.Equal(t, tc.code, response.Code)
		if tc.code == http.StatusOK {
			require.Equal(t, tc.actor, response.Body.String())
		}
	}
}

func TestEmptyStaticTokenDoesNotAuthenticateActor(t *testing.T) {
	t.Setenv("BEARER_AUTH_TOKEN", "")
	router := gin.New()
	router.Use(HttpBasicApiAuth())
	router.GET("/", func(c *gin.Context) { t.Fatal("empty static credential must not reach handler") })
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer ")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, req)
	require.Equal(t, http.StatusForbidden, response.Code)
}
