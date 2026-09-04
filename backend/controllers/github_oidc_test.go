package controllers

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/diggerhq/digger/backend/middleware"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/libs/operation"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

func githubOIDCTestVerifier(t *testing.T) (*githubExecutionIdentityVerifier, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/.well-known/jwks", r.URL.Path)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "RSA", "kid": "test-key", "use": "sig", "alg": "RS256",
			"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
		}}}))
	}))
	t.Cleanup(server.Close)
	return &githubExecutionIdentityVerifier{client: server.Client(), jwksURL: server.URL + "/.well-known/jwks"}, key
}

func githubOIDCTestClaims(audience string) githubExecutionClaims {
	now := time.Now()
	return githubExecutionClaims{
		RegisteredClaims: jwt.RegisteredClaims{Issuer: githubOIDCIssuer, Audience: jwt.ClaimStrings{audience}, Subject: "repo:monoai-co/sre:ref:refs/heads/main", ID: "token-id", IssuedAt: jwt.NewNumericDate(now), NotBefore: jwt.NewNumericDate(now.Add(-time.Minute)), ExpiresAt: jwt.NewNumericDate(now.Add(5 * time.Minute))},
		Repository:       "monoai-co/sre", RepositoryID: "12345", RunID: "901", RunAttempt: "1",
		WorkflowRef: "monoai-co/sre/.github/workflows/digger.yml@refs/heads/main", WorkflowSHA: strings.Repeat("a", 40), EventName: "workflow_dispatch",
	}
}

func signGithubOIDCTestClaims(t *testing.T, key *rsa.PrivateKey, claims githubExecutionClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "test-key"
	raw, err := token.SignedString(key)
	require.NoError(t, err)
	return raw
}

func TestGithubOIDCRejectsInvalidTokens(t *testing.T) {
	verifier, key := githubOIDCTestVerifier(t)
	claims := githubOIDCTestClaims("exact-job")
	identity, err := verifier.Verify(context.Background(), signGithubOIDCTestClaims(t, key, claims), "exact-job")
	require.NoError(t, err)
	require.Equal(t, int64(901), identity.RunID)
	require.Equal(t, int64(12345), identity.RepositoryID)
	for name, change := range map[string]func(*githubExecutionClaims){
		"issuer":             func(c *githubExecutionClaims) { c.Issuer = "https://attacker.test" },
		"audience":           func(c *githubExecutionClaims) { c.Audience = jwt.ClaimStrings{"another-job"} },
		"multiple audiences": func(c *githubExecutionClaims) { c.Audience = jwt.ClaimStrings{"exact-job", "another-job"} },
		"expired":            func(c *githubExecutionClaims) { c.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Minute)) },
		"missing expiry":     func(c *githubExecutionClaims) { c.ExpiresAt = nil },
		"future issued":      func(c *githubExecutionClaims) { c.IssuedAt = jwt.NewNumericDate(time.Now().Add(time.Minute)) },
		"missing issued":     func(c *githubExecutionClaims) { c.IssuedAt = nil },
		"missing nbf":        func(c *githubExecutionClaims) { c.NotBefore = nil },
		"missing jti":        func(c *githubExecutionClaims) { c.ID = "" },
		"repository id":      func(c *githubExecutionClaims) { c.RepositoryID = "0" },
		"run id":             func(c *githubExecutionClaims) { c.RunID = "0" },
		"attempt":            func(c *githubExecutionClaims) { c.RunAttempt = "0" },
		"workflow repository": func(c *githubExecutionClaims) {
			c.WorkflowRef = "another/repo/.github/workflows/digger.yml@refs/heads/main"
		},
		"workflow sha": func(c *githubExecutionClaims) { c.WorkflowSHA = "main" },
		"event":        func(c *githubExecutionClaims) { c.EventName = "pull_request" },
	} {
		t.Run(name, func(t *testing.T) {
			altered := githubOIDCTestClaims("exact-job")
			change(&altered)
			_, err := verifier.Verify(context.Background(), signGithubOIDCTestClaims(t, key, altered), "exact-job")
			require.ErrorIs(t, err, errExecutionIdentity)
		})
	}
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	_, err = verifier.Verify(context.Background(), signGithubOIDCTestClaims(t, otherKey, claims), "exact-job")
	require.ErrorIs(t, err, errExecutionIdentity)
}

func TestExecutionClaimRejectsMismatchedAttestationBeforeDatabaseAccess(t *testing.T) {
	verifier, key := githubOIDCTestVerifier(t)
	controller := DiggerController{ControlPlaneDatabaseIdentity: "test-db", ControlPlaneWriterEpoch: 7,
		ExecutionGrantSigningKeyID: "key", ExecutionGrantSecrets: map[string][]byte{"key": bytes.Repeat([]byte{1}, 32)},
		ExecutionIdentityVerifier: verifier, TrustedActionRef: "monoai-co/digger@" + strings.Repeat("b", 40), TrustedCLISHA256: strings.Repeat("c", 64)}
	operationID := "op1_" + strings.Repeat("d", 64)
	audience, err := operation.ExecutionClaimAudience(operationID, "job-1")
	require.NoError(t, err)
	// Any accidental database access panics: all rejected requests must exit
	// before looking up or changing job, token, or claim records.
	previous := models.DB
	models.DB = nil
	t.Cleanup(func() { models.DB = previous })
	for name, change := range map[string]func(*claimJobExecutionRequest){
		"repository":    func(r *claimJobExecutionRequest) { r.RepositoryFullName = "other/repo" },
		"run":           func(r *claimJobExecutionRequest) { r.RunID++ },
		"attempt":       func(r *claimJobExecutionRequest) { r.RunAttempt++ },
		"workflow ref":  func(r *claimJobExecutionRequest) { r.WorkflowRef += "other" },
		"workflow sha":  func(r *claimJobExecutionRequest) { r.WorkflowSHA = strings.Repeat("e", 40) },
		"action":        func(r *claimJobExecutionRequest) { r.ActionRef = "monoai-co/digger@main" },
		"cli":           func(r *claimJobExecutionRequest) { r.CLISHA256 = strings.Repeat("f", 64) },
		"protocol":      func(r *claimJobExecutionRequest) { r.ProtocolVersion = 1 },
		"missing token": func(r *claimJobExecutionRequest) { r.OIDCToken = "" },
		"other audience": func(r *claimJobExecutionRequest) {
			r.OIDCToken = signGithubOIDCTestClaims(t, key, githubOIDCTestClaims("other"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			claims := githubOIDCTestClaims(audience)
			request := claimJobExecutionRequest{RepositoryFullName: claims.Repository, ProjectName: "root", OperationID: operationID,
				RunID: 901, RunAttempt: 1, WorkflowRef: claims.WorkflowRef, WorkflowSHA: claims.WorkflowSHA,
				ActionRef: controller.TrustedActionRef, CLISHA256: controller.TrustedCLISHA256, ProtocolVersion: 2, DispatchWriterEpoch: 7,
				OIDCToken: signGithubOIDCTestClaims(t, key, claims)}
			change(&request)
			body, err := json.Marshal(request)
			require.NoError(t, err)
			response := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(response)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/jobs/job-1/execution-claims", bytes.NewReader(body))
			c.Params = gin.Params{{Key: "jobId", Value: "job-1"}}
			c.Set(middleware.JOB_TOKEN_KEY, "exact-job-token")
			controller.ClaimJobExecution(c)
			require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
			require.NotContains(t, response.Body.String(), request.OIDCToken+".")
		})
	}
}

func TestGithubOIDCRefreshesRotatedKeysAndFailsClosedOnUnavailableKeys(t *testing.T) {
	verifier, key := githubOIDCTestVerifier(t)
	claims := githubOIDCTestClaims("exact-job")
	raw := signGithubOIDCTestClaims(t, key, claims)
	_, err := verifier.Verify(context.Background(), raw, "exact-job")
	require.NoError(t, err)
	// Expiring the cache must fetch the new keyset rather than using stale keys.
	verifier.expiresAt = time.Now().Add(-time.Minute)
	verifier.lastFetch = time.Now().Add(-time.Minute)
	verifier.keys = map[string]*rsa.PublicKey{"old-key": &key.PublicKey}
	_, err = verifier.Verify(context.Background(), raw, "exact-job")
	require.NoError(t, err)
	require.Contains(t, verifier.keys, "test-key")
	require.NotContains(t, verifier.keys, "old-key")
	verifier.expiresAt = time.Now().Add(-time.Minute)
	verifier.lastFetch = time.Now().Add(-time.Minute)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	t.Cleanup(server.Close)
	verifier.jwksURL = server.URL
	_, err = verifier.Verify(context.Background(), raw, "exact-job")
	require.ErrorIs(t, err, errExecutionIdentityUnavailable)
}
