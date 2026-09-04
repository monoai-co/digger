package controllers

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const githubOIDCIssuer = "https://token.actions.githubusercontent.com"

var (
	errExecutionIdentity            = errors.New("invalid GitHub execution identity")
	errExecutionIdentityUnavailable = errors.New("GitHub execution identity verification unavailable")
	immutableActionRef              = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*@[0-9a-f]{40}$`)
	cliDigest                       = regexp.MustCompile(`^[0-9a-f]{64}$`)
	workflowDigest                  = regexp.MustCompile(`^[0-9a-f]{40}$`)
)

type VerifiedGithubExecutionIdentity struct {
	Issuer, Audience, Subject           string
	RepositoryFullName                  string
	RepositoryID, RunID, RunAttempt     int64
	WorkflowRef, WorkflowSHA, EventName string
}

type ExecutionIdentityVerifier interface {
	Verify(context.Context, string, string) (*VerifiedGithubExecutionIdentity, error)
}

type githubExecutionClaims struct {
	jwt.RegisteredClaims
	Repository   string `json:"repository"`
	RepositoryID string `json:"repository_id"`
	RunID        string `json:"run_id"`
	RunAttempt   string `json:"run_attempt"`
	WorkflowRef  string `json:"workflow_ref"`
	WorkflowSHA  string `json:"workflow_sha"`
	EventName    string `json:"event_name"`
}

type githubExecutionIdentityVerifier struct {
	client    *http.Client
	jwksURL   string
	mu        sync.Mutex
	keys      map[string]*rsa.PublicKey
	expiresAt time.Time
	lastFetch time.Time
}

func NewGithubExecutionIdentityVerifier() ExecutionIdentityVerifier {
	return &githubExecutionIdentityVerifier{
		client:  &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		jwksURL: githubOIDCIssuer + "/.well-known/jwks",
	}
}

func (v *githubExecutionIdentityVerifier) Verify(ctx context.Context, rawToken, audience string) (*VerifiedGithubExecutionIdentity, error) {
	if len(rawToken) == 0 || len(rawToken) > 32768 || audience == "" {
		return nil, errExecutionIdentity
	}
	claims := &githubExecutionClaims{}
	_, err := jwt.ParseWithClaims(rawToken, claims, func(token *jwt.Token) (interface{}, error) {
		kid, ok := token.Header["kid"].(string)
		if !ok || kid == "" || len(kid) > 256 {
			return nil, errExecutionIdentity
		}
		return v.signingKey(ctx, kid)
	}, jwt.WithValidMethods([]string{"RS256"}), jwt.WithIssuer(githubOIDCIssuer), jwt.WithAudience(audience), jwt.WithExpirationRequired(), jwt.WithIssuedAt(), jwt.WithLeeway(15*time.Second))
	if err != nil {
		if errors.Is(err, errExecutionIdentityUnavailable) {
			return nil, errExecutionIdentityUnavailable
		}
		return nil, errExecutionIdentity
	}
	repositoryID, repositoryErr := strconv.ParseInt(claims.RepositoryID, 10, 64)
	runID, runErr := strconv.ParseInt(claims.RunID, 10, 64)
	runAttempt, attemptErr := strconv.ParseInt(claims.RunAttempt, 10, 64)
	if repositoryErr != nil || runErr != nil || attemptErr != nil || repositoryID <= 0 || runID <= 0 || runAttempt <= 0 ||
		claims.IssuedAt == nil || claims.NotBefore == nil || claims.ID == "" || len(claims.Subject) == 0 || len(claims.Subject) > 1024 ||
		len(claims.Audience) != 1 || claims.EventName != "workflow_dispatch" ||
		len(strings.Split(claims.Repository, "/")) != 2 || len(claims.Repository) > 256 ||
		!strings.HasPrefix(claims.WorkflowRef, claims.Repository+"/.github/workflows/") || len(claims.WorkflowRef) > 1024 ||
		!workflowDigest.MatchString(claims.WorkflowSHA) || !claims.ExpiresAt.After(claims.IssuedAt.Time) {
		return nil, errExecutionIdentity
	}
	return &VerifiedGithubExecutionIdentity{
		Issuer: claims.Issuer, Audience: audience, Subject: claims.Subject,
		RepositoryFullName: claims.Repository, RepositoryID: repositoryID, RunID: runID, RunAttempt: runAttempt,
		WorkflowRef: claims.WorkflowRef, WorkflowSHA: claims.WorkflowSHA, EventName: claims.EventName,
	}, nil
}

// The endpoint is fixed by the server, never by JWT headers. A short refresh
// interval permits key rotation while bounding requests for unknown key IDs.
func (v *githubExecutionIdentityVerifier) signingKey(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	now := time.Now()
	if key := v.keys[kid]; key != nil && now.Before(v.expiresAt) {
		return key, nil
	}
	if now.Sub(v.lastFetch) < time.Second {
		return nil, errExecutionIdentityUnavailable
	}
	v.lastFetch = now
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.jwksURL, nil)
	if err != nil {
		return nil, errExecutionIdentityUnavailable
	}
	response, err := v.client.Do(req)
	if err != nil {
		return nil, errExecutionIdentityUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, errExecutionIdentityUnavailable
	}
	var document struct {
		Keys []struct {
			Kty string `json:"kty"`
			Kid string `json:"kid"`
			Use string `json:"use"`
			Alg string `json:"alg"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 256*1024))
	if err := decoder.Decode(&document); err != nil {
		return nil, errExecutionIdentityUnavailable
	}
	if err := decoder.Decode(new(interface{})); err != io.EOF {
		return nil, errExecutionIdentityUnavailable
	}
	keys := make(map[string]*rsa.PublicKey)
	for _, key := range document.Keys {
		if key.Kty != "RSA" || key.Kid == "" || (key.Use != "" && key.Use != "sig") || (key.Alg != "" && key.Alg != "RS256") {
			continue
		}
		modulus, nErr := base64.RawURLEncoding.DecodeString(key.N)
		exponent, eErr := base64.RawURLEncoding.DecodeString(key.E)
		if nErr != nil || eErr != nil || len(modulus) < 256 || len(modulus) > 1024 || len(exponent) == 0 || len(exponent) > 4 {
			continue
		}
		n := new(big.Int).SetBytes(modulus)
		e := new(big.Int).SetBytes(exponent).Int64()
		if n.BitLen() < 2048 || e < 3 || e > 2147483647 || e%2 == 0 {
			continue
		}
		if keys[key.Kid] != nil {
			return nil, errExecutionIdentityUnavailable
		}
		keys[key.Kid] = &rsa.PublicKey{N: n, E: int(e)}
	}
	if len(keys) == 0 {
		return nil, errExecutionIdentityUnavailable
	}
	v.keys, v.expiresAt = keys, now.Add(5*time.Minute)
	if key := keys[kid]; key != nil {
		return key, nil
	}
	return nil, errExecutionIdentity
}
