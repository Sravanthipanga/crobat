// Package jwtauth verifies tenant JWTs (HS256) for the REST APIs. The
// tenant id travels in the `tenant_id` claim; every handler scopes its
// queries to that tenant, so a token can never reach another tenant's
// tasks or jobs.
package jwtauth

import (
	"errors"
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

// Claims is the Crobat tenant token payload.
type Claims struct {
	TenantID string `json:"tenant_id"`
	jwt.RegisteredClaims
}

// Verifier checks bearer tokens against a shared secret.
type Verifier struct {
	secret []byte
}

// NewVerifier builds a verifier from the CROBAT_JWT_SECRET value.
func NewVerifier(secret string) *Verifier { return &Verifier{secret: []byte(secret)} }

// Verify parses and validates a token, returning the tenant id.
func (v *Verifier) Verify(token string) (string, error) {
	if len(v.secret) == 0 {
		return "", errors.New("jwt secret not configured")
	}
	claims := &Claims{}
	parsed, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method %v", t.Header["alg"])
		}
		return v.secret, nil
	})
	if err != nil {
		return "", err
	}
	if !parsed.Valid {
		return "", errors.New("invalid token")
	}
	if claims.TenantID == "" {
		return "", errors.New("token has no tenant_id claim")
	}
	return claims.TenantID, nil
}

// Sign mints a token for a tenant (used by tests and local tooling; in
// production tokens are issued by the tenant's own identity provider or
// an admin CLI using the same secret).
func (v *Verifier) Sign(tenantID string) (string, error) {
	claims := Claims{TenantID: tenantID}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(v.secret)
}
