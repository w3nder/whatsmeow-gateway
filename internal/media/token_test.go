package media_test

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/w3nder/whatsmeow-gateway/internal/media"
)

const testSecret = "test-secret"

func sign(t *testing.T, claims jwt.MapClaims, secret string) string {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return token
}

func validClaims() jwt.MapClaims {
	return jwt.MapClaims{
		"tenantId":  "tenant-1",
		"channelId": "channel-1",
		"callId":    "CALL1",
		"userId":    "user-1",
		"exp":       time.Now().Add(5 * time.Minute).Unix(),
	}
}

func TestVerifyCallTokenAcceptsAValidToken(t *testing.T) {
	got, err := media.VerifyCallToken(sign(t, validClaims(), testSecret), testSecret)
	if err != nil {
		t.Fatalf("VerifyCallToken: %v", err)
	}
	want := media.CallClaims{TenantID: "tenant-1", ChannelID: "channel-1", CallID: "CALL1", UserID: "user-1"}
	if got != want {
		t.Errorf("claims = %+v, want %+v", got, want)
	}
}

func TestVerifyCallTokenRejectsAWrongSecret(t *testing.T) {
	if _, err := media.VerifyCallToken(sign(t, validClaims(), "other-secret"), testSecret); err == nil {
		t.Fatal("err = nil, want a signature failure")
	}
}

func TestVerifyCallTokenRejectsAnExpiredToken(t *testing.T) {
	claims := validClaims()
	claims["exp"] = time.Now().Add(-time.Minute).Unix()
	if _, err := media.VerifyCallToken(sign(t, claims, testSecret), testSecret); err == nil {
		t.Fatal("err = nil, want an expiry failure")
	}
}

// A token without exp would never expire, which defeats the point of a short
// lived credential.
func TestVerifyCallTokenRejectsAMissingExpiry(t *testing.T) {
	claims := validClaims()
	delete(claims, "exp")
	if _, err := media.VerifyCallToken(sign(t, claims, testSecret), testSecret); err == nil {
		t.Fatal("err = nil, want a missing-expiry failure")
	}
}

// alg=none is the classic JWT bypass: a token that carries no signature at all
// must never be accepted.
func TestVerifyCallTokenRejectsAlgNone(t *testing.T) {
	unsigned, err := jwt.NewWithClaims(jwt.SigningMethodNone, validClaims()).
		SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("sign none: %v", err)
	}
	if _, err := media.VerifyCallToken(unsigned, testSecret); err == nil {
		t.Fatal("err = nil, want alg=none to be refused")
	}
}

func TestVerifyCallTokenRejectsMissingClaims(t *testing.T) {
	for _, missing := range []string{"tenantId", "channelId", "callId", "userId"} {
		claims := validClaims()
		delete(claims, missing)
		if _, err := media.VerifyCallToken(sign(t, claims, testSecret), testSecret); err == nil {
			t.Errorf("token without %q was accepted", missing)
		}
	}
}

// An empty string is technically "present" but is not a usable identifier;
// treating it as missing closes off a degenerate token that would otherwise
// carry, say, tenantId: "" past the check above.
func TestVerifyCallTokenRejectsEmptyClaims(t *testing.T) {
	for _, empty := range []string{"tenantId", "channelId", "callId", "userId"} {
		claims := validClaims()
		claims[empty] = ""
		if _, err := media.VerifyCallToken(sign(t, claims, testSecret), testSecret); err == nil {
			t.Errorf("token with empty %q was accepted", empty)
		}
	}
}

// jsonwebtoken (the API's signer) stamps iat whenever expiresIn is used, so
// every real token carries one. VerifyCallToken must ignore it rather than
// validate it -- there is no iat requirement in the contract, and adding one
// later (e.g. jwt.WithIssuedAt()) would silently start rejecting every real
// token this test guards against that regression.
func TestVerifyCallTokenAcceptsARealisticIssuedAt(t *testing.T) {
	claims := validClaims()
	claims["iat"] = time.Now().Unix()

	got, err := media.VerifyCallToken(sign(t, claims, testSecret), testSecret)
	if err != nil {
		t.Fatalf("VerifyCallToken: %v", err)
	}
	want := media.CallClaims{TenantID: "tenant-1", ChannelID: "channel-1", CallID: "CALL1", UserID: "user-1"}
	if got != want {
		t.Errorf("claims = %+v, want %+v", got, want)
	}
}
