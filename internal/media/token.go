package media

import (
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

// CallClaims identifies the operator and call a media-stream request is
// scoped to.
type CallClaims struct {
	TenantID  string
	ChannelID string
	CallID    string
	UserID    string
}

// VerifyCallToken checks an HS256 token minted by the API and returns its
// claims. The token authorises an operator to open a single call's media
// stream, so every claim it carries must be trustworthy: forged or expired
// tokens must fail here, not deeper in the media pipeline.
func VerifyCallToken(token, secret string) (CallClaims, error) {
	parsed, err := jwt.Parse(token, func(*jwt.Token) (interface{}, error) {
		return []byte(secret), nil
	},
		// Without pinning the method, a token declaring alg=none (no
		// signature) or an asymmetric alg (verified against secret as a
		// public key) would both be accepted -- the classic JWT bypass.
		jwt.WithValidMethods([]string{"HS256"}),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return CallClaims{}, fmt.Errorf("media: verify call token: %w", err)
	}

	claims, ok := parsed.Claims.(jwt.MapClaims)
	if !ok || !parsed.Valid {
		return CallClaims{}, fmt.Errorf("media: verify call token: invalid claims")
	}

	out := CallClaims{}
	for claim, dst := range map[string]*string{
		"tenantId":  &out.TenantID,
		"channelId": &out.ChannelID,
		"callId":    &out.CallID,
		"userId":    &out.UserID,
	} {
		v, ok := claims[claim].(string)
		if !ok || v == "" {
			return CallClaims{}, fmt.Errorf("media: verify call token: missing or empty claim %q", claim)
		}
		*dst = v
	}

	return out, nil
}
