package media

import (
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

type CallClaims struct {
	TenantID  string
	ChannelID string
	CallID    string
	UserID    string
}

func VerifyCallToken(token, secret string) (CallClaims, error) {
	parsed, err := jwt.Parse(token, func(*jwt.Token) (interface{}, error) {
		return []byte(secret), nil
	},
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
