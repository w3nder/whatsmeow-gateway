package gateway

import (
	"errors"
	"fmt"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

func TestPairingFailureReasonTellsAnExpiredQRApart(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		byCode bool
		want   string
	}{
		{"qr not scanned in time", fmt.Errorf("pair: %w", session.ErrPairingTimeout), false, pairingQRTimeoutReason},
		{"code not used in time", session.ErrPairingCodeExpired, true, pairingCodeExpiredReason},
		{"invalid phone", session.ErrInvalidPhoneNumber, true, invalidPhoneNumberReason},
		{"any other qr failure", errors.New("boom"), false, pairingFailedReason},
		{"any other code failure", errors.New("boom"), true, pairingCodeRefusedReason},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := pairingFailureReason(tc.err, tc.byCode); got != tc.want {
				t.Errorf("reason = %q, want %q", got, tc.want)
			}
		})
	}
}
