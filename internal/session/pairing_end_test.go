package session

import (
	"errors"
	"testing"

	"go.mau.fi/whatsmeow"
)

func TestPairingEndErrorKeepsTheQRTimeoutApartFromTheCodeExpiry(t *testing.T) {
	timeout := whatsmeow.QRChannelTimeout

	if err := pairingEndError(&pairing{}, timeout); !errors.Is(err, ErrPairingTimeout) {
		t.Errorf("qr timeout = %v, want ErrPairingTimeout", err)
	}
	if err := pairingEndError(&pairing{phone: "5511987654321"}, timeout); !errors.Is(err, ErrPairingCodeExpired) {
		t.Errorf("code timeout = %v, want ErrPairingCodeExpired", err)
	}
	if err := pairingEndError(&pairing{}, whatsmeow.QRChannelErrUnexpectedEvent); errors.Is(err, ErrPairingTimeout) {
		t.Errorf("unexpected event = %v, must not read as a timeout", err)
	}
}
