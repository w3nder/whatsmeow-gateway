package history

import (
	"time"

	"github.com/w3nder/whatsmeow-gateway/internal/deviceprops"
)

type Limits struct {
	Window time.Duration
}

func DefaultLimits() Limits {
	return Limits{
		Window: deviceprops.WindowDays * 24 * time.Hour,
	}
}
