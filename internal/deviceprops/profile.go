package deviceprops

import "go.mau.fi/whatsmeow/proto/waCompanionReg"

const WindowDays = 90

const (
	HistoryPlatform        = waCompanionReg.DeviceProps_DESKTOP
	HistoryRequireFullSync = true
	HistorySizeMbLimit     = 2048
)
