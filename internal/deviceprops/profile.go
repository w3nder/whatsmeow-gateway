package deviceprops

import (
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/proto/waWa6"
	"go.mau.fi/whatsmeow/store"
	"google.golang.org/protobuf/proto"
)

const WindowDays = 90

const (
	HistoryPlatform        = waCompanionReg.DeviceProps_DESKTOP
	HistoryRequireFullSync = true
	HistorySizeMbLimit     = 2048
)

func History() *waCompanionReg.DeviceProps {
	props := proto.Clone(store.DeviceProps).(*waCompanionReg.DeviceProps)
	props.PlatformType = HistoryPlatform.Enum()
	props.RequireFullSync = proto.Bool(HistoryRequireFullSync)
	props.HistorySyncConfig.FullSyncDaysLimit = proto.Uint32(WindowDays)
	props.HistorySyncConfig.RecentSyncDaysLimit = proto.Uint32(WindowDays)
	props.HistorySyncConfig.FullSyncSizeMbLimit = proto.Uint32(HistorySizeMbLimit)
	return props
}

func HistoryPayload(device *store.Device) func() *waWa6.ClientPayload {
	return func() *waWa6.ClientPayload {
		payload := device.GetClientPayload()
		if registration := payload.GetDevicePairingData(); registration != nil {
			registration.DeviceProps, _ = proto.Marshal(History())
		}
		return payload
	}
}
