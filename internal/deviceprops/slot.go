package deviceprops

import (
	"context"
	"sync"

	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/store"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/proto"
)

const pairingCapacity = 1 << 20

var pairings = semaphore.NewWeighted(pairingCapacity)

func Acquire(ctx context.Context, importHistory bool) (func(), error) {
	weight := int64(1)
	if importHistory {
		weight = pairingCapacity
	}
	if err := pairings.Acquire(ctx, weight); err != nil {
		return nil, err
	}
	previous := store.DeviceProps
	if importHistory {
		store.DeviceProps = withHistory(previous)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			if importHistory {
				store.DeviceProps = previous
			}
			pairings.Release(weight)
		})
	}, nil
}

func withHistory(base *waCompanionReg.DeviceProps) *waCompanionReg.DeviceProps {
	props := proto.Clone(base).(*waCompanionReg.DeviceProps)
	props.PlatformType = HistoryPlatform.Enum()
	props.RequireFullSync = proto.Bool(HistoryRequireFullSync)
	props.HistorySyncConfig.FullSyncDaysLimit = proto.Uint32(WindowDays)
	props.HistorySyncConfig.RecentSyncDaysLimit = proto.Uint32(WindowDays)
	props.HistorySyncConfig.FullSyncSizeMbLimit = proto.Uint32(HistorySizeMbLimit)
	return props
}
