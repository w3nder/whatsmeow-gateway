package channelsettings

type Settings struct {
	ListenGroups      bool
	ReceiveCalls      bool
	CallRejectMessage string
	Version           int64
}

func Defaults() Settings {
	return Settings{ListenGroups: true, ReceiveCalls: true}
}
