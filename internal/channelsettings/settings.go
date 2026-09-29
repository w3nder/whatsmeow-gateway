package channelsettings

type Settings struct {
	ListenGroups      bool
	ReceiveCalls      bool
	CallRejectMessage string
}

func Defaults() Settings {
	return Settings{ListenGroups: true, ReceiveCalls: true}
}
