package call

type Phase int

const (
	PhaseIdle Phase = iota
	PhaseCalling
	PhaseRinging
	PhaseConnecting
	PhaseActive
	PhaseEnded
	PhaseWaitingRoom
)

func (p Phase) String() string {
	switch p {
	case PhaseIdle:
		return "idle"
	case PhaseCalling:
		return "calling"
	case PhaseRinging:
		return "ringing"
	case PhaseConnecting:
		return "connecting"
	case PhaseActive:
		return "active"
	case PhaseEnded:
		return "ended"
	case PhaseWaitingRoom:
		return "waiting_room"
	default:
		return "unknown"
	}
}

const (
	DirectionInbound  = "inbound"
	DirectionOutbound = "outbound"
)

const (
	EventIncoming      = "incoming"
	EventRinging       = "ringing"
	EventAccepted      = "accepted"
	EventRejected      = "rejected"
	EventEnded         = "ended"
	EventRecording     = "recording"
	EventState         = "state"
	EventVideo         = "video.state"
	EventReactionType  = "reaction"
	EventGroupState    = "group.state"
	EventWaitingRoom   = "waitingroom.state"
	EventHand          = "hand"
	EventScreenShare   = "screenshare"
	EventLinkCreated   = "link.created"
	EventLinkPreview   = "link.preview"
	EventCommandAck    = "command.ack"
	EventCommandFailed = "command.failed"
)

const (
	CodeCallNotFound          = "call_not_found"
	CodeUnknownAction         = "unknown_action"
	CodeInvalidTarget         = "invalid_target"
	CodeActionFailed          = "action_failed"
	CodeNoCaller              = "no_caller"
	CodeMediaFetch            = "media_fetch_failed"
	CodeRecordingUploadFailed = "recording_upload_failed"
)

type VideoState struct {
	Active      bool
	Upgrade     bool
	Orientation int
	Raw         int
}

type Reaction struct {
	Emoji         string
	ParticipantID string
	Sender        string
	Removed       bool
}

type GroupParticipant struct {
	JID        string `json:"jid"`
	PN         string `json:"pn,omitempty"`
	State      string `json:"state,omitempty"`
	HandRaised bool   `json:"handRaised,omitempty"`
}

type GroupState struct {
	TransactionID uint32             `json:"transactionId"`
	Participants  []GroupParticipant `json:"participants,omitempty"`
}

type WaitingRoomUser struct {
	JID   string `json:"jid"`
	PN    string `json:"pn,omitempty"`
	State string `json:"state,omitempty"`
}

type WaitingRoom struct {
	Enabled       bool              `json:"enabled"`
	IsAdmin       bool              `json:"isAdmin,omitempty"`
	InWaitingRoom bool              `json:"inWaitingRoom,omitempty"`
	TransactionID uint32            `json:"transactionId,omitempty"`
	Users         []WaitingRoomUser `json:"users,omitempty"`
}

type HandState struct {
	Participant string `json:"participant"`
	Raised      bool   `json:"raised"`
}

type ScreenShare struct {
	Participant string `json:"participant"`
	Active      bool   `json:"active"`
	Version     uint32 `json:"version,omitempty"`
}

type Media struct {
	Key      string `json:"key"`
	MimeType string `json:"mimeType"`
	Filename string `json:"filename,omitempty"`
	Duration int    `json:"duration,omitempty"`
}

type EventVideoState struct {
	Active      bool `json:"active"`
	Upgrade     bool `json:"upgrade,omitempty"`
	Orientation int  `json:"orientation,omitempty"`
}

type EventReaction struct {
	Emoji   string `json:"emoji"`
	Sender  string `json:"sender,omitempty"`
	Removed bool   `json:"removed,omitempty"`
}

type EventLink struct {
	Token            string `json:"token"`
	URL              string `json:"url,omitempty"`
	Video            bool   `json:"video,omitempty"`
	ApprovalRequired bool   `json:"approvalRequired,omitempty"`
	IsAdmin          bool   `json:"isAdmin,omitempty"`
}

type EventError struct {
	Code   string `json:"code"`
	Reason string `json:"reason,omitempty"`
}

type Event struct {
	PhoneNumberID string           `json:"phoneNumberId"`
	TenantID      string           `json:"tenantId"`
	ChannelID     string           `json:"channelId"`
	CallID        string           `json:"callId"`
	CommandID     string           `json:"commandId,omitempty"`
	From          string           `json:"from,omitempty"`
	SenderLid     string           `json:"senderLid,omitempty"`
	SenderPn      string           `json:"senderPn,omitempty"`
	Direction     string           `json:"direction"`
	Type          string           `json:"type"`
	Timestamp     string           `json:"timestamp"`
	State         string           `json:"state,omitempty"`
	IsVideo       bool             `json:"isVideo,omitempty"`
	Duration      int              `json:"duration,omitempty"`
	Reason        string           `json:"reason,omitempty"`
	Muted         *bool            `json:"muted,omitempty"`
	Media         *Media           `json:"media,omitempty"`
	PeerMedia     *Media           `json:"peerMedia,omitempty"`
	OperatorMedia *Media           `json:"operatorMedia,omitempty"`
	VideoMedia    *Media           `json:"videoMedia,omitempty"`
	Video         *EventVideoState `json:"video,omitempty"`
	Reaction      *EventReaction   `json:"reaction,omitempty"`
	Group         *GroupState      `json:"group,omitempty"`
	WaitingRoom   *WaitingRoom     `json:"waitingRoom,omitempty"`
	Hand          *HandState       `json:"hand,omitempty"`
	ScreenShare   *ScreenShare     `json:"screenShare,omitempty"`
	Link          *EventLink       `json:"link,omitempty"`
	Error         *EventError      `json:"error,omitempty"`
}
