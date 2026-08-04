// Package call turns the WhatsApp calling stack into gateway commands and events.
//
// The gateway does not carry live call media to a human: it signals and records.
// A call's audio and video are captured to S3 and delivered by key on the ended
// event, exactly the way inbound message media already is, and every other
// lifecycle transition is published as an event for the backend to act on.
package call

// Phase is a call's lifecycle phase, mirroring the calling library's own phases.
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

// String renders the phase for the wire. An unrecognized phase renders as
// "unknown" rather than panicking: the library may gain phases we do not know
// about yet, and a call event is not worth crashing a channel over.
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

// Direction of a call, from the gateway's point of view.
const (
	DirectionInbound  = "inbound"
	DirectionOutbound = "outbound"
)

// Event types published on the call routing key.
const (
	EventIncoming      = "incoming"
	EventRinging       = "ringing"
	EventAccepted      = "accepted"
	EventRejected      = "rejected"
	EventEnded         = "ended"
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

// Error codes carried in a command.failed event.
const (
	CodeCallNotFound          = "call_not_found"
	CodeUnknownAction         = "unknown_action"
	CodeInvalidTarget         = "invalid_target"
	CodeActionFailed          = "action_failed"
	CodeNoCaller              = "no_caller"
	CodeMediaFetch            = "media_fetch_failed"
	CodeRecordingUploadFailed = "recording_upload_failed"
)

// VideoState is the peer's video state from a mid-call video stanza.
type VideoState struct {
	// Active reports the peer's camera is on.
	Active bool
	// Upgrade reports a mid-call audio-to-video upgrade.
	Upgrade bool
	// Orientation is the peer's device orientation (0..3); rotate the rendered
	// video by Orientation quarter turns to display it upright.
	Orientation int
	// Raw is the unmapped state attribute, kept for diagnosing states we do not
	// map yet.
	Raw int
}

// Reaction is a transient emoji reaction sent over a call's app-data stream.
type Reaction struct {
	Emoji         string
	ParticipantID string
	Sender        string
	Removed       bool
}

// GroupParticipant is one participant in a group call's roster.
type GroupParticipant struct {
	JID        string `json:"jid"`
	PN         string `json:"pn,omitempty"`
	State      string `json:"state,omitempty"`
	HandRaised bool   `json:"handRaised,omitempty"`
}

// GroupState is a group call's roster at one transaction.
type GroupState struct {
	TransactionID uint32             `json:"transactionId"`
	Participants  []GroupParticipant `json:"participants,omitempty"`
}

// WaitingRoomUser is one user pending admission to a call link.
type WaitingRoomUser struct {
	JID   string `json:"jid"`
	PN    string `json:"pn,omitempty"`
	State string `json:"state,omitempty"`
}

// WaitingRoom is a call link's admission state. It never carries the link token.
type WaitingRoom struct {
	Enabled       bool              `json:"enabled"`
	IsAdmin       bool              `json:"isAdmin,omitempty"`
	InWaitingRoom bool              `json:"inWaitingRoom,omitempty"`
	TransactionID uint32            `json:"transactionId,omitempty"`
	Users         []WaitingRoomUser `json:"users,omitempty"`
}

// HandState is one participant's raised/lowered hand.
type HandState struct {
	Participant string `json:"participant"`
	Raised      bool   `json:"raised"`
}

// ScreenShare is one participant's screen-share state.
type ScreenShare struct {
	Participant string `json:"participant"`
	Active      bool   `json:"active"`
	Version     uint32 `json:"version,omitempty"`
}

// Media points at a recording in the object store. It carries the key, never a
// resolved URL: the backend resolves it exactly the way it already resolves
// inbound message media.
type Media struct {
	Key      string `json:"key"`
	MimeType string `json:"mimeType"`
	Filename string `json:"filename,omitempty"`
	Duration int    `json:"duration,omitempty"`
}

// EventVideoState is the video state as published.
type EventVideoState struct {
	Active      bool `json:"active"`
	Upgrade     bool `json:"upgrade,omitempty"`
	Orientation int  `json:"orientation,omitempty"`
}

// EventReaction is a reaction as published.
type EventReaction struct {
	Emoji   string `json:"emoji"`
	Sender  string `json:"sender,omitempty"`
	Removed bool   `json:"removed,omitempty"`
}

// EventLink is a call link as published. Token is the link's public token.
type EventLink struct {
	Token            string `json:"token"`
	URL              string `json:"url,omitempty"`
	Video            bool   `json:"video,omitempty"`
	ApprovalRequired bool   `json:"approvalRequired,omitempty"`
	IsAdmin          bool   `json:"isAdmin,omitempty"`
}

// EventError explains a command.failed.
type EventError struct {
	Code   string `json:"code"`
	Reason string `json:"reason,omitempty"`
}

// Event is one call lifecycle event published to the backend. Every optional
// field is omitted when absent so a consumer can tell "no recording" from
// "empty recording".
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
