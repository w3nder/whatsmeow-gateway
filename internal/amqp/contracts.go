package amqp

type MediaPayload struct {
	URL             string `json:"url"`
	Mime            string `json:"mime"`
	Filename        string `json:"filename,omitempty"`
	Voice           bool   `json:"voice,omitempty"`
	Waveform        string `json:"waveform,omitempty"`
	DurationSeconds uint32 `json:"durationSeconds,omitempty"`
}

type LocationPayload struct {
	Lat     float64 `json:"lat"`
	Lng     float64 `json:"lng"`
	Name    string  `json:"name,omitempty"`
	Address string  `json:"address,omitempty"`
}

type ContactPayload struct {
	Name  string `json:"name"`
	Vcard string `json:"vcard"`
}

type ReplyToPayload struct {
	ProviderMessageID string `json:"providerMessageId"`
	ParticipantJID    string `json:"participantJid,omitempty"`
}

type InteractiveButton struct {
	ID   string `json:"id,omitempty"`
	Text string `json:"text"`
	URL  string `json:"url,omitempty"`
}

type InteractiveRow struct {
	ID          string `json:"id,omitempty"`
	Title       string `json:"title"`
	Description string `json:"description,omitempty"`
}

type InteractiveSection struct {
	Title string           `json:"title,omitempty"`
	Rows  []InteractiveRow `json:"rows"`
}

type InteractivePayload struct {
	Body       string               `json:"body"`
	Footer     string               `json:"footer,omitempty"`
	Buttons    []InteractiveButton  `json:"buttons,omitempty"`
	ButtonText string               `json:"buttonText,omitempty"`
	Sections   []InteractiveSection `json:"sections,omitempty"`
}

type GatewaySendCommand struct {
	TenantID                string              `json:"tenantId"`
	ChannelID               string              `json:"channelId"`
	MessageID               string              `json:"messageId"`
	To                      string              `json:"to"`
	Type                    string              `json:"type"`
	Kind                    string              `json:"kind,omitempty"`
	TargetProviderMessageID string              `json:"targetProviderMessageId,omitempty"`
	TargetFromMe            bool                `json:"targetFromMe,omitempty"`
	TargetParticipantJID    string              `json:"targetParticipantJid,omitempty"`
	Emoji                   string              `json:"emoji,omitempty"`
	Forwarded               bool                `json:"forwarded,omitempty"`
	Text                    string              `json:"text,omitempty"`
	Media                   *MediaPayload       `json:"media,omitempty"`
	Location                *LocationPayload    `json:"location,omitempty"`
	Contacts                []ContactPayload    `json:"contacts,omitempty"`
	ReplyTo                 *ReplyToPayload     `json:"replyTo,omitempty"`
	Interactive             *InteractivePayload `json:"interactive,omitempty"`
}

type GatewayCallCommand struct {
	TenantID    string   `json:"tenantId"`
	ChannelID   string   `json:"channelId"`
	CommandID   string   `json:"commandId"`
	CallID      string   `json:"callId,omitempty"`
	Action      string   `json:"action"`
	To          string   `json:"to,omitempty"`
	Targets     []string `json:"targets,omitempty"`
	GroupID     string   `json:"groupId,omitempty"`
	Video       bool     `json:"video,omitempty"`
	MediaURL    string   `json:"mediaUrl,omitempty"`
	Emoji       string   `json:"emoji,omitempty"`
	Orientation int      `json:"orientation,omitempty"`
	Enabled     bool     `json:"enabled,omitempty"`
	Raised      bool     `json:"raised,omitempty"`
	Participant string   `json:"participant,omitempty"`
	LinkToken   string   `json:"linkToken,omitempty"`
	Record      *bool    `json:"record,omitempty"`
}

type PairCommand struct {
	TenantID  string `json:"tenantId"`
	ChannelID string `json:"channelId"`
	UserID    string `json:"userId"`
}

type GroupActionParams struct {
	Phones      []string `json:"phones,omitempty"`
	Name        string   `json:"name,omitempty"`
	Description string   `json:"description,omitempty"`
	PhotoURL    string   `json:"photoUrl,omitempty"`
}

type GatewayGroupCommand struct {
	CommandID string            `json:"commandId"`
	TenantID  string            `json:"tenantId"`
	ChannelID string            `json:"channelId"`
	Action    string            `json:"action"`
	GroupJIDs []string          `json:"groupJids"`
	Params    GroupActionParams `json:"params"`
}

type GroupActionEvent struct {
	TenantID  string `json:"tenantId"`
	ChannelID string `json:"channelId"`
	CommandID string `json:"commandId"`
	GroupJID  string `json:"groupJid"`
	Action    string `json:"action"`
	OK        bool   `json:"ok"`
	Error     string `json:"error,omitempty"`
	Removed   *int   `json:"removed,omitempty"`
}

type GroupParticipant struct {
	JID   string `json:"jid"`
	LID   string `json:"lid,omitempty"`
	Phone string `json:"phone,omitempty"`
}

type GroupParticipantsEvent struct {
	TenantID     string             `json:"tenantId"`
	ChannelID    string             `json:"channelId"`
	GroupJID     string             `json:"groupJid"`
	Type         string             `json:"type"`
	Participants []GroupParticipant `json:"participants"`
	EventID      string             `json:"eventId"`
	OccurredAt   string             `json:"occurredAt"`
}
