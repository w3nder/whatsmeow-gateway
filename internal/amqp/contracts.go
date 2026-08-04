package amqp

type MediaPayload struct {
	URL      string `json:"url"`
	Mime     string `json:"mime"`
	Filename string `json:"filename,omitempty"`
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

type GatewaySendCommand struct {
	TenantID                string           `json:"tenantId"`
	ChannelID               string           `json:"channelId"`
	MessageID               string           `json:"messageId"`
	To                      string           `json:"to"`
	Type                    string           `json:"type"`
	Kind                    string           `json:"kind,omitempty"`
	TargetProviderMessageID string           `json:"targetProviderMessageId,omitempty"`
	TargetFromMe            bool             `json:"targetFromMe,omitempty"`
	Emoji                   string           `json:"emoji,omitempty"`
	Forwarded               bool             `json:"forwarded,omitempty"`
	Text                    string           `json:"text,omitempty"`
	Media                   *MediaPayload    `json:"media,omitempty"`
	Location                *LocationPayload `json:"location,omitempty"`
	Contacts                []ContactPayload `json:"contacts,omitempty"`
	ReplyTo                 *ReplyToPayload  `json:"replyTo,omitempty"`
}

// GatewayCallCommand drives one action on the calling stack. Unlike a send
// command it is imperative rather than idempotent -- replaying a hangup is not
// the same as replaying a message -- so it never goes through the dedupe store.
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
	// Record turns recording off for one call when explicitly false; nil keeps
	// the gateway-wide default.
	Record *bool `json:"record,omitempty"`
}

type PairCommand struct {
	TenantID  string `json:"tenantId"`
	ChannelID string `json:"channelId"`
	UserID    string `json:"userId"`
}
