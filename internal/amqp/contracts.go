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
	Forwarded               bool             `json:"forwarded,omitempty"`
	Text                    string           `json:"text,omitempty"`
	Media                   *MediaPayload    `json:"media,omitempty"`
	Location                *LocationPayload `json:"location,omitempty"`
	Contacts                []ContactPayload `json:"contacts,omitempty"`
	ReplyTo                 *ReplyToPayload  `json:"replyTo,omitempty"`
}

type PairCommand struct {
	TenantID  string `json:"tenantId"`
	ChannelID string `json:"channelId"`
	UserID    string `json:"userId"`
}
