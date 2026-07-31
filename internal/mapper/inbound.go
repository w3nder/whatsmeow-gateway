package mapper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

var ErrSkip = errors.New("mapper: inbound message has no mappable user content")

const maxUnwrapDepth = 8

type Downloader interface {
	Download(ctx context.Context, msg whatsmeow.DownloadableMessage) ([]byte, error)
}

type PNResolver interface {
	PNForLID(ctx context.Context, lid types.JID) (types.JID, bool, error)
}

type MediaStore interface {
	Put(ctx context.Context, key, mime string, data []byte) error
}

type InboundText struct {
	Body string `json:"body,omitempty"`
}

type InboundMedia struct {
	Key      string `json:"key,omitempty"`
	MimeType string `json:"mimeType,omitempty"`
	Filename string `json:"filename,omitempty"`
	Caption  string `json:"caption,omitempty"`
}

type InboundLocation struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Name      string  `json:"name,omitempty"`
	Address   string  `json:"address,omitempty"`
	Live      bool    `json:"live,omitempty"`
}

type InboundContactName struct {
	FormattedName string `json:"formatted_name,omitempty"`
	FirstName     string `json:"first_name,omitempty"`
}

type InboundContactPhone struct {
	Phone string `json:"phone,omitempty"`
	WaID  string `json:"wa_id,omitempty"`
	Type  string `json:"type,omitempty"`
}

type InboundContact struct {
	Name   *InboundContactName   `json:"name,omitempty"`
	Phones []InboundContactPhone `json:"phones,omitempty"`
	Vcard  string                `json:"vcard,omitempty"`
}

type InboundReaction struct {
	MessageID string `json:"messageId,omitempty"`
	Emoji     string `json:"emoji,omitempty"`
}

type InboundUnsupported struct {
	Type string `json:"type,omitempty"`
}

type InboundTarget struct {
	ProviderMessageID string `json:"providerMessageId"`
}

type InboundRichButton struct {
	ID   string `json:"id,omitempty"`
	Text string `json:"text"`
	Name string `json:"name,omitempty"`
	URL  string `json:"url,omitempty"`
}

type InboundRichListRow struct {
	ID          string `json:"id,omitempty"`
	Title       string `json:"title"`
	Description string `json:"description,omitempty"`
}

type InboundRichListSection struct {
	Title string               `json:"title,omitempty"`
	Rows  []InboundRichListRow `json:"rows"`
}

type InboundRichList struct {
	ButtonText string                   `json:"buttonText,omitempty"`
	Sections   []InboundRichListSection `json:"sections"`
}

type InboundRichProduct struct {
	Title       string `json:"title"`
	Description string `json:"description,omitempty"`
	PriceText   string `json:"priceText,omitempty"`
	Currency    string `json:"currency,omitempty"`
	RetailerID  string `json:"retailerId,omitempty"`
}

type InboundRichEvent struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	StartTime   string `json:"startTime,omitempty"`
	EndTime     string `json:"endTime,omitempty"`
	Location    string `json:"location,omitempty"`
	JoinLink    string `json:"joinLink,omitempty"`
	Canceled    bool   `json:"canceled,omitempty"`
}

type InboundRichContent struct {
	Kind    string              `json:"kind"`
	Flow    string              `json:"flow,omitempty"`
	Body    string              `json:"body,omitempty"`
	Footer  string              `json:"footer,omitempty"`
	Header  string              `json:"header,omitempty"`
	Buttons []InboundRichButton `json:"buttons,omitempty"`
	List    *InboundRichList    `json:"list,omitempty"`
	Product *InboundRichProduct `json:"product,omitempty"`
	Event   *InboundRichEvent   `json:"event,omitempty"`
}

type InboundEvent struct {
	PhoneNumberID     string              `json:"phoneNumberId"`
	From              string              `json:"from"`
	SenderLid         string              `json:"senderLid,omitempty"`
	SenderPn          string              `json:"senderPn,omitempty"`
	FromMe            bool                `json:"fromMe,omitempty"`
	ProfileName       string              `json:"profileName,omitempty"`
	ProviderMessageID string              `json:"providerMessageId"`
	Timestamp         string              `json:"timestamp"`
	Type              string              `json:"type"`
	Text              *InboundText        `json:"text,omitempty"`
	Media             *InboundMedia       `json:"media,omitempty"`
	Location          *InboundLocation    `json:"location,omitempty"`
	Contacts          []InboundContact    `json:"contacts,omitempty"`
	ContextMessageID  string              `json:"contextMessageId,omitempty"`
	Reaction          *InboundReaction    `json:"reaction,omitempty"`
	Unsupported       *InboundUnsupported `json:"unsupported,omitempty"`
	Target            *InboundTarget      `json:"target,omitempty"`
	RichContent       *InboundRichContent `json:"richContent,omitempty"`
}

type StatusError struct {
	Code   string `json:"code"`
	Reason string `json:"reason,omitempty"`
}

type StatusEvent struct {
	ProviderMessageID string       `json:"providerMessageId"`
	OpaqueMessageID   string       `json:"opaqueMessageId,omitempty"`
	Status            string       `json:"status"`
	Timestamp         string       `json:"timestamp"`
	Error             *StatusError `json:"error,omitempty"`
}

func BuildInbound(ctx context.Context, dl Downloader, resolver PNResolver, s3 MediaStore, channelID, tenantID string, evt *events.Message) (InboundEvent, error) {
	identityJID, identityAlt := evt.Info.Sender, evt.Info.SenderAlt
	if evt.Info.IsFromMe {
		identityJID, identityAlt = evt.Info.Chat, evt.Info.RecipientAlt
	}
	senderLid, senderPn := resolveSenderIdentifiers(ctx, resolver, identityJID, identityAlt)
	from := senderPn
	if from == "" {
		from = senderLid
	}

	out := InboundEvent{
		PhoneNumberID:     channelID,
		From:              from,
		SenderLid:         senderLid,
		SenderPn:          senderPn,
		FromMe:            evt.Info.IsFromMe,
		ProfileName:       evt.Info.PushName,
		ProviderMessageID: evt.Info.ID,
		Timestamp:         strconv.FormatInt(evt.Info.Timestamp.Unix(), 10),
	}

	msg := unwrapMessage(evt.Message)

	if pm := msg.GetProtocolMessage(); pm != nil {
		targetID := pm.GetKey().GetID()
		switch {
		case pm.GetType() == waE2E.ProtocolMessage_REVOKE && targetID != "":
			out.Type = "revoke"
			out.Target = &InboundTarget{ProviderMessageID: targetID}
			return out, nil
		case pm.GetType() == waE2E.ProtocolMessage_MESSAGE_EDIT && targetID != "":
			out.Type = "edit"
			out.Target = &InboundTarget{ProviderMessageID: targetID}
			out.Text = &InboundText{Body: extractText(unwrapMessage(pm.GetEditedMessage()))}
			return out, nil
		default:
			return InboundEvent{}, ErrSkip
		}
	}

	switch {
	case msg.GetConversation() != "":
		out.Type = "text"
		out.Text = &InboundText{Body: msg.GetConversation()}

	case msg.GetExtendedTextMessage() != nil:
		ext := msg.GetExtendedTextMessage()
		out.Type = "text"
		out.Text = &InboundText{Body: ext.GetText()}
		out.ContextMessageID = ext.GetContextInfo().GetStanzaID()

	case msg.GetReactionMessage() != nil:
		r := msg.GetReactionMessage()
		out.Type = "reaction"
		out.Reaction = &InboundReaction{MessageID: r.GetKey().GetID(), Emoji: r.GetText()}

	case msg.GetImageMessage() != nil:
		img := msg.GetImageMessage()
		media, err := downloadAndStore(ctx, dl, s3, tenantID, evt.Info.ID, img.GetMimetype(), img)
		if err != nil {
			return InboundEvent{}, err
		}
		media.Caption = img.GetCaption()
		out.Type = "image"
		out.Media = media
		out.ContextMessageID = img.GetContextInfo().GetStanzaID()

	case msg.GetVideoMessage() != nil:
		video := msg.GetVideoMessage()
		media, err := downloadAndStore(ctx, dl, s3, tenantID, evt.Info.ID, video.GetMimetype(), video)
		if err != nil {
			return InboundEvent{}, err
		}
		media.Caption = video.GetCaption()
		out.Type = "video"
		out.Media = media
		out.ContextMessageID = video.GetContextInfo().GetStanzaID()

	case msg.GetAudioMessage() != nil:
		audio := msg.GetAudioMessage()
		media, err := downloadAndStore(ctx, dl, s3, tenantID, evt.Info.ID, audio.GetMimetype(), audio)
		if err != nil {
			return InboundEvent{}, err
		}
		if audio.GetPTT() {
			out.Type = "voice"
		} else {
			out.Type = "audio"
		}
		out.Media = media
		out.ContextMessageID = audio.GetContextInfo().GetStanzaID()

	case msg.GetDocumentMessage() != nil:
		doc := msg.GetDocumentMessage()
		media, err := downloadAndStore(ctx, dl, s3, tenantID, evt.Info.ID, doc.GetMimetype(), doc)
		if err != nil {
			return InboundEvent{}, err
		}
		media.Filename = doc.GetFileName()
		media.Caption = doc.GetCaption()
		out.Type = "document"
		out.Media = media
		out.ContextMessageID = doc.GetContextInfo().GetStanzaID()

	case msg.GetLocationMessage() != nil:
		loc := msg.GetLocationMessage()
		out.Type = "location"
		out.Location = &InboundLocation{
			Latitude:  loc.GetDegreesLatitude(),
			Longitude: loc.GetDegreesLongitude(),
			Name:      loc.GetName(),
			Address:   loc.GetAddress(),
		}
		out.ContextMessageID = loc.GetContextInfo().GetStanzaID()

	case msg.GetContactMessage() != nil:
		contact := msg.GetContactMessage()
		out.Type = "contacts"
		out.Contacts = []InboundContact{contactFrom(contact.GetDisplayName(), contact.GetVcard())}
		out.ContextMessageID = contact.GetContextInfo().GetStanzaID()

	case msg.GetContactsArrayMessage() != nil:
		arr := msg.GetContactsArrayMessage()
		out.Type = "contacts"
		contacts := make([]InboundContact, 0, len(arr.GetContacts()))
		for _, c := range arr.GetContacts() {
			contacts = append(contacts, contactFrom(c.GetDisplayName(), c.GetVcard()))
		}
		out.Contacts = contacts
		out.ContextMessageID = arr.GetContextInfo().GetStanzaID()

	case msg.GetStickerMessage() != nil:
		sticker := msg.GetStickerMessage()
		media, err := downloadAndStore(ctx, dl, s3, tenantID, evt.Info.ID, sticker.GetMimetype(), sticker)
		if err != nil {
			return InboundEvent{}, err
		}
		out.Type = "sticker"
		out.Media = media
		out.ContextMessageID = sticker.GetContextInfo().GetStanzaID()

	case msg.GetPtvMessage() != nil:
		ptv := msg.GetPtvMessage()
		media, err := downloadAndStore(ctx, dl, s3, tenantID, evt.Info.ID, ptv.GetMimetype(), ptv)
		if err != nil {
			return InboundEvent{}, err
		}
		media.Caption = ptv.GetCaption()
		out.Type = "video"
		out.Media = media
		out.ContextMessageID = ptv.GetContextInfo().GetStanzaID()

	case msg.GetLiveLocationMessage() != nil:
		live := msg.GetLiveLocationMessage()
		out.Type = "location"
		out.Location = &InboundLocation{
			Latitude:  live.GetDegreesLatitude(),
			Longitude: live.GetDegreesLongitude(),
			Name:      live.GetCaption(),
			Live:      true,
		}
		out.ContextMessageID = live.GetContextInfo().GetStanzaID()

	case msg.GetButtonsResponseMessage() != nil:
		resp := msg.GetButtonsResponseMessage()
		body := resp.GetSelectedDisplayText()
		if body == "" {
			body = resp.GetSelectedButtonID()
		}
		out.Type = "text"
		out.Text = &InboundText{Body: body}
		out.ContextMessageID = resp.GetContextInfo().GetStanzaID()

	case msg.GetListResponseMessage() != nil:
		resp := msg.GetListResponseMessage()
		body := resp.GetTitle()
		if body == "" {
			body = resp.GetSingleSelectReply().GetSelectedRowID()
		}
		out.Type = "text"
		out.Text = &InboundText{Body: body}
		out.ContextMessageID = resp.GetContextInfo().GetStanzaID()

	case msg.GetTemplateButtonReplyMessage() != nil:
		resp := msg.GetTemplateButtonReplyMessage()
		body := resp.GetSelectedDisplayText()
		if body == "" {
			body = resp.GetSelectedID()
		}
		out.Type = "text"
		out.Text = &InboundText{Body: body}
		out.ContextMessageID = resp.GetContextInfo().GetStanzaID()

	case msg.GetInteractiveResponseMessage() != nil:
		resp := msg.GetInteractiveResponseMessage()
		body := resp.GetBody().GetText()
		if body == "" {
			body = resp.GetNativeFlowResponseMessage().GetParamsJSON()
		}
		if body == "" {
			body = resp.GetNativeFlowResponseMessage().GetName()
		}
		out.Type = "text"
		out.Text = &InboundText{Body: body}
		out.ContextMessageID = resp.GetContextInfo().GetStanzaID()

	case msg.GetPollCreationMessage() != nil, msg.GetPollCreationMessageV2() != nil, msg.GetPollCreationMessageV3() != nil:
		poll := msg.GetPollCreationMessage()
		if poll == nil {
			poll = msg.GetPollCreationMessageV2()
		}
		if poll == nil {
			poll = msg.GetPollCreationMessageV3()
		}
		out.Type = "text"
		out.Text = &InboundText{Body: pollSummary(poll)}
		out.ContextMessageID = poll.GetContextInfo().GetStanzaID()

	case msg.GetButtonsMessage() != nil:
		btn := msg.GetButtonsMessage()
		if err := applyRichOrFallback(&out, msg, buildButtonsRich(btn)); err != nil {
			return InboundEvent{}, err
		}
		out.ContextMessageID = btn.GetContextInfo().GetStanzaID()

	case msg.GetListMessage() != nil:
		list := msg.GetListMessage()
		if err := applyRichOrFallback(&out, msg, buildListRich(list)); err != nil {
			return InboundEvent{}, err
		}
		out.ContextMessageID = list.GetContextInfo().GetStanzaID()

	case msg.GetInteractiveMessage() != nil:
		interactive := msg.GetInteractiveMessage()
		if err := applyRichOrFallback(&out, msg, buildInteractiveRich(interactive)); err != nil {
			return InboundEvent{}, err
		}
		out.ContextMessageID = interactive.GetContextInfo().GetStanzaID()

	case msg.GetProductMessage() != nil:
		product := msg.GetProductMessage()
		if err := applyRichOrFallback(&out, msg, buildProductRich(product)); err != nil {
			return InboundEvent{}, err
		}
		out.ContextMessageID = product.GetContextInfo().GetStanzaID()

	case msg.GetEventMessage() != nil:
		event := msg.GetEventMessage()
		if err := applyRichOrFallback(&out, msg, buildEventRich(event)); err != nil {
			return InboundEvent{}, err
		}
		out.ContextMessageID = event.GetContextInfo().GetStanzaID()

	default:
		content, found := detectContentField(msg)
		if !found {
			return InboundEvent{}, ErrSkip
		}
		out.Type = "unsupported"
		out.Unsupported = &InboundUnsupported{Type: content}
	}

	return out, nil
}

func unwrapMessage(msg *waE2E.Message) *waE2E.Message {
	for i := 0; i < maxUnwrapDepth; i++ {
		switch {
		case msg.GetEphemeralMessage().GetMessage() != nil:
			msg = msg.GetEphemeralMessage().GetMessage()
		case msg.GetViewOnceMessage().GetMessage() != nil:
			msg = msg.GetViewOnceMessage().GetMessage()
		case msg.GetViewOnceMessageV2().GetMessage() != nil:
			msg = msg.GetViewOnceMessageV2().GetMessage()
		case msg.GetViewOnceMessageV2Extension().GetMessage() != nil:
			msg = msg.GetViewOnceMessageV2Extension().GetMessage()
		case msg.GetDocumentWithCaptionMessage().GetMessage() != nil:
			msg = msg.GetDocumentWithCaptionMessage().GetMessage()
		default:
			return msg
		}
	}
	return msg
}

func extractText(msg *waE2E.Message) string {
	if body := msg.GetConversation(); body != "" {
		return body
	}
	return msg.GetExtendedTextMessage().GetText()
}

func resolveSenderIdentifiers(ctx context.Context, resolver PNResolver, sender, senderAlt types.JID) (senderLid, senderPn string) {
	switch sender.Server {
	case types.HiddenUserServer:
		senderLid = sender.User
		if senderAlt.Server == types.DefaultUserServer {
			senderPn = senderAlt.User
		} else if resolver != nil {
			if pn, ok, err := resolver.PNForLID(ctx, sender); err == nil && ok && pn.Server == types.DefaultUserServer {
				senderPn = pn.User
			}
		}
	case types.DefaultUserServer:
		senderPn = sender.User
		if senderAlt.Server == types.HiddenUserServer {
			senderLid = senderAlt.User
		}
	default:
		senderPn = sender.User
	}
	return senderLid, senderPn
}

func pollSummary(poll *waE2E.PollCreationMessage) string {
	options := make([]string, 0, len(poll.GetOptions()))
	for _, opt := range poll.GetOptions() {
		options = append(options, opt.GetOptionName())
	}
	return "📊 " + poll.GetName() + " — " + strings.Join(options, ", ")
}

func applyRichOrFallback(out *InboundEvent, msg *waE2E.Message, rich *InboundRichContent) error {
	if rich != nil {
		out.Type = rich.Kind
		out.RichContent = rich
		return nil
	}
	content, found := detectContentField(msg)
	if !found {
		return ErrSkip
	}
	out.Type = "unsupported"
	out.Unsupported = &InboundUnsupported{Type: content}
	return nil
}

func buildButtonsRich(btn *waE2E.ButtonsMessage) *InboundRichContent {
	body := btn.GetContentText()
	if body == "" {
		body = btn.GetText()
	}
	footer := btn.GetFooterText()

	buttons := make([]InboundRichButton, 0, len(btn.GetButtons()))
	for _, b := range btn.GetButtons() {
		buttons = append(buttons, InboundRichButton{
			ID:   b.GetButtonID(),
			Text: b.GetButtonText().GetDisplayText(),
		})
	}

	if body == "" && footer == "" && len(buttons) == 0 {
		return nil
	}

	return &InboundRichContent{
		Kind:    "buttons",
		Body:    body,
		Footer:  footer,
		Buttons: buttons,
	}
}

func buildListRich(list *waE2E.ListMessage) *InboundRichContent {
	body := list.GetDescription()
	footer := list.GetFooterText()
	buttonText := list.GetButtonText()

	sections := make([]InboundRichListSection, 0, len(list.GetSections()))
	for _, s := range list.GetSections() {
		rows := make([]InboundRichListRow, 0, len(s.GetRows()))
		for _, r := range s.GetRows() {
			rows = append(rows, InboundRichListRow{
				ID:          r.GetRowID(),
				Title:       r.GetTitle(),
				Description: r.GetDescription(),
			})
		}
		sections = append(sections, InboundRichListSection{Title: s.GetTitle(), Rows: rows})
	}

	if body == "" && footer == "" && buttonText == "" && len(sections) == 0 {
		return nil
	}

	return &InboundRichContent{
		Kind:   "list",
		Body:   body,
		Footer: footer,
		List:   &InboundRichList{ButtonText: buttonText, Sections: sections},
	}
}

type nativeFlowButtonParams struct {
	DisplayText string                    `json:"display_text"`
	Title       string                    `json:"title"`
	ID          string                    `json:"id"`
	URL         string                    `json:"url"`
	Sections    []nativeFlowButtonSection `json:"sections"`
}

type nativeFlowButtonSection struct {
	Title string                `json:"title"`
	Rows  []nativeFlowButtonRow `json:"rows"`
}

type nativeFlowButtonRow struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

func parseNativeFlowButtonParams(raw string) nativeFlowButtonParams {
	var params nativeFlowButtonParams
	if raw == "" {
		return params
	}
	_ = json.Unmarshal([]byte(raw), &params)
	return params
}

func nativeFlowListFrom(sections []nativeFlowButtonSection, buttonText string) *InboundRichList {
	out := make([]InboundRichListSection, 0, len(sections))
	for _, s := range sections {
		rows := make([]InboundRichListRow, 0, len(s.Rows))
		for _, r := range s.Rows {
			rows = append(rows, InboundRichListRow{ID: r.ID, Title: r.Title, Description: r.Description})
		}
		out = append(out, InboundRichListSection{Title: s.Title, Rows: rows})
	}
	return &InboundRichList{ButtonText: buttonText, Sections: out}
}

func classifyNativeFlowName(name string) string {
	lower := strings.ToLower(name)
	switch {
	case strings.Contains(lower, "pay"), strings.Contains(lower, "payment"), strings.Contains(lower, "pix"), strings.Contains(lower, "order"):
		return "payment"
	case strings.Contains(lower, "cta_url"), strings.Contains(lower, "open_url"):
		return "cta"
	case strings.Contains(lower, "single_select"):
		return "list"
	case strings.Contains(lower, "quick_reply"):
		return "buttons"
	default:
		return "other"
	}
}

func dominantNativeFlow(flows []string) string {
	if len(flows) == 0 {
		return ""
	}
	counts := make(map[string]int, len(flows))
	best := flows[0]
	bestCount := 0
	for _, f := range flows {
		counts[f]++
		if counts[f] > bestCount {
			bestCount = counts[f]
			best = f
		}
	}
	return best
}

func buildInteractiveRich(im *waE2E.InteractiveMessage) *InboundRichContent {
	body := im.GetBody().GetText()
	footer := im.GetFooter().GetText()
	nativeButtons := im.GetNativeFlowMessage().GetButtons()

	buttons := make([]InboundRichButton, 0, len(nativeButtons))
	flows := make([]string, 0, len(nativeButtons))
	var list *InboundRichList

	for _, b := range nativeButtons {
		name := b.GetName()
		flows = append(flows, classifyNativeFlowName(name))

		params := parseNativeFlowButtonParams(b.GetButtonParamsJSON())
		if len(params.Sections) > 0 {
			list = nativeFlowListFrom(params.Sections, params.Title)
			continue
		}

		text := params.DisplayText
		if text == "" {
			text = params.Title
		}
		if text == "" {
			text = name
		}
		buttons = append(buttons, InboundRichButton{
			ID:   params.ID,
			Text: text,
			Name: name,
			URL:  params.URL,
		})
	}

	if body == "" && footer == "" && len(buttons) == 0 && list == nil {
		return nil
	}

	return &InboundRichContent{
		Kind:    "interactive",
		Flow:    dominantNativeFlow(flows),
		Body:    body,
		Footer:  footer,
		Buttons: buttons,
		List:    list,
	}
}

func formatProductPriceText(amount1000 int64, currency string) string {
	if amount1000 == 0 && currency == "" {
		return ""
	}
	value := float64(amount1000) / 1000.0
	symbol := currency
	if currency == "BRL" {
		symbol = "R$"
	}
	if symbol == "" {
		return fmt.Sprintf("%.2f", value)
	}
	return fmt.Sprintf("%s %.2f", symbol, value)
}

func buildProductRich(pm *waE2E.ProductMessage) *InboundRichContent {
	snap := pm.GetProduct()
	title := snap.GetTitle()
	if title == "" {
		return nil
	}

	return &InboundRichContent{
		Kind: "product",
		Product: &InboundRichProduct{
			Title:       title,
			Description: snap.GetDescription(),
			PriceText:   formatProductPriceText(snap.GetPriceAmount1000(), snap.GetCurrencyCode()),
			Currency:    snap.GetCurrencyCode(),
			RetailerID:  snap.GetRetailerID(),
		},
	}
}

func buildEventRich(em *waE2E.EventMessage) *InboundRichContent {
	name := em.GetName()
	if name == "" {
		return nil
	}

	event := &InboundRichEvent{
		Name:        name,
		Description: em.GetDescription(),
		Location:    em.GetLocation().GetName(),
		JoinLink:    em.GetJoinLink(),
		Canceled:    em.GetIsCanceled(),
	}
	if st := em.GetStartTime(); st != 0 {
		event.StartTime = strconv.FormatInt(st, 10)
	}
	if et := em.GetEndTime(); et != 0 {
		event.EndTime = strconv.FormatInt(et, 10)
	}

	return &InboundRichContent{Kind: "event", Event: event}
}

var contentFieldIgnore = map[string]bool{
	"SenderKeyDistributionMessage":               true,
	"FastRatchetKeySenderKeyDistributionMessage": true,
	"MessageContextInfo":                         true,
}

func detectContentField(msg *waE2E.Message) (string, bool) {
	if msg == nil {
		return "", false
	}

	v := reflect.ValueOf(msg).Elem()
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() || contentFieldIgnore[field.Name] {
			continue
		}
		fv := v.Field(i)
		if fv.Kind() != reflect.Ptr || fv.IsNil() {
			continue
		}
		return jsonFieldName(field), true
	}
	return "", false
}

func jsonFieldName(field reflect.StructField) string {
	tag := field.Tag.Get("json")
	if tag == "" {
		return field.Name
	}
	name := strings.Split(tag, ",")[0]
	if name == "" {
		return field.Name
	}
	return name
}

func contactFrom(displayName, vcard string) InboundContact {
	return InboundContact{
		Name:  &InboundContactName{FormattedName: displayName},
		Vcard: vcard,
	}
}

func downloadAndStore(ctx context.Context, dl Downloader, s3 MediaStore, tenantID, providerMessageID, mime string, msg whatsmeow.DownloadableMessage) (*InboundMedia, error) {
	data, err := dl.Download(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("mapper: download media: %w", err)
	}

	key := fmt.Sprintf("inbound-media/%s/%s", tenantID, providerMessageID)
	if err := s3.Put(ctx, key, mime, data); err != nil {
		return nil, fmt.Errorf("mapper: store media: %w", err)
	}

	return &InboundMedia{Key: key, MimeType: mime}, nil
}

func BuildStatus(evt *events.Receipt) []StatusEvent {
	status := statusFor(evt.Type)
	if status == "" {
		return nil
	}

	timestamp := strconv.FormatInt(evt.Timestamp.Unix(), 10)
	out := make([]StatusEvent, 0, len(evt.MessageIDs))
	for _, id := range evt.MessageIDs {
		out = append(out, StatusEvent{ProviderMessageID: id, Status: status, Timestamp: timestamp})
	}
	return out
}

func statusFor(t types.ReceiptType) string {
	switch t {
	case types.ReceiptTypeDelivered:
		return "delivered"
	case types.ReceiptTypeRead:
		return "read"
	default:
		return ""
	}
}
