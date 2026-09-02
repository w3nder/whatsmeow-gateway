package mapper

import (
	"encoding/base64"
	"net/url"
	"strings"

	"go.mau.fi/whatsmeow/proto/waE2E"
)

type InboundAdReferral struct {
	CtwaClid     string `json:"ctwaClid,omitempty"`
	SourceID     string `json:"sourceId,omitempty"`
	SourceType   string `json:"sourceType,omitempty"`
	SourceApp    string `json:"sourceApp,omitempty"`
	SourceURL    string `json:"sourceUrl,omitempty"`
	Headline     string `json:"headline,omitempty"`
	Body         string `json:"body,omitempty"`
	MediaType    string `json:"mediaType,omitempty"`
	MediaURL     string `json:"mediaUrl,omitempty"`
	ThumbnailURL string `json:"thumbnailUrl,omitempty"`
	ThumbnailB64 string `json:"thumbnailB64,omitempty"`
}

func adReferralFrom(ci *waE2E.ContextInfo) *InboundAdReferral {
	ad := ci.GetExternalAdReply()
	if ad == nil {
		return nil
	}
	conversion := firstNonEmpty(ci.GetConversionSource(), ci.GetEntryPointConversionSource())
	if ad.GetCtwaClid() == "" && (ad.GetSourceID() == "" || conversion == "") {
		return nil
	}
	referral := &InboundAdReferral{
		CtwaClid:     ad.GetCtwaClid(),
		SourceID:     ad.GetSourceID(),
		SourceType:   ad.GetSourceType(),
		SourceApp:    sourceAppFrom(ad.GetSourceURL()),
		SourceURL:    ad.GetSourceURL(),
		Headline:     ad.GetTitle(),
		Body:         ad.GetBody(),
		MediaType:    adMediaType(ad.GetMediaType()),
		MediaURL:     ad.GetMediaURL(),
		ThumbnailURL: ad.GetThumbnailURL(),
	}
	if thumb := ad.GetThumbnail(); len(thumb) > 0 {
		referral.ThumbnailB64 = base64.StdEncoding.EncodeToString(thumb)
	}
	return referral
}

var facebookAdHosts = []string{"facebook.com", "fb.me", "fb.com"}
var instagramAdHosts = []string{"instagram.com", "ig.me"}

func sourceAppFrom(sourceURL string) string {
	if sourceURL == "" {
		return ""
	}
	parsed, err := url.Parse(sourceURL)
	if err != nil {
		return ""
	}
	host := strings.ToLower(strings.TrimPrefix(parsed.Hostname(), "www."))
	if host == "" {
		return ""
	}
	if matchesAdHost(host, facebookAdHosts) {
		return "facebook"
	}
	if matchesAdHost(host, instagramAdHosts) {
		return "instagram"
	}
	return ""
}

func matchesAdHost(host string, knownHosts []string) bool {
	for _, known := range knownHosts {
		if host == known || strings.HasSuffix(host, "."+known) {
			return true
		}
	}
	return false
}

func adMediaType(t waE2E.ContextInfo_ExternalAdReplyInfo_MediaType) string {
	switch t {
	case waE2E.ContextInfo_ExternalAdReplyInfo_IMAGE:
		return "image"
	case waE2E.ContextInfo_ExternalAdReplyInfo_VIDEO:
		return "video"
	default:
		return ""
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func applyContextInfo(out *InboundEvent, ci *waE2E.ContextInfo) {
	if ci == nil {
		return
	}
	if stanza := ci.GetStanzaID(); stanza != "" {
		out.ContextMessageID = stanza
	}
	if referral := adReferralFrom(ci); referral != nil {
		out.AdReferral = referral
	}
}
