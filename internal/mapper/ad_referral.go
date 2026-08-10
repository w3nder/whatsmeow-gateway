package mapper

import (
	"encoding/base64"

	"go.mau.fi/whatsmeow/proto/waE2E"
)

// InboundAdReferral is the ad a conversation came from, in the same shape the
// WABA adapter produces. Both channels feed one contract, so the backend never
// learns which one it was talking to.
type InboundAdReferral struct {
	CtwaClid         string `json:"ctwaClid,omitempty"`
	SourceID         string `json:"sourceId,omitempty"`
	SourceType       string `json:"sourceType,omitempty"`
	SourceApp        string `json:"sourceApp,omitempty"`
	SourceURL        string `json:"sourceUrl,omitempty"`
	Headline         string `json:"headline,omitempty"`
	Body             string `json:"body,omitempty"`
	MediaType        string `json:"mediaType,omitempty"`
	MediaURL         string `json:"mediaUrl,omitempty"`
	ThumbnailURL     string `json:"thumbnailUrl,omitempty"`
	ThumbnailB64     string `json:"thumbnailB64,omitempty"`
	EntryPoint       string `json:"entryPoint"`
	ConversionSource string `json:"conversionSource,omitempty"`
}

// adReferralFrom reads the ad behind a message, and returns nil when there is
// none.
//
// The guard is the whole point of this function. ExternalAdReply also rides on
// an ordinary reply to a shared link -- WhatsApp renders both with the same
// card -- so treating its mere presence as "this lead came from an ad" would
// credit ads for conversations they never bought. A genuine click-to-WhatsApp
// entry always carries either the click id or an ad id alongside a conversion
// marker; a link preview carries neither.
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
		CtwaClid:         ad.GetCtwaClid(),
		SourceID:         ad.GetSourceID(),
		SourceType:       ad.GetSourceType(),
		SourceApp:        ad.GetSourceApp(),
		SourceURL:        ad.GetSourceURL(),
		Headline:         ad.GetTitle(),
		Body:             ad.GetBody(),
		MediaType:        adMediaType(ad.GetMediaType()),
		MediaURL:         ad.GetMediaURL(),
		ThumbnailURL:     ad.GetThumbnailURL(),
		EntryPoint:       "message",
		ConversionSource: conversion,
	}
	if thumb := ad.GetThumbnail(); len(thumb) > 0 {
		referral.ThumbnailB64 = base64.StdEncoding.EncodeToString(thumb)
	}
	return referral
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

// applyContextInfo pulls from one ContextInfo the two things an inbound event
// needs from it: the message being replied to, and the ad that started the
// conversation. Every message type that carries a ContextInfo goes through
// here, so neither fact can be picked up for one type and forgotten for
// another.
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
