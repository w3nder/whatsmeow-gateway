package mapper

import (
	"encoding/base64"
	"testing"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

func TestAdReferralFromExternalAdReply(t *testing.T) {
	ci := &waE2E.ContextInfo{
		ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{
			SourceID:     proto.String("120210000000000000"),
			SourceType:   proto.String("ad"),
			SourceURL:    proto.String("https://fb.me/2abc"),
			SourceApp:    proto.String("facebook"),
			CtwaClid:     proto.String("ARBxyz"),
			Title:        proto.String("Frete gratis hoje"),
			Body:         proto.String("So ate as 18h"),
			ThumbnailURL: proto.String("https://scontent.example/thumb.jpg"),
			Thumbnail:    []byte{0xff, 0xd8, 0xff},
		},
	}

	got := adReferralFrom(ci)

	if got == nil {
		t.Fatal("expected a referral")
	}
	if got.CtwaClid != "ARBxyz" || got.SourceID != "120210000000000000" {
		t.Fatalf("unexpected identity: %+v", got)
	}
	if got.Headline != "Frete gratis hoje" {
		t.Fatalf("expected the ad title as headline, got %q", got.Headline)
	}
	if got.EntryPoint != "message" {
		t.Fatalf("expected entryPoint message, got %q", got.EntryPoint)
	}
	if got.ThumbnailB64 != base64.StdEncoding.EncodeToString([]byte{0xff, 0xd8, 0xff}) {
		t.Fatalf("expected the thumbnail bytes encoded, got %q", got.ThumbnailB64)
	}
}

func TestAdReferralFromSourceIDWithConversionMarkerAndNoCtwaClid(t *testing.T) {
	ci := &waE2E.ContextInfo{
		ConversionSource: proto.String("ctwa_landing_page"),
		ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{
			SourceID:  proto.String("120210000000000000"),
			SourceURL: proto.String("https://fb.me/2abc"),
			Title:     proto.String("Frete gratis hoje"),
		},
	}

	got := adReferralFrom(ci)

	if got == nil {
		t.Fatal("expected a referral: SourceID plus a conversion marker is a genuine ad entry even without a CtwaClid")
	}
	if got.CtwaClid != "" {
		t.Fatalf("expected no CtwaClid, got %q", got.CtwaClid)
	}
	if got.SourceID != "120210000000000000" {
		t.Fatalf("unexpected SourceID: %q", got.SourceID)
	}
	if got.ConversionSource != "ctwa_landing_page" {
		t.Fatalf("expected the conversion marker preserved, got %q", got.ConversionSource)
	}
}

func TestAdReferralNormalizesSourceAppFromKnownHost(t *testing.T) {
	ci := &waE2E.ContextInfo{
		ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{
			CtwaClid:  proto.String("ARBxyz"),
			SourceURL: proto.String("https://www.instagram.com/p/abc123"),
			SourceApp: proto.String("some-internal-code-1234"),
		},
	}

	got := adReferralFrom(ci)

	if got == nil {
		t.Fatal("expected a referral")
	}
	if got.SourceApp != "instagram" {
		t.Fatalf("expected sourceApp derived from the source_url host, got %q", got.SourceApp)
	}
}

func TestAdReferralOmitsSourceAppForAnUnknownHost(t *testing.T) {
	ci := &waE2E.ContextInfo{
		ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{
			CtwaClid:  proto.String("ARBxyz"),
			SourceURL: proto.String("https://ads.example.com/click/1"),
			SourceApp: proto.String("facebook"),
		},
	}

	got := adReferralFrom(ci)

	if got == nil {
		t.Fatal("expected a referral")
	}
	if got.SourceApp != "" {
		t.Fatalf("expected sourceApp omitted for an unrecognized host, got %q", got.SourceApp)
	}
}

func TestAdReferralIgnoresPlainLinkReply(t *testing.T) {
	ci := &waE2E.ContextInfo{
		ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{
			Title:     proto.String("Uma noticia qualquer"),
			SourceURL: proto.String("https://portal.example/materia"),
		},
	}

	if got := adReferralFrom(ci); got != nil {
		t.Fatalf("a link preview must not become an attribution, got %+v", got)
	}
}

func TestApplyContextInfoKeepsTheQuotedMessage(t *testing.T) {
	out := &InboundEvent{}
	applyContextInfo(out, &waE2E.ContextInfo{StanzaID: proto.String("wamid.quoted")})

	if out.ContextMessageID != "wamid.quoted" {
		t.Fatalf("expected the quoted id preserved, got %q", out.ContextMessageID)
	}
	if out.AdReferral != nil {
		t.Fatal("a plain reply carries no attribution")
	}
}
