package mapper_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/w3nder/whatsmeow-gateway/internal/avatar"
	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
)

const groupInboundWireContract = `{"phoneNumberId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","from":"120363000000000000@g.us","providerMessageId":"3EB068F90C62346073B954","timestamp":"1700000000","type":"text","text":{"body":"bom dia pessoal"},"group":{"jid":"120363000000000000@g.us","name":"Vendas Nordeste","profilePicture":{"key":"profile-pictures/1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c/120363000000000000/group-pic-1","id":"group-pic-1","mimeType":"image/jpeg"},"participant":{"jid":"173907587899617@lid","lid":"173907587899617","pn":"5511888887777","name":"João Silva","profilePicture":{"key":"profile-pictures/1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c/173907587899617/participant-pic-1","id":"participant-pic-1","mimeType":"image/jpeg"}}}}`

const groupInboundFromMeWireContract = `{"phoneNumberId":"2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d","from":"120363000000000000@g.us","fromMe":true,"providerMessageId":"3EB068F90C62346073B954","timestamp":"1700000000","type":"text","text":{"body":"bom dia pessoal"},"group":{"jid":"120363000000000000@g.us","name":"Vendas Nordeste","profilePicture":{"key":"profile-pictures/1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c/120363000000000000/group-pic-1","id":"group-pic-1","mimeType":"image/jpeg"}}}`

type contractAvatars struct{}

func (contractAvatars) For(_ context.Context, jid types.JID) *avatar.Picture {
	if jid.Server == types.GroupServer {
		return &avatar.Picture{
			Key:      "profile-pictures/1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c/120363000000000000/group-pic-1",
			ID:       "group-pic-1",
			MimeType: "image/jpeg",
		}
	}
	return &avatar.Picture{
		Key:      "profile-pictures/1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c/173907587899617/participant-pic-1",
		ID:       "participant-pic-1",
		MimeType: "image/jpeg",
	}
}

func contractDeps() mapper.InboundDeps {
	return mapper.InboundDeps{
		Groups:    staticGroupNamer("Vendas Nordeste"),
		Avatars:   contractAvatars{},
		ChannelID: "2a4b6c8d-0e1f-4a3b-9c5d-7e8f0a1b2c3d",
		TenantID:  "1f3a5c7e-9b2d-4e6f-8a1c-3d5e7f9b1a2c",
	}
}

func contractGroupEvent(fromMe bool) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:      types.NewJID("120363000000000000", types.GroupServer),
				Sender:    types.NewJID("173907587899617", types.HiddenUserServer),
				SenderAlt: types.NewJID("5511888887777", types.DefaultUserServer),
				IsFromMe:  fromMe,
				IsGroup:   true,
			},
			ID:        "3EB068F90C62346073B954",
			PushName:  "João Silva",
			Timestamp: time.Unix(1700000000, 0),
		},
		Message: &waE2E.Message{Conversation: proto.String("bom dia pessoal")},
	}
}

func TestGroupInboundWireContractWithParticipant(t *testing.T) {
	out, err := mapper.BuildInbound(context.Background(), contractDeps(), contractGroupEvent(false))
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}

	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal whatsapp.group.inbound.v1 event: %v", err)
	}
	if string(raw) != groupInboundWireContract {
		t.Fatalf("whatsapp.group.inbound.v1 wire contract drifted\n got: %s\nwant: %s", raw, groupInboundWireContract)
	}
}

func TestGroupInboundWireContractOmitsParticipantOnOurOwnMessage(t *testing.T) {
	out, err := mapper.BuildInbound(context.Background(), contractDeps(), contractGroupEvent(true))
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}

	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal whatsapp.group.inbound.v1 event: %v", err)
	}
	if string(raw) != groupInboundFromMeWireContract {
		t.Fatalf("whatsapp.group.inbound.v1 fromMe wire contract drifted\n got: %s\nwant: %s", raw, groupInboundFromMeWireContract)
	}
}

const pollVoteWithdrawnWireContract = `{"phoneNumberId":"channel-1","from":"5511999999999","senderPn":"5511999999999","profileName":"Jane Doe","providerMessageId":"wamid.poll-vote-1","timestamp":"1700000000","type":"poll_vote","target":{"providerMessageId":"POLL123"},"pollVote":{"selectedOptionHashes":[]}}`

const pollVoteWithSelectionsWireContract = `{"phoneNumberId":"channel-1","from":"5511999999999","senderPn":"5511999999999","profileName":"Jane Doe","providerMessageId":"wamid.poll-vote-1","timestamp":"1700000000","type":"poll_vote","target":{"providerMessageId":"POLL123"},"pollVote":{"selectedOptionHashes":["c164","5b19"]}}`

func TestPollVoteWireContractWithdrawnKeepsAnEmptyArray(t *testing.T) {
	out, err := mapper.BuildInbound(context.Background(), depsWithSecrets(fakeSecrets{
		vote: &waE2E.PollVoteMessage{},
	}), pollUpdateEvent())
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}

	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal poll_vote event: %v", err)
	}
	if string(raw) != pollVoteWithdrawnWireContract {
		t.Fatalf("withdrawn poll_vote wire contract drifted\n got: %s\nwant: %s", raw, pollVoteWithdrawnWireContract)
	}
}

const adReferralWireContract = `{"phoneNumberId":"channel-1","from":"5511999999999","senderPn":"5511999999999","profileName":"Jane Doe","providerMessageId":"wamid.ad-referral-1","timestamp":"1700000000","type":"text","text":{"body":"quero saber mais"},"contextMessageId":"wamid.ad-source","adReferral":{"ctwaClid":"ARBxyz","sourceId":"120210000000000000","sourceType":"ad","sourceApp":"facebook","sourceUrl":"https://fb.me/2abc","headline":"Frete gratis hoje","body":"So ate as 18h","mediaType":"image","mediaUrl":"https://scontent.example/ad.jpg","thumbnailUrl":"https://scontent.example/thumb.jpg","thumbnailB64":"/9j/","entryPoint":"message"}}`

func adReferralEvent() *events.Message {
	return &events.Message{
		Info: baseInfo("wamid.ad-referral-1", "5511999999999"),
		Message: &waE2E.Message{
			ExtendedTextMessage: &waE2E.ExtendedTextMessage{
				Text: proto.String("quero saber mais"),
				ContextInfo: &waE2E.ContextInfo{
					StanzaID:         proto.String("wamid.ad-source"),
					ConversionSource: proto.String("ctwa_landing_page"),
					ExternalAdReply: &waE2E.ContextInfo_ExternalAdReplyInfo{
						CtwaClid:     proto.String("ARBxyz"),
						SourceID:     proto.String("120210000000000000"),
						SourceType:   proto.String("ad"),
						SourceApp:    proto.String("facebook"),
						SourceURL:    proto.String("https://fb.me/2abc"),
						Title:        proto.String("Frete gratis hoje"),
						Body:         proto.String("So ate as 18h"),
						MediaType:    waE2E.ContextInfo_ExternalAdReplyInfo_IMAGE.Enum(),
						MediaURL:     proto.String("https://scontent.example/ad.jpg"),
						ThumbnailURL: proto.String("https://scontent.example/thumb.jpg"),
						Thumbnail:    []byte{0xff, 0xd8, 0xff},
					},
				},
			},
		},
	}
}

func TestAdReferralWireContract(t *testing.T) {
	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), adReferralEvent())
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}

	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal whatsapp.inbound.v1 event: %v", err)
	}
	if string(raw) != adReferralWireContract {
		t.Fatalf("adReferral wire contract drifted\n got: %s\nwant: %s", raw, adReferralWireContract)
	}
}

func TestPollVoteWireContractWithSelectionsEncodesLowercaseHex(t *testing.T) {
	out, err := mapper.BuildInbound(context.Background(), depsWithSecrets(fakeSecrets{
		vote: &waE2E.PollVoteMessage{SelectedOptions: [][]byte{{0xc1, 0x64}, {0x5b, 0x19}}},
	}), pollUpdateEvent())
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}

	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal poll_vote event: %v", err)
	}
	if string(raw) != pollVoteWithSelectionsWireContract {
		t.Fatalf("poll_vote with selections wire contract drifted\n got: %s\nwant: %s", raw, pollVoteWithSelectionsWireContract)
	}
}
