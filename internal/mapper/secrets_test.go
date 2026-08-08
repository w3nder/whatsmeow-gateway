package mapper_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/w3nder/whatsmeow-gateway/internal/mapper"
)

type fakeSecrets struct {
	decrypted *waE2E.Message
	vote      *waE2E.PollVoteMessage
	err       error
}

func (f fakeSecrets) DecryptSecretEncryptedMessage(ctx context.Context, evt *events.Message) (*waE2E.Message, error) {
	return f.decrypted, f.err
}

func (f fakeSecrets) DecryptPollVote(ctx context.Context, evt *events.Message) (*waE2E.PollVoteMessage, error) {
	return f.vote, f.err
}

func depsWithSecrets(s mapper.MessageSecrets) mapper.InboundDeps {
	deps := testDeps(fakeDownloader{}, nil, &fakeMediaStore{})
	deps.Secrets = s
	return deps
}

func secretEditEvent() *events.Message {
	return &events.Message{
		Info: baseInfo("wamid.secret-edit-1", "5511999999999"),
		Message: &waE2E.Message{
			SecretEncryptedMessage: &waE2E.SecretEncryptedMessage{
				TargetMessageKey: &waCommon.MessageKey{ID: proto.String("TARGET123")},
				SecretEncType:    waE2E.SecretEncryptedMessage_MESSAGE_EDIT.Enum(),
			},
		},
	}
}

func TestBuildInboundSecretEncryptedEditMatchesThePlainEdit(t *testing.T) {
	secret := secretEditEvent()
	secretOut, err := mapper.BuildInbound(context.Background(), depsWithSecrets(fakeSecrets{
		decrypted: &waE2E.Message{Conversation: proto.String("novo texto")},
	}), secret)
	if err != nil {
		t.Fatalf("BuildInbound on the secret edit: %v", err)
	}

	plain := &events.Message{
		Info: baseInfo("wamid.secret-edit-1", "5511999999999"),
		Message: &waE2E.Message{
			ProtocolMessage: &waE2E.ProtocolMessage{
				Type:          waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(),
				Key:           &waCommon.MessageKey{ID: proto.String("TARGET123")},
				EditedMessage: &waE2E.Message{Conversation: proto.String("novo texto")},
			},
		},
	}
	plainOut, err := mapper.BuildInbound(context.Background(), depsWithSecrets(fakeSecrets{}), plain)
	if err != nil {
		t.Fatalf("BuildInbound on the plain edit: %v", err)
	}

	if !reflect.DeepEqual(secretOut, plainOut) {
		t.Fatalf("the encrypted edit must produce the same event as the plain one\n secret: %+v\n plain:  %+v", secretOut, plainOut)
	}
	if secretOut.Type != "edit" {
		t.Fatalf("expected Type=edit, got %q", secretOut.Type)
	}
}

func TestBuildInboundSecretEncryptedEditUnwrapsANestedProtocolMessage(t *testing.T) {
	out, err := mapper.BuildInbound(context.Background(), depsWithSecrets(fakeSecrets{
		decrypted: &waE2E.Message{
			ProtocolMessage: &waE2E.ProtocolMessage{
				Type:          waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(),
				Key:           &waCommon.MessageKey{ID: proto.String("TARGET123")},
				EditedMessage: &waE2E.Message{Conversation: proto.String("texto de dentro")},
			},
		},
	}), secretEditEvent())
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Text == nil || out.Text.Body != "texto de dentro" {
		t.Fatalf("expected the nested edited message to be unwrapped, got %+v", out.Text)
	}
}

func TestBuildInboundSecretEncryptedNonEditIsSkipped(t *testing.T) {
	evt := secretEditEvent()
	evt.Message.SecretEncryptedMessage.SecretEncType = waE2E.SecretEncryptedMessage_UNKNOWN.Enum()

	_, err := mapper.BuildInbound(context.Background(), depsWithSecrets(fakeSecrets{}), evt)
	if !errors.Is(err, mapper.ErrSkip) {
		t.Fatalf("expected mapper.ErrSkip for a non-edit secret enc type, got %v", err)
	}
}

func TestBuildInboundSecretEncryptedEditWithoutTargetIsSkipped(t *testing.T) {
	evt := secretEditEvent()
	evt.Message.SecretEncryptedMessage.TargetMessageKey = nil

	_, err := mapper.BuildInbound(context.Background(), depsWithSecrets(fakeSecrets{
		decrypted: &waE2E.Message{Conversation: proto.String("novo texto")},
	}), evt)
	if !errors.Is(err, mapper.ErrSkip) {
		t.Fatalf("expected mapper.ErrSkip for a secret edit with no target key, got %v", err)
	}
}

func TestBuildInboundSecretEncryptedEditThatFailsToDecryptIsSkipped(t *testing.T) {
	_, err := mapper.BuildInbound(context.Background(), depsWithSecrets(fakeSecrets{
		err: errors.New("no message secret"),
	}), secretEditEvent())
	if !errors.Is(err, mapper.ErrSkip) {
		t.Fatalf("expected mapper.ErrSkip when decryption fails, got %v", err)
	}
}

func TestBuildInboundSecretEncryptedWithoutSecretsBehavesAsBefore(t *testing.T) {
	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), secretEditEvent())
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "unsupported" {
		t.Fatalf("expected Type=unsupported with no MessageSecrets, got %q", out.Type)
	}
}

func pollUpdateEvent() *events.Message {
	return &events.Message{
		Info: baseInfo("wamid.poll-vote-1", "5511999999999"),
		Message: &waE2E.Message{
			PollUpdateMessage: &waE2E.PollUpdateMessage{
				PollCreationMessageKey: &waCommon.MessageKey{
					ID:        proto.String("POLL123"),
					FromMe:    proto.Bool(true),
					RemoteJID: proto.String("2002125877314@lid"),
				},
				Vote: &waE2E.PollEncValue{
					EncPayload: []byte("cifrado"),
					EncIV:      []byte("iv"),
				},
			},
		},
	}
}

func TestBuildInboundPollUpdateBecomesAPollVote(t *testing.T) {
	out, err := mapper.BuildInbound(context.Background(), depsWithSecrets(fakeSecrets{
		vote: &waE2E.PollVoteMessage{SelectedOptions: [][]byte{{0xc1, 0x64}, {0x5b, 0x19}}},
	}), pollUpdateEvent())
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "poll_vote" {
		t.Fatalf("expected Type=poll_vote, got %q", out.Type)
	}
	if out.Target == nil || out.Target.ProviderMessageID != "POLL123" {
		t.Fatalf("expected the poll creation id in Target, got %+v", out.Target)
	}
	if out.PollVote == nil {
		t.Fatalf("expected a PollVote payload, got nil")
	}
	if got := out.PollVote.SelectedOptionHashes; len(got) != 2 || got[0] != "c164" || got[1] != "5b19" {
		t.Fatalf("expected the hashes in lowercase hex, got %+v", got)
	}
}

func TestBuildInboundPollVoteWithNoSelectionIsAnEmptyList(t *testing.T) {
	out, err := mapper.BuildInbound(context.Background(), depsWithSecrets(fakeSecrets{
		vote: &waE2E.PollVoteMessage{},
	}), pollUpdateEvent())
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.PollVote == nil || out.PollVote.SelectedOptionHashes == nil {
		t.Fatalf("a withdrawn vote must carry an empty list, not nil: %+v", out.PollVote)
	}
	if len(out.PollVote.SelectedOptionHashes) != 0 {
		t.Fatalf("expected no hashes, got %+v", out.PollVote.SelectedOptionHashes)
	}
}

func TestBuildInboundPollVoteWithoutAPollKeyIsSkipped(t *testing.T) {
	evt := pollUpdateEvent()
	evt.Message.PollUpdateMessage.PollCreationMessageKey = nil

	_, err := mapper.BuildInbound(context.Background(), depsWithSecrets(fakeSecrets{
		vote: &waE2E.PollVoteMessage{SelectedOptions: [][]byte{{0xc1, 0x64}}},
	}), evt)
	if !errors.Is(err, mapper.ErrSkip) {
		t.Fatalf("expected mapper.ErrSkip for a vote with no poll key, got %v", err)
	}
}

func TestBuildInboundPollUpdateThatFailsToDecryptIsSkipped(t *testing.T) {
	_, err := mapper.BuildInbound(context.Background(), depsWithSecrets(fakeSecrets{
		err: errors.New("no message secret"),
	}), pollUpdateEvent())
	if !errors.Is(err, mapper.ErrSkip) {
		t.Fatalf("expected mapper.ErrSkip when the vote cannot be decrypted, got %v", err)
	}
}

func TestBuildInboundPollUpdateWithoutSecretsBehavesAsBefore(t *testing.T) {
	out, err := mapper.BuildInbound(context.Background(), testDeps(fakeDownloader{}, nil, &fakeMediaStore{}), pollUpdateEvent())
	if err != nil {
		t.Fatalf("BuildInbound: %v", err)
	}
	if out.Type != "unsupported" {
		t.Fatalf("expected Type=unsupported with no MessageSecrets, got %q", out.Type)
	}
}
