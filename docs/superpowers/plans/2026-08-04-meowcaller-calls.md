# Integração meowcaller — Plano de Implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expor no whatsmeow-gateway a superfície completa da biblioteca `github.com/purpshell/meowcaller` — chamadas 1:1 de áudio e vídeo, chamadas de grupo, call links, reações, hand raise e screen share — em modo sinalização + gravação, com a gravação subindo para o S3 e sendo entregue no evento pela `key`, igual à mídia das mensagens.

**Architecture:** Um pacote novo `internal/call` concentra registry de chamadas vivas, tradução de callbacks da lib em eventos AMQP, despacho de comandos e gravação. `internal/session` passa a construir o `meowcaller.Client` junto com o `whatsmeow.Client` (obrigatoriamente antes do `Connect()`) e o expõe por uma interface nossa, `call.Caller`, para que os fakes de teste continuem montáveis sem stack VoIP real. Uma exchange/fila nova (`whatsapp.gateway.call.v1` → `gateway.call`) carrega os comandos, separada de `gateway.send` porque chamada é longa e travaria o prefetch das mensagens. Os eventos saem no `sender.events` já existente, na routing key nova `whatsapp.call.v1`.

**Tech Stack:** Go 1.25 · `go.mau.fi/whatsmeow` · `github.com/purpshell/meowcaller` · RabbitMQ (`amqp091-go`) · S3 (`aws-sdk-go-v2`) · testcontainers-go

**Spec:** `docs/superpowers/specs/2026-08-04-meowcaller-call-integration-design.md`

## Global Constraints

- Go 1.25.0. Build obrigatoriamente com `CGO_ENABLED=0` — a imagem é `gcr.io/distroless/static-debian12`. Nenhuma dependência que exija CGO entra.
- Sem ffmpeg, sem encoder e sem decoder de vídeo no gateway. `Call.SendVideo` recebe H.264 Annex-B já codificado; `Call.ReceiveVideo` entrega Annex-B cru, que é gravado como está.
- `github.com/purpshell/meowcaller` usa o mesmo pin de whatsmeow que o gateway: `go.mau.fi/whatsmeow v0.0.0-20260722203353-e9a033b24933`. Se o `go get` tentar mover esse pin, pare e reporte — não faça upgrade de whatsmeow dentro deste plano.
- Áudio da lib é PCM mono `[]float32` a `meowcaller.SampleRate` (16000 Hz), em frames de `meowcaller.FrameSamples`.
- Toda mensagem de erro e todo log seguem o padrão do repositório: prefixo do pacote e `%w` no wrap — `fmt.Errorf("call: dial %s: %w", target, err)`.
- Nomes de campo JSON em lowerCamelCase, como em `InboundEvent` e `GatewaySendCommand`.
- Comandos de chamada **não** passam pelo `dedupe.Store`.
- `OnParticipantVideoFrame` e `OnVideoKeyframeRequest` nunca viram evento AMQP.
- Lint: `make lint` (golangci-lint) e `make vet` precisam passar limpos ao fim de cada tarefa.

## Aviso da biblioteca

A meowcaller marca dois caminhos como **NOT VALIDATED** no próprio código-fonte:

- `Call.ReceiveVideo` — "the inbound-video media path is unproven (no captured video-RTP vector)"
- `Call.SendVideo` / `SendVideoWithDuration` — "the video send media path is unproven"

Ou seja: a sinalização de vídeo (negociar, aceitar, upgrade, orientação) é exercitada, mas o transporte de frames de vídeo não foi validado pelos autores contra tráfego real. O plano implementa os dois caminhos porque o contrato pede, e os testa contra fakes — mas **não** assuma que gravação de vídeo funciona em produção sem uma chamada real de ponta a ponta. Áudio não tem essa ressalva.

## Estrutura de arquivos

| Arquivo | Responsabilidade |
|---|---|
| `internal/call/contracts.go` (criar) | Tipos de comando e evento de chamada, e a normalização de valores da lib para string |
| `internal/call/port.go` (criar) | Interfaces `Caller`, `LiveCall`, `RecordingStore` — a fronteira entre o gateway e a meowcaller |
| `internal/call/recorder.go` (criar) | Gravação de áudio (WAV) e vídeo (Annex-B) em arquivo temporário e upload |
| `internal/call/registry.go` (criar) | Mapa channelID → callID → chamada viva, com timestamps e gravadores |
| `internal/call/manager.go` (criar) | Assina os callbacks da lib e traduz em `CallEvent` publicado |
| `internal/call/command.go` (criar) | Despacho de `GatewayCallCommand` para ação em `Caller`/`LiveCall` |
| `internal/call/annexb.go` (criar) | Fatiamento de um arquivo H.264 Annex-B em access units |
| `internal/session/caller.go` (criar) | Adaptador dos tipos concretos da meowcaller para `call.Caller`/`call.LiveCall` |
| `internal/session/client.go` (modificar) | Construir o `meowcaller.Client` antes do `Connect()`; expor `Calls()` |
| `internal/media/s3.go` (modificar) | `PutStream` para subir gravação sem carregar tudo em memória |
| `internal/amqp/contracts.go` (modificar) | `GatewayCallCommand` |
| `internal/amqp/topology.go` (modificar) | Exchange/fila/DLX de chamada e a routing key de evento |
| `internal/amqp/consumer.go` (modificar) | `StartCall` |
| `internal/amqp/publisher.go` (modificar) | `PublishCall` |
| `internal/gateway/gateway.go` (modificar) | `CallHandler`, fiação do `call.Manager`, teardown por canal |
| `internal/config/config.go` (modificar) | `CallTmpDir`, `CallRecord` |
| `cmd/gateway/main.go` (modificar) | Instanciar e injetar o `call.Manager` |
| `test/fake_test.go` (modificar) | `BuildReaction` e `Calls()` no fake |
| `test/gateway_call_test.go` (criar) | E2E: comando na fila → evento no `sender.events` → objeto no MinIO |

---

### Task 0: Consertar o pacote de testes quebrado na main

O pacote `test` não compila hoje. `BuildReaction` entrou em `session.WAClient` no commit `8fdb775` e o `fakeWAClient` nunca foi atualizado, então `var _ session.WAClient = (*fakeWAClient)(nil)` falha. Sem isso nenhuma tarefa seguinte consegue rodar seus próprios testes.

**Files:**
- Modify: `test/fake_test.go:127-129`

**Interfaces:**
- Consumes: nada
- Produces: pacote `test` compilando, pré-requisito de todas as tarefas seguintes

- [ ] **Step 1: Reproduzir a quebra**

Run: `go vet ./...`
Expected: FAIL com `*fakeWAClient does not implement session.WAClient (missing method BuildReaction)`

- [ ] **Step 2: Implementar o método faltante**

Em `test/fake_test.go`, logo depois de `BuildRevoke` (linha 129), no mesmo estilo dos outros builders do fake:

```go
func (f *fakeWAClient) BuildReaction(chat, sender types.JID, id types.MessageID, reaction string) *waE2E.Message {
	return &waE2E.Message{
		ReactionMessage: &waE2E.ReactionMessage{
			Key:  &waCommon.MessageKey{ID: proto.String(string(id))},
			Text: proto.String(reaction),
		},
	}
}
```

Adicione os imports que faltarem em `test/fake_test.go`:

```go
"go.mau.fi/whatsmeow/proto/waCommon"
"google.golang.org/protobuf/proto"
```

- [ ] **Step 3: Verificar que compila e os testes passam**

Run: `go vet ./... && go test ./internal/...`
Expected: vet limpo; testes de `internal/` passando

- [ ] **Step 4: Commit**

```bash
git add test/fake_test.go
git commit -m "fix(test): implement BuildReaction on the WAClient fake so the test package compiles"
```

---

### Task 1: Dependência meowcaller e a fronteira `internal/call/port.go`

Traz a lib para o módulo e define as interfaces que isolam o gateway dos tipos concretos dela. Tudo depois desta tarefa programa contra estas interfaces, nunca contra `*meowcaller.Call`.

**Files:**
- Modify: `go.mod`, `go.sum`
- Create: `internal/call/port.go`
- Test: `internal/call/port_test.go`

**Interfaces:**
- Consumes: nada
- Produces:

```go
package call

type GroupOptions struct {
	GroupJID string
	Video    bool
}

type Link struct {
	Token string
	URL   string
	Video bool
}

type LinkPreview struct {
	Token            string
	Video            bool
	ApprovalRequired bool
	IsAdmin          bool
	Creator          string
	CreatorPN        string
}

type Caller interface {
	Call(ctx context.Context, target string, video bool) (LiveCall, error)
	GroupCall(ctx context.Context, targets []string, opts GroupOptions) (LiveCall, error)
	GroupCallByID(ctx context.Context, groupID string, opts GroupOptions) (LiveCall, error)
	CreateCallLink(ctx context.Context, video bool) (Link, error)
	PreviewCallLink(ctx context.Context, tokenOrURL string, video bool) (LinkPreview, error)
	JoinCallLink(ctx context.Context, tokenOrURL string, video bool) (LiveCall, error)
	OnIncomingCall(fn func(LiveCall))
}

type LiveCall interface {
	ID() string
	Peer() string
	IsVideo() bool
	Answer() error
	Reject() error
	Hangup() error
	StartVideo() error
	AcceptVideo() error
	StopVideo() error
	SetVideoEnabled(enabled bool) error
	SetVideoOrientation(orientation int) error
	SendVideo(accessUnit []byte, duration time.Duration) error
	SendReaction(emoji string) error
	SetHandRaised(raised bool) error
	StartScreenShare(id *uint32) error
	StopScreenShare() error
	AddParticipant(ctx context.Context, target string) error
	RingParticipant(ctx context.Context, target string) error
	SetApprovalRequired(ctx context.Context, enabled bool) error
	AdmitParticipant(ctx context.Context, user string) error
	DenyParticipant(ctx context.Context, user string) error
	Play(src io.ReadCloser) error
	Receive(sink func(frame []float32))
	ReceiveVideo(sink func(accessUnit []byte))
	OnReady(fn func())
	OnEnd(fn func(reason string))
	OnStateChange(fn func(phase Phase))
	OnPeerAccept(fn func())
	OnMuteState(fn func(muted bool))
	OnVideoState(fn func(VideoState))
	OnReaction(fn func(Reaction))
	OnGroupState(fn func(GroupState))
	OnWaitingRoomState(fn func(WaitingRoom))
	OnHandRaise(fn func(HandState))
	OnScreenShare(fn func(ScreenShare))
}

type RecordingStore interface {
	PutStream(ctx context.Context, key, mime string, r io.Reader) error
}
```

`Play` recebe `io.ReadCloser` de PCM s16le mono 16 kHz — o adaptador embrulha com `meowcaller.PCMStream`. Decodificação de mp3/wav/opus fica no adaptador (Task 6), fora do núcleo testável.

- [ ] **Step 1: Adicionar a dependência**

```bash
go get github.com/purpshell/meowcaller@6d9b7b2
go mod tidy
```

- [ ] **Step 2: Verificar que o pin de whatsmeow não mudou**

Run: `grep 'go.mau.fi/whatsmeow ' go.mod`
Expected: `go.mau.fi/whatsmeow v0.0.0-20260722203353-e9a033b24933` — exatamente o valor anterior. Se mudou, pare e reporte antes de continuar.

- [ ] **Step 3: Verificar que o build sem CGO continua de pé**

Run: `CGO_ENABLED=0 go build ./...`
Expected: sucesso, sem erro de dependência nativa

- [ ] **Step 4: Escrever o teste que fixa a fronteira**

Crie `internal/call/port_test.go`. O teste garante que a interface é implementável por um fake do próprio pacote — é o que todas as tarefas seguintes vão usar:

```go
package call_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

// fakeCall implements call.LiveCall with recorded invocations.
type fakeCall struct {
	id       string
	peer     string
	video    bool
	actions  []string
	onEnd    func(string)
	audioIn  func([]float32)
	videoIn  func([]byte)
	videoOut [][]byte
}

func (f *fakeCall) ID() string     { return f.id }
func (f *fakeCall) Peer() string   { return f.peer }
func (f *fakeCall) IsVideo() bool  { return f.video }
func (f *fakeCall) Answer() error  { f.actions = append(f.actions, "answer"); return nil }
func (f *fakeCall) Reject() error  { f.actions = append(f.actions, "reject"); return nil }
func (f *fakeCall) Hangup() error  { f.actions = append(f.actions, "hangup"); return nil }
func (f *fakeCall) StartVideo() error  { f.actions = append(f.actions, "video.start"); return nil }
func (f *fakeCall) AcceptVideo() error { f.actions = append(f.actions, "video.accept"); return nil }
func (f *fakeCall) StopVideo() error   { f.actions = append(f.actions, "video.stop"); return nil }
func (f *fakeCall) SetVideoEnabled(bool) error      { f.actions = append(f.actions, "video.enable"); return nil }
func (f *fakeCall) SetVideoOrientation(int) error   { f.actions = append(f.actions, "video.orientation"); return nil }
func (f *fakeCall) SendVideo(au []byte, _ time.Duration) error { f.videoOut = append(f.videoOut, au); return nil }
func (f *fakeCall) SendReaction(string) error       { f.actions = append(f.actions, "reaction"); return nil }
func (f *fakeCall) SetHandRaised(bool) error        { f.actions = append(f.actions, "hand.raise"); return nil }
func (f *fakeCall) StartScreenShare(*uint32) error  { f.actions = append(f.actions, "screenshare.start"); return nil }
func (f *fakeCall) StopScreenShare() error          { f.actions = append(f.actions, "screenshare.stop"); return nil }
func (f *fakeCall) AddParticipant(context.Context, string) error  { f.actions = append(f.actions, "participant.add"); return nil }
func (f *fakeCall) RingParticipant(context.Context, string) error { f.actions = append(f.actions, "participant.ring"); return nil }
func (f *fakeCall) SetApprovalRequired(context.Context, bool) error { f.actions = append(f.actions, "approval.set"); return nil }
func (f *fakeCall) AdmitParticipant(context.Context, string) error  { f.actions = append(f.actions, "participant.admit"); return nil }
func (f *fakeCall) DenyParticipant(context.Context, string) error   { f.actions = append(f.actions, "participant.deny"); return nil }
func (f *fakeCall) Play(io.ReadCloser) error        { f.actions = append(f.actions, "play"); return nil }
func (f *fakeCall) Receive(sink func([]float32))    { f.audioIn = sink }
func (f *fakeCall) ReceiveVideo(sink func([]byte))  { f.videoIn = sink }
func (f *fakeCall) OnReady(func())                  {}
func (f *fakeCall) OnEnd(fn func(string))           { f.onEnd = fn }
func (f *fakeCall) OnStateChange(func(call.Phase))  {}
func (f *fakeCall) OnPeerAccept(func())             {}
func (f *fakeCall) OnMuteState(func(bool))          {}
func (f *fakeCall) OnVideoState(func(call.VideoState)) {}
func (f *fakeCall) OnReaction(func(call.Reaction))     {}
func (f *fakeCall) OnGroupState(func(call.GroupState)) {}
func (f *fakeCall) OnWaitingRoomState(func(call.WaitingRoom)) {}
func (f *fakeCall) OnHandRaise(func(call.HandState))   {}
func (f *fakeCall) OnScreenShare(func(call.ScreenShare)) {}

var _ call.LiveCall = (*fakeCall)(nil)

func TestFakeCallSatisfiesLiveCall(t *testing.T) {
	var c call.LiveCall = &fakeCall{id: "ABC"}
	if c.ID() != "ABC" {
		t.Fatalf("ID() = %q, want ABC", c.ID())
	}
}
```

- [ ] **Step 5: Rodar o teste e ver falhar**

Run: `go test ./internal/call/ -run TestFakeCallSatisfiesLiveCall -v`
Expected: FAIL — o pacote `call` ainda não existe

- [ ] **Step 6: Escrever `internal/call/port.go`**

Escreva exatamente as interfaces e structs do bloco "Produces" acima, com doc comments explicando **por que** a fronteira existe (os tipos concretos da meowcaller não são construíveis em teste sem uma stack VoIP real). Os tipos `Phase`, `VideoState`, `Reaction`, `GroupState`, `WaitingRoom`, `HandState`, `ScreenShare` são definidos na Task 2 — para esta tarefa compilar, declare-os em `contracts.go` já nesta etapa com os campos da Task 2.

- [ ] **Step 7: Rodar o teste e ver passar**

Run: `go test ./internal/call/ -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add go.mod go.sum internal/call/
git commit -m "feat(call): add the meowcaller dependency and the Caller/LiveCall port"
```

---

### Task 2: Contratos de comando e evento

Define o que trafega no broker e a normalização dos valores da lib. É código puro, sem I/O, e é o que o backend vai consumir.

**Files:**
- Create: `internal/call/contracts.go` (completar o que a Task 1 esboçou)
- Modify: `internal/amqp/contracts.go`
- Test: `internal/call/contracts_test.go`

**Interfaces:**
- Consumes: nada
- Produces:

```go
package call

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

func (p Phase) String() string  // idle|calling|ringing|connecting|active|ended|waiting_room

type VideoState struct {
	Active      bool
	Upgrade     bool
	Orientation int
	Raw         int
}

type Reaction struct {
	Emoji         string
	ParticipantID string
	Sender        string
	Removed       bool
}

type GroupParticipant struct {
	JID        string `json:"jid"`
	PN         string `json:"pn,omitempty"`
	State      string `json:"state,omitempty"`
	HandRaised bool   `json:"handRaised,omitempty"`
}

type GroupState struct {
	TransactionID uint32             `json:"transactionId"`
	Participants  []GroupParticipant `json:"participants,omitempty"`
}

type WaitingRoomUser struct {
	JID   string `json:"jid"`
	PN    string `json:"pn,omitempty"`
	State string `json:"state,omitempty"`
}

type WaitingRoom struct {
	Enabled       bool              `json:"enabled"`
	IsAdmin       bool              `json:"isAdmin,omitempty"`
	InWaitingRoom bool              `json:"inWaitingRoom,omitempty"`
	TransactionID uint32            `json:"transactionId,omitempty"`
	Users         []WaitingRoomUser `json:"users,omitempty"`
}

type HandState struct {
	Participant string `json:"participant"`
	Raised      bool   `json:"raised"`
}

type ScreenShare struct {
	Participant string `json:"participant"`
	Active      bool   `json:"active"`
	Version     uint32 `json:"version,omitempty"`
}

type Media struct {
	Key      string `json:"key"`
	MimeType string `json:"mimeType"`
	Filename string `json:"filename,omitempty"`
	Duration int    `json:"duration,omitempty"`
}

type EventVideoState struct {
	Active      bool `json:"active"`
	Upgrade     bool `json:"upgrade,omitempty"`
	Orientation int  `json:"orientation,omitempty"`
}

type EventError struct {
	Code   string `json:"code"`
	Reason string `json:"reason,omitempty"`
}

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

type EventReaction struct {
	Emoji   string `json:"emoji"`
	Sender  string `json:"sender,omitempty"`
	Removed bool   `json:"removed,omitempty"`
}

type EventLink struct {
	Token            string `json:"token"`
	URL              string `json:"url,omitempty"`
	Video            bool   `json:"video,omitempty"`
	ApprovalRequired bool   `json:"approvalRequired,omitempty"`
	IsAdmin          bool   `json:"isAdmin,omitempty"`
}
```

Tipos de evento, como constantes exportadas: `EventIncoming = "incoming"`, `EventRinging = "ringing"`, `EventAccepted = "accepted"`, `EventRejected = "rejected"`, `EventEnded = "ended"`, `EventState = "state"`, `EventVideo = "video.state"`, `EventReactionType = "reaction"`, `EventGroupState = "group.state"`, `EventWaitingRoom = "waitingroom.state"`, `EventHand = "hand"`, `EventScreenShare = "screenshare"`, `EventLinkCreated = "link.created"`, `EventLinkPreview = "link.preview"`, `EventCommandAck = "command.ack"`, `EventCommandFailed = "command.failed"`.

Direções: `DirectionInbound = "inbound"`, `DirectionOutbound = "outbound"`.

Em `internal/amqp/contracts.go`:

```go
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
```

- [ ] **Step 1: Escrever os testes**

Crie `internal/call/contracts_test.go`:

```go
package call_test

import (
	"encoding/json"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

func TestPhaseString(t *testing.T) {
	cases := map[call.Phase]string{
		call.PhaseIdle:        "idle",
		call.PhaseCalling:     "calling",
		call.PhaseRinging:     "ringing",
		call.PhaseConnecting:  "connecting",
		call.PhaseActive:      "active",
		call.PhaseEnded:       "ended",
		call.PhaseWaitingRoom: "waiting_room",
	}
	for phase, want := range cases {
		if got := phase.String(); got != want {
			t.Errorf("Phase(%d).String() = %q, want %q", phase, got, want)
		}
	}
}

func TestPhaseStringUnknown(t *testing.T) {
	if got := call.Phase(99).String(); got != "unknown" {
		t.Errorf("Phase(99).String() = %q, want unknown", got)
	}
}

// The event JSON is the backend's contract. Absent fields must stay absent so a
// consumer can tell "no recording" from "empty recording".
func TestEventOmitsAbsentFields(t *testing.T) {
	body, err := json.Marshal(call.Event{
		PhoneNumberID: "5511999999999",
		TenantID:      "t1",
		ChannelID:     "c1",
		CallID:        "ABCDEF",
		Direction:     call.DirectionInbound,
		Type:          call.EventIncoming,
		Timestamp:     "1754300000",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(body)
	want := `{"phoneNumberId":"5511999999999","tenantId":"t1","channelId":"c1",` +
		`"callId":"ABCDEF","direction":"inbound","type":"incoming","timestamp":"1754300000"}`
	if got != want {
		t.Errorf("marshal =\n%s\nwant\n%s", got, want)
	}
}

// media carries an S3 key, never a URL: the backend resolves it exactly like it
// already resolves inbound message media.
func TestEventEndedCarriesBothRecordings(t *testing.T) {
	body, err := json.Marshal(call.Event{
		CallID:     "ABCDEF",
		Direction:  call.DirectionInbound,
		Type:       call.EventEnded,
		Timestamp:  "1754300100",
		Duration:   100,
		Media:      &call.Media{Key: "calls/c1/ABCDEF.wav", MimeType: "audio/wav"},
		VideoMedia: &call.Media{Key: "calls/c1/ABCDEF.h264", MimeType: "video/h264"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	media, ok := out["media"].(map[string]any)
	if !ok || media["key"] != "calls/c1/ABCDEF.wav" {
		t.Errorf("media = %v, want the wav key", out["media"])
	}
	videoMedia, ok := out["videoMedia"].(map[string]any)
	if !ok || videoMedia["key"] != "calls/c1/ABCDEF.h264" {
		t.Errorf("videoMedia = %v, want the h264 key", out["videoMedia"])
	}
	if _, exists := out["url"]; exists {
		t.Error("event must not carry a resolved URL, only the S3 key")
	}
}
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `go test ./internal/call/ -v`
Expected: FAIL — `Phase.String`, as constantes de evento e os campos ainda não existem

- [ ] **Step 3: Implementar `contracts.go`**

Escreva os tipos, as constantes e `Phase.String()` conforme o bloco "Produces". `String()` retorna `"unknown"` para valor fora da faixa — nunca panica, porque a lib pode ganhar fases novas.

- [ ] **Step 4: Adicionar `GatewayCallCommand`**

Acrescente o struct em `internal/amqp/contracts.go`, ao lado de `GatewaySendCommand`.

- [ ] **Step 5: Rodar e ver passar**

Run: `go test ./internal/call/ ./internal/amqp/ -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/call/contracts.go internal/call/contracts_test.go internal/amqp/contracts.go
git commit -m "feat(call): define the call command and event contracts"
```

---

### Task 3: `PutStream` no S3 e o gravador

Gravação vai para arquivo temporário e sobe em streaming. Um WAV de 30 min a 16 kHz mono s16 tem ~57 MB; carregar isso em memória por chamada não escala, e o `Put` atual só aceita `[]byte`.

**Files:**
- Modify: `internal/media/s3.go`
- Create: `internal/call/recorder.go`
- Test: `internal/call/recorder_test.go`, `test/media_s3_stream_test.go`

**Interfaces:**
- Consumes: `call.RecordingStore` (Task 1)
- Produces:

```go
// internal/media
func (s *S3Store) PutStream(ctx context.Context, key, mime string, r io.Reader) error

// internal/call
type Recorder struct{ ... }

// NewRecorder opens the temp files for a call. Both recorders are lazy: no
// file is created until the first frame arrives, so a call with no audio or
// no video leaves no object behind.
func NewRecorder(tmpDir, channelID, callID string) *Recorder

func (r *Recorder) WriteAudio(frame []float32)
func (r *Recorder) WriteVideo(accessUnit []byte)

// Finish closes the temp files, uploads whatever was captured and returns the
// resulting media descriptors. Either may be nil when nothing was captured.
// The temp files are always removed, upload error or not.
func (r *Recorder) Finish(ctx context.Context, store RecordingStore) (audio *Media, video *Media, err error)
```

Keys: `calls/<channelID>/<callID>.wav` (`audio/wav`) e `calls/<channelID>/<callID>.h264` (`video/h264`).

- [ ] **Step 1: Escrever o teste do gravador**

Crie `internal/call/recorder_test.go`:

```go
package call_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

type memStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	mimes   map[string]string
	err     error
}

func newMemStore() *memStore {
	return &memStore{objects: map[string][]byte{}, mimes: map[string]string{}}
}

func (m *memStore) PutStream(_ context.Context, key, mime string, r io.Reader) error {
	if m.err != nil {
		return m.err
	}
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = body
	m.mimes[key] = mime
	return nil
}

func TestRecorderWritesCanonicalWAV(t *testing.T) {
	dir := t.TempDir()
	rec := call.NewRecorder(dir, "chan1", "CALL1")

	// Full-scale positive, silence, full-scale negative.
	rec.WriteAudio([]float32{1, 0, -1})

	store := newMemStore()
	audio, video, err := rec.Finish(context.Background(), store)
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if video != nil {
		t.Errorf("video = %+v, want nil when no video frame was written", video)
	}
	if audio == nil || audio.Key != "calls/chan1/CALL1.wav" || audio.MimeType != "audio/wav" {
		t.Fatalf("audio = %+v, want the wav key and mime", audio)
	}

	body := store.objects["calls/chan1/CALL1.wav"]
	if len(body) != 44+6 {
		t.Fatalf("len(wav) = %d, want 50 (44-byte header + 3 samples)", len(body))
	}
	if string(body[0:4]) != "RIFF" || string(body[8:12]) != "WAVE" {
		t.Errorf("header = %q, want a RIFF/WAVE header", body[0:12])
	}
	if rate := binary.LittleEndian.Uint32(body[24:28]); rate != 16000 {
		t.Errorf("sample rate = %d, want 16000", rate)
	}
	if ch := binary.LittleEndian.Uint16(body[22:24]); ch != 1 {
		t.Errorf("channels = %d, want 1 (mono)", ch)
	}
	if dataLen := binary.LittleEndian.Uint32(body[40:44]); dataLen != 6 {
		t.Errorf("data chunk size = %d, want 6", dataLen)
	}
	samples := []int16{
		int16(binary.LittleEndian.Uint16(body[44:46])),
		int16(binary.LittleEndian.Uint16(body[46:48])),
		int16(binary.LittleEndian.Uint16(body[48:50])),
	}
	want := []int16{32767, 0, -32767}
	for i := range want {
		if samples[i] != want[i] {
			t.Errorf("sample[%d] = %d, want %d", i, samples[i], want[i])
		}
	}
}

// float32 outside [-1,1] must clamp, not wrap around into the opposite sign.
func TestRecorderClampsOutOfRangeSamples(t *testing.T) {
	dir := t.TempDir()
	rec := call.NewRecorder(dir, "chan1", "CALL1")
	rec.WriteAudio([]float32{2.5, -2.5})

	store := newMemStore()
	if _, _, err := rec.Finish(context.Background(), store); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	body := store.objects["calls/chan1/CALL1.wav"]
	first := int16(binary.LittleEndian.Uint16(body[44:46]))
	second := int16(binary.LittleEndian.Uint16(body[46:48]))
	if first != 32767 || second != -32767 {
		t.Errorf("samples = %d,%d, want 32767,-32767", first, second)
	}
}

func TestRecorderWritesVideoVerbatim(t *testing.T) {
	dir := t.TempDir()
	rec := call.NewRecorder(dir, "chan1", "CALL1")
	rec.WriteVideo([]byte{0, 0, 0, 1, 0x67, 0x42})
	rec.WriteVideo([]byte{0, 0, 0, 1, 0x65, 0x88})

	store := newMemStore()
	audio, video, err := rec.Finish(context.Background(), store)
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if audio != nil {
		t.Errorf("audio = %+v, want nil when no audio frame was written", audio)
	}
	if video == nil || video.Key != "calls/chan1/CALL1.h264" || video.MimeType != "video/h264" {
		t.Fatalf("video = %+v, want the h264 key and mime", video)
	}
	want := []byte{0, 0, 0, 1, 0x67, 0x42, 0, 0, 0, 1, 0x65, 0x88}
	if got := store.objects["calls/chan1/CALL1.h264"]; !bytes.Equal(got, want) {
		t.Errorf("h264 = % x, want % x", got, want)
	}
}

// A call that captured nothing must not leave an empty object in the bucket.
func TestRecorderUploadsNothingWhenNoFrames(t *testing.T) {
	dir := t.TempDir()
	rec := call.NewRecorder(dir, "chan1", "CALL1")

	store := newMemStore()
	audio, video, err := rec.Finish(context.Background(), store)
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if audio != nil || video != nil {
		t.Errorf("audio=%+v video=%+v, want both nil", audio, video)
	}
	if len(store.objects) != 0 {
		t.Errorf("objects = %v, want none", store.objects)
	}
}

// The temp file must go away even when the upload fails, or a long-running
// gateway fills its disk with dead recordings.
func TestRecorderRemovesTempFilesOnUploadError(t *testing.T) {
	dir := t.TempDir()
	rec := call.NewRecorder(dir, "chan1", "CALL1")
	rec.WriteAudio([]float32{0.5})

	store := newMemStore()
	store.err = errors.New("bucket is on fire")
	if _, _, err := rec.Finish(context.Background(), store); err == nil {
		t.Fatal("Finish err = nil, want the upload error")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, filepath.Join(dir, e.Name()))
		}
		t.Errorf("leftover temp files: %v", names)
	}
}
```

Adicione os imports `errors` e `io` ao arquivo de teste.

- [ ] **Step 2: Rodar e ver falhar**

Run: `go test ./internal/call/ -run TestRecorder -v`
Expected: FAIL — `call.NewRecorder` não existe

- [ ] **Step 3: Implementar `internal/call/recorder.go`**

Pontos obrigatórios da implementação:

- O header WAV é escrito com `dataLen` zerado na criação e reescrito com o valor real em `Finish`, via `f.Seek(0, io.SeekStart)` antes do upload. Mesma estratégia do `wavRecorder` da lib, mas em arquivo que nós controlamos.
- Conversão float32 → s16: `int16(max(-1, min(1, v)) * 32767)`. Clamp antes de multiplicar.
- `WriteAudio`/`WriteVideo` são chamados das goroutines de mídia da lib: protegidos por mutex e **nunca** retornam erro para a lib. Erro de escrita é registrado no `Recorder` e devolvido em `Finish`.
- Antes de subir, `Seek(0)` e passa o `*os.File` como `io.Reader` para `PutStream`.
- `defer os.Remove(path)` para cada arquivo, executado independentemente do resultado do upload.

- [ ] **Step 4: Rodar e ver passar**

Run: `go test ./internal/call/ -run TestRecorder -v`
Expected: PASS

- [ ] **Step 5: Implementar `PutStream` em `internal/media/s3.go`**

```go
// PutStream uploads r under key without buffering it in memory. Call recordings
// are hundreds of megabytes for a long call; the []byte-taking Put would hold
// the whole thing in RAM.
func (s *S3Store) PutStream(ctx context.Context, key, mime string, r io.Reader) error {
	uploader := manager.NewUploader(s.client)
	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        r,
		ContentType: aws.String(mime),
	})
	if err != nil {
		return fmt.Errorf("media: put stream %s: %w", key, err)
	}
	return nil
}
```

```bash
go get github.com/aws/aws-sdk-go-v2/feature/s3/manager
```

Import: `"github.com/aws/aws-sdk-go-v2/feature/s3/manager"`.

- [ ] **Step 6: Escrever o teste de integração do `PutStream`**

Crie `test/media_s3_stream_test.go`. Um payload de 6 MB passa do limite de parte única e força o caminho multipart do uploader, que é justamente o que o `Put` atual nunca exercita:

```go
package test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/w3nder/whatsmeow-gateway/internal/media"
)

func TestS3StorePutStreamRoundTrip(t *testing.T) {
	ctx := context.Background()

	container, err := minio.Run(ctx, minioImage)
	if err != nil {
		t.Fatalf("failed to start minio container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Errorf("failed to terminate minio container: %v", err)
		}
	})

	endpoint, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	const bucket = "vectax-calls-test"
	rawS3, err := rawS3Client(ctx, endpoint, container.Username, container.Password)
	if err != nil {
		t.Fatalf("failed to build raw s3 client: %v", err)
	}
	if _, err := rawS3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	store, err := media.NewS3Store(ctx, media.S3Config{
		Bucket:          bucket,
		Region:          "us-east-1",
		Endpoint:        "http://" + endpoint,
		AccessKeyID:     container.Username,
		SecretAccessKey: container.Password,
	})
	if err != nil {
		t.Fatalf("NewS3Store failed: %v", err)
	}

	// 6 MB is past the uploader's single-part threshold, so this exercises the
	// multipart path a call recording will actually take.
	data := bytes.Repeat([]byte("meowcaller-recording"), 6*1024*1024/20)
	key := "calls/channel-1/CALL1.wav"

	if err := store.PutStream(ctx, key, "audio/wav", bytes.NewReader(data)); err != nil {
		t.Fatalf("PutStream failed: %v", err)
	}

	obj, err := rawS3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer func() {
		if err := obj.Body.Close(); err != nil {
			t.Errorf("failed to close object body: %v", err)
		}
	}()

	got, err := io.ReadAll(obj.Body)
	if err != nil {
		t.Fatalf("failed to read object body: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("body length %d, want %d", len(got), len(data))
	}
	if aws.ToString(obj.ContentType) != "audio/wav" {
		t.Fatalf("ContentType = %q, want audio/wav", aws.ToString(obj.ContentType))
	}
}
```

- [ ] **Step 7: Rodar os testes**

Run: `go test ./internal/... && go test ./test/ -run TestS3PutStream -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add internal/media/s3.go internal/call/recorder.go internal/call/recorder_test.go test/media_s3_stream_test.go go.mod go.sum
git commit -m "feat(call): record call audio to WAV and video to Annex-B, streamed to S3"
```

---

### Task 4: Registry e Manager — callbacks viram eventos

O coração da tarefa: assinar todos os callbacks da lib e traduzi-los em `call.Event` publicado. Estado por canal, com teardown que não deixa chamada órfã.

**Files:**
- Create: `internal/call/registry.go`, `internal/call/manager.go`
- Test: `internal/call/registry_test.go`, `internal/call/manager_test.go`

**Interfaces:**
- Consumes: `Caller`, `LiveCall`, `RecordingStore` (Task 1); `Event` e constantes (Task 2); `Recorder` (Task 3)
- Produces:

```go
type Publisher interface {
	PublishCall(ctx context.Context, evt Event) error
}

type Identity struct {
	PhoneNumberID string
	TenantID      string
}

type Options struct {
	TmpDir      string
	Record      bool
	Now         func() time.Time // injected for tests; defaults to time.Now
}

func NewManager(pub Publisher, store RecordingStore, identity func(channelID string) Identity, opts Options, log *slog.Logger) *Manager

// Attach subscribes to the channel's incoming calls. Called once per live session.
func (m *Manager) Attach(channelID string, caller Caller)

// Track wires every callback on an already-obtained call (used by Attach for
// inbound calls and by the command dispatcher for outbound ones) and registers
// it. Returns the tracked call.
func (m *Manager) Track(channelID string, lc LiveCall, direction string, record bool) *Tracked

// Get returns a live call, or false.
func (m *Manager) Get(channelID, callID string) (*Tracked, bool)

// AbortChannel ends every live call on a channel, flushing recordings and
// publishing an "ended" event with the given reason. Called on LoggedOut,
// on session drop and on shutdown.
func (m *Manager) AbortChannel(ctx context.Context, channelID, reason string)

type Tracked struct {
	CallID    string
	ChannelID string
	Direction string
	Peer      string
	// Live is the underlying call, for the command dispatcher.
	Live LiveCall
}
```

- [ ] **Step 1: Escrever o teste do registry**

Crie `internal/call/registry_test.go`:

```go
package call_test

import (
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

func TestRegistryIsolatesChannels(t *testing.T) {
	r := call.NewRegistry()
	a := &call.Tracked{CallID: "C1", ChannelID: "chan-a"}
	b := &call.Tracked{CallID: "C1", ChannelID: "chan-b"}

	r.Insert(a)
	r.Insert(b)

	// The same call-id on two channels must not collide.
	if got, ok := r.Get("chan-a", "C1"); !ok || got != a {
		t.Errorf("Get(chan-a, C1) = %v,%v, want the chan-a call", got, ok)
	}
	if got, ok := r.Get("chan-b", "C1"); !ok || got != b {
		t.Errorf("Get(chan-b, C1) = %v,%v, want the chan-b call", got, ok)
	}
}

func TestRegistryTakeChannelEmptiesIt(t *testing.T) {
	r := call.NewRegistry()
	r.Insert(&call.Tracked{CallID: "C1", ChannelID: "chan-a"})
	r.Insert(&call.Tracked{CallID: "C2", ChannelID: "chan-a"})
	r.Insert(&call.Tracked{CallID: "C3", ChannelID: "chan-b"})

	taken := r.TakeChannel("chan-a")
	if len(taken) != 2 {
		t.Fatalf("TakeChannel returned %d calls, want 2", len(taken))
	}
	if _, ok := r.Get("chan-a", "C1"); ok {
		t.Error("chan-a still has C1 after TakeChannel")
	}
	if _, ok := r.Get("chan-b", "C3"); !ok {
		t.Error("TakeChannel(chan-a) removed a call from chan-b")
	}
}

func TestRegistryRemoveIsIdempotent(t *testing.T) {
	r := call.NewRegistry()
	r.Insert(&call.Tracked{CallID: "C1", ChannelID: "chan-a"})

	if !r.Remove("chan-a", "C1") {
		t.Error("first Remove = false, want true")
	}
	if r.Remove("chan-a", "C1") {
		t.Error("second Remove = true, want false")
	}
}
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `go test ./internal/call/ -run TestRegistry -v`
Expected: FAIL — `call.NewRegistry` não existe

- [ ] **Step 3: Implementar `internal/call/registry.go`**

`Registry` com `sync.Mutex` e `map[string]map[string]*Tracked` (channelID → callID → chamada). Métodos: `NewRegistry`, `Insert`, `Get`, `Remove` (bool: existia), `TakeChannel` (remove e devolve todas do canal, para teardown atômico).

- [ ] **Step 4: Rodar e ver passar**

Run: `go test ./internal/call/ -run TestRegistry -v`
Expected: PASS

- [ ] **Step 5: Escrever o teste do manager**

Crie `internal/call/manager_test.go`:

```go
package call_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

type memPublisher struct {
	mu     sync.Mutex
	events []call.Event
}

func (p *memPublisher) PublishCall(_ context.Context, evt call.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, evt)
	return nil
}

func (p *memPublisher) typed(t string) []call.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []call.Event
	for _, e := range p.events {
		if e.Type == t {
			out = append(out, e)
		}
	}
	return out
}

// fakeCaller implements call.Caller, exposing the registered incoming handler.
type fakeCaller struct {
	onIncoming func(call.LiveCall)
	placed     *fakeCall
}

func (f *fakeCaller) Call(_ context.Context, target string, video bool) (call.LiveCall, error) {
	f.placed = &fakeCall{id: "OUT1", peer: target, video: video}
	return f.placed, nil
}
func (f *fakeCaller) GroupCall(context.Context, []string, call.GroupOptions) (call.LiveCall, error) {
	f.placed = &fakeCall{id: "GRP1"}
	return f.placed, nil
}
func (f *fakeCaller) GroupCallByID(context.Context, string, call.GroupOptions) (call.LiveCall, error) {
	f.placed = &fakeCall{id: "GRP2"}
	return f.placed, nil
}
func (f *fakeCaller) CreateCallLink(context.Context, bool) (call.Link, error) {
	return call.Link{Token: "TOK", URL: "https://call.whatsapp.com/voice/TOK"}, nil
}
func (f *fakeCaller) PreviewCallLink(context.Context, string, bool) (call.LinkPreview, error) {
	return call.LinkPreview{Token: "TOK", ApprovalRequired: true}, nil
}
func (f *fakeCaller) JoinCallLink(context.Context, string, bool) (call.LiveCall, error) {
	f.placed = &fakeCall{id: "LINK1"}
	return f.placed, nil
}
func (f *fakeCaller) OnIncomingCall(fn func(call.LiveCall)) { f.onIncoming = fn }

var _ call.Caller = (*fakeCaller)(nil)

func newTestManager(t *testing.T, pub call.Publisher, store call.RecordingStore, now func() time.Time) *call.Manager {
	t.Helper()
	return call.NewManager(pub, store,
		func(string) call.Identity {
			return call.Identity{PhoneNumberID: "5511999999999", TenantID: "t1"}
		},
		call.Options{TmpDir: t.TempDir(), Record: true, Now: now},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}

func TestManagerPublishesIncomingCall(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	caller.onIncoming(&fakeCall{id: "C1", peer: "5511888888888@s.whatsapp.net", video: true})

	incoming := pub.typed(call.EventIncoming)
	if len(incoming) != 1 {
		t.Fatalf("got %d incoming events, want 1", len(incoming))
	}
	evt := incoming[0]
	if evt.CallID != "C1" {
		t.Errorf("CallID = %q, want C1", evt.CallID)
	}
	if evt.Direction != call.DirectionInbound {
		t.Errorf("Direction = %q, want inbound", evt.Direction)
	}
	if !evt.IsVideo {
		t.Error("IsVideo = false, want true for a video offer")
	}
	if evt.TenantID != "t1" || evt.PhoneNumberID != "5511999999999" {
		t.Errorf("identity = %q/%q, want t1/5511999999999", evt.TenantID, evt.PhoneNumberID)
	}
	if _, ok := m.Get("chan-a", "C1"); !ok {
		t.Error("incoming call was not registered")
	}
}

// The end event has to carry the real talk time, measured from the answer, not
// from the offer — a call that rang for 40s and talked for 10s is a 10s call.
func TestManagerEndedCarriesAnsweredDuration(t *testing.T) {
	pub := &memPublisher{}
	clock := &fakeClock{now: time.Unix(1_754_300_000, 0)}
	m := newTestManager(t, pub, newMemStore(), clock.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1", peer: "5511888888888@s.whatsapp.net"}
	caller.onIncoming(lc)

	clock.advance(40 * time.Second) // ringing
	lc.fireReady()
	clock.advance(10 * time.Second) // talking
	lc.fireEnd("hangup")

	ended := pub.typed(call.EventEnded)
	if len(ended) != 1 {
		t.Fatalf("got %d ended events, want 1", len(ended))
	}
	if ended[0].Duration != 10 {
		t.Errorf("Duration = %d, want 10 (answered to end, not offer to end)", ended[0].Duration)
	}
	if ended[0].Reason != "hangup" {
		t.Errorf("Reason = %q, want hangup", ended[0].Reason)
	}
	if _, ok := m.Get("chan-a", "C1"); ok {
		t.Error("call still registered after ending")
	}
}

func TestManagerEndedCarriesRecording(t *testing.T) {
	pub := &memPublisher{}
	store := newMemStore()
	m := newTestManager(t, pub, store, time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1", peer: "5511888888888@s.whatsapp.net"}
	caller.onIncoming(lc)
	lc.fireReady()
	lc.audioIn([]float32{0.5, -0.5})
	lc.fireEnd("hangup")

	ended := pub.typed(call.EventEnded)[0]
	if ended.Media == nil || ended.Media.Key != "calls/chan-a/C1.wav" {
		t.Fatalf("Media = %+v, want the wav recording key", ended.Media)
	}
	if _, ok := store.objects["calls/chan-a/C1.wav"]; !ok {
		t.Error("recording was never uploaded")
	}
}

// Losing the recording must not hide the end of the call from the backend.
func TestManagerPublishesEndedEvenWhenUploadFails(t *testing.T) {
	pub := &memPublisher{}
	store := newMemStore()
	store.err = errors.New("bucket is on fire")
	m := newTestManager(t, pub, store, time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.onIncoming(lc)
	lc.fireReady()
	lc.audioIn([]float32{0.5})
	lc.fireEnd("hangup")

	ended := pub.typed(call.EventEnded)
	if len(ended) != 1 {
		t.Fatalf("got %d ended events, want 1", len(ended))
	}
	if ended[0].Media != nil {
		t.Errorf("Media = %+v, want nil when the upload failed", ended[0].Media)
	}
	failed := pub.typed(call.EventCommandFailed)
	if len(failed) != 1 || failed[0].Error == nil || failed[0].Error.Code != "recording_upload_failed" {
		t.Errorf("failures = %+v, want one recording_upload_failed", failed)
	}
}

func TestManagerAbortChannelEndsLiveCalls(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	lc := &fakeCall{id: "C1"}
	caller.onIncoming(lc)
	lc.fireReady()

	m.AbortChannel(context.Background(), "chan-a", "disconnected")

	ended := pub.typed(call.EventEnded)
	if len(ended) != 1 || ended[0].Reason != "disconnected" {
		t.Fatalf("ended = %+v, want one ended with reason disconnected", ended)
	}
	if _, ok := m.Get("chan-a", "C1"); ok {
		t.Error("call still registered after AbortChannel")
	}
}

// A panicking library callback must not take the gateway down with it.
func TestManagerSurvivesPanickingPublish(t *testing.T) {
	m := newTestManager(t, &panicPublisher{}, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	// Must not panic.
	caller.onIncoming(&fakeCall{id: "C1"})
}
```

Acrescente ao `port_test.go` os disparadores que o fake precisa, e o relógio falso:

```go
func (f *fakeCall) fireReady()             { if f.onReady != nil { f.onReady() } }
func (f *fakeCall) fireEnd(reason string)  { if f.onEnd != nil { f.onEnd(reason) } }
func (f *fakeCall) fireVideoState(v call.VideoState) { if f.onVideoState != nil { f.onVideoState(v) } }
func (f *fakeCall) fireReaction(r call.Reaction)     { if f.onReaction != nil { f.onReaction(r) } }

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type panicPublisher struct{}

func (panicPublisher) PublishCall(context.Context, call.Event) error { panic("boom") }
```

Guarde `onReady`, `onVideoState`, `onReaction`, `onGroupState`, `onWaitingRoom`, `onHand`, `onScreenShare`, `onState`, `onPeerAccept`, `onMute` nos campos do `fakeCall` e atribua nos respectivos `On...`.

- [ ] **Step 6: Rodar e ver falhar**

Run: `go test ./internal/call/ -run TestManager -v`
Expected: FAIL — `call.NewManager` não existe

- [ ] **Step 7: Implementar `internal/call/manager.go`**

Pontos obrigatórios:

- `Attach` chama `caller.OnIncomingCall` com um handler que faz `Track(channelID, lc, DirectionInbound, opts.Record)` e publica `EventIncoming`.
- `Track` assina **todos** os callbacks e liga os sinks quando `record` é verdadeiro: `lc.Receive(rec.WriteAudio)` e `lc.ReceiveVideo(rec.WriteVideo)`.
- `OnReady` marca `answeredAt` e publica `EventAccepted`. `OnPeerAccept` publica `EventAccepted` só para chamada de saída, e apenas uma vez — guarde um `sync.Once` por chamada para não duplicar quando `OnReady` e `OnPeerAccept` dispararem juntos.
- `OnEnd` faz o encerramento: `Finish` no gravador, publica `EventEnded` com `Duration` medido de `answeredAt` (zero se nunca atendeu), anexa `Media`/`VideoMedia`, e remove do registry. Erro de upload publica `EventCommandFailed` com código `recording_upload_failed` **depois** do `ended`.
- Toda publicação passa por um helper `publish` que recupera de pânico, loga com `channel_id`/`call_id` e segue. Callback de lib rodando na goroutine de mídia não pode derrubar o processo.
- `AbortChannel` usa `registry.TakeChannel`, e para cada chamada faz o mesmo encerramento do `OnEnd` com o `reason` recebido, sem depender de a lib disparar callback.

- [ ] **Step 8: Rodar e ver passar**

Run: `go test ./internal/call/ -v`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add internal/call/registry.go internal/call/manager.go internal/call/registry_test.go internal/call/manager_test.go internal/call/port_test.go
git commit -m "feat(call): track live calls and translate library callbacks into events"
```

---

### Task 5: Despacho de comandos

Traduz `GatewayCallCommand` em ação. Uma tabela de despacho, uma função por ação.

**Files:**
- Create: `internal/call/command.go`
- Test: `internal/call/command_test.go`

**Interfaces:**
- Consumes: `Manager`, `Caller`, `LiveCall`, `GatewayCallCommand`
- Produces:

```go
// Errors carried in the command.failed event's Error.Code.
const (
	CodeCallNotFound   = "call_not_found"
	CodeUnknownAction  = "unknown_action"
	CodeInvalidTarget  = "invalid_target"
	CodeActionFailed   = "action_failed"
	CodeNoCaller       = "no_caller"
	CodeMediaFetch     = "media_fetch_failed"
)

// MediaFetcher fetches the bytes behind a command's mediaUrl.
type MediaFetcher func(ctx context.Context, url string) ([]byte, error)

// Dispatch executes cmd against the channel's caller. It always publishes either a
// command.ack or a command.failed carrying cmd.CommandID, and returns an error only
// when the failure is the caller's to retry (never for a bad command).
func (m *Manager) Dispatch(ctx context.Context, caller Caller, cmd amqp.GatewayCallCommand, fetch MediaFetcher) error
```

Mapa ação → efeito:

| Action | Efeito |
|---|---|
| `dial` | `caller.Call(ctx, cmd.To, cmd.Video)` → `Track(..., DirectionOutbound, record)`, publica `ringing` |
| `group.dial` | `caller.GroupCall(ctx, cmd.Targets, GroupOptions{GroupJID: cmd.GroupID, Video: cmd.Video})` |
| `group.dial_by_id` | `caller.GroupCallByID(ctx, cmd.GroupID, GroupOptions{Video: cmd.Video})` |
| `answer` | `lc.Answer()` |
| `reject` | `lc.Reject()` |
| `hangup` | `lc.Hangup()` |
| `play` | busca `cmd.MediaURL`, decodifica para PCM e chama `lc.Play` |
| `video.start` | `lc.StartVideo()` |
| `video.accept` | `lc.AcceptVideo()` |
| `video.stop` | `lc.StopVideo()` |
| `video.enable` | `lc.SetVideoEnabled(cmd.Enabled)` |
| `video.orientation` | `lc.SetVideoOrientation(cmd.Orientation)` |
| `video.play` | busca `cmd.MediaURL`, fatia Annex-B (Task 7) e envia frame a frame |
| `reaction` | `lc.SendReaction(cmd.Emoji)` |
| `hand.raise` | `lc.SetHandRaised(cmd.Raised)` |
| `screenshare.start` | `lc.StartScreenShare(nil)` |
| `screenshare.stop` | `lc.StopScreenShare()` |
| `participant.add` | `lc.AddParticipant(ctx, cmd.Participant)` |
| `participant.ring` | `lc.RingParticipant(ctx, cmd.Participant)` |
| `approval.set` | `lc.SetApprovalRequired(ctx, cmd.Enabled)` |
| `participant.admit` | `lc.AdmitParticipant(ctx, cmd.Participant)` |
| `participant.deny` | `lc.DenyParticipant(ctx, cmd.Participant)` |
| `link.create` | `caller.CreateCallLink(ctx, cmd.Video)` → publica `link.created` |
| `link.preview` | `caller.PreviewCallLink(ctx, cmd.LinkToken, cmd.Video)` → publica `link.preview` |
| `link.join` | `caller.JoinCallLink(ctx, cmd.LinkToken, cmd.Video)` → `Track(..., DirectionOutbound, record)` |

- [ ] **Step 1: Escrever os testes**

Crie `internal/call/command_test.go`:

```go
package call_test

import (
	"context"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

func noFetch(context.Context, string) ([]byte, error) { return nil, nil }

func TestDispatchRoutesActionsToTheCall(t *testing.T) {
	cases := []struct {
		action string
		want   string
	}{
		{"answer", "answer"},
		{"reject", "reject"},
		{"hangup", "hangup"},
		{"video.start", "video.start"},
		{"video.accept", "video.accept"},
		{"video.stop", "video.stop"},
		{"video.enable", "video.enable"},
		{"video.orientation", "video.orientation"},
		{"reaction", "reaction"},
		{"hand.raise", "hand.raise"},
		{"screenshare.start", "screenshare.start"},
		{"screenshare.stop", "screenshare.stop"},
		{"participant.add", "participant.add"},
		{"participant.ring", "participant.ring"},
		{"approval.set", "approval.set"},
		{"participant.admit", "participant.admit"},
		{"participant.deny", "participant.deny"},
	}
	for _, tc := range cases {
		t.Run(tc.action, func(t *testing.T) {
			pub := &memPublisher{}
			m := newTestManager(t, pub, newMemStore(), time.Now)
			caller := &fakeCaller{}
			m.Attach("chan-a", caller)
			lc := &fakeCall{id: "C1"}
			caller.onIncoming(lc)

			err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
				ChannelID: "chan-a", CallID: "C1", CommandID: "cmd-1", Action: tc.action,
			}, noFetch)
			if err != nil {
				t.Fatalf("Dispatch: %v", err)
			}
			if len(lc.actions) != 1 || lc.actions[0] != tc.want {
				t.Errorf("actions = %v, want [%s]", lc.actions, tc.want)
			}
			if acks := pub.typed(call.EventCommandAck); len(acks) != 1 || acks[0].CommandID != "cmd-1" {
				t.Errorf("acks = %+v, want one carrying cmd-1", acks)
			}
		})
	}
}

// An unknown call-id must fail loudly and must not be retried: requeueing would
// loop until the DLQ.
func TestDispatchUnknownCallFails(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CallID: "NOPE", CommandID: "cmd-1", Action: "hangup",
	}, noFetch)
	if err != nil {
		t.Fatalf("Dispatch returned %v, want nil (the failure is reported as an event, not retried)", err)
	}
	failed := pub.typed(call.EventCommandFailed)
	if len(failed) != 1 || failed[0].Error == nil || failed[0].Error.Code != call.CodeCallNotFound {
		t.Errorf("failures = %+v, want one call_not_found", failed)
	}
}

func TestDispatchUnknownActionFails(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	caller.onIncoming(&fakeCall{id: "C1"})

	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CallID: "C1", CommandID: "cmd-1", Action: "teleport",
	}, noFetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	failed := pub.typed(call.EventCommandFailed)
	if len(failed) != 1 || failed[0].Error.Code != call.CodeUnknownAction {
		t.Errorf("failures = %+v, want one unknown_action", failed)
	}
}

func TestDispatchDialTracksTheOutboundCall(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)

	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CommandID: "cmd-1", Action: "dial",
		To: "+5511888888888", Video: true,
	}, noFetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	tracked, ok := m.Get("chan-a", "OUT1")
	if !ok {
		t.Fatal("outbound call was not registered")
	}
	if tracked.Direction != call.DirectionOutbound {
		t.Errorf("Direction = %q, want outbound", tracked.Direction)
	}
	if ringing := pub.typed(call.EventRinging); len(ringing) != 1 || !ringing[0].IsVideo {
		t.Errorf("ringing = %+v, want one video ringing event", ringing)
	}
}

func TestDispatchDialWithoutTargetFails(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}

	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CommandID: "cmd-1", Action: "dial",
	}, noFetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	failed := pub.typed(call.EventCommandFailed)
	if len(failed) != 1 || failed[0].Error.Code != call.CodeInvalidTarget {
		t.Errorf("failures = %+v, want one invalid_target", failed)
	}
}

func TestDispatchLinkCreatePublishesTheToken(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}

	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CommandID: "cmd-1", Action: "link.create", Video: true,
	}, noFetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	created := pub.typed(call.EventLinkCreated)
	if len(created) != 1 || created[0].Link == nil || created[0].Link.Token != "TOK" {
		t.Errorf("link.created = %+v, want the TOK link", created)
	}
}
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `go test ./internal/call/ -run TestDispatch -v`
Expected: FAIL — `Dispatch` não existe

- [ ] **Step 3: Implementar `internal/call/command.go`**

Um `switch` sobre `cmd.Action`. Ações que precisam de chamada viva resolvem primeiro por `m.Get(cmd.ChannelID, cmd.CallID)` e falham com `CodeCallNotFound` se não acharem. Ações de discagem e de link não exigem `callId`. Sucesso publica `command.ack`; falha publica `command.failed`. `Dispatch` retorna `error` não-nulo **apenas** quando publicar o próprio evento falhou — comando ruim nunca vira erro de entrega, senão o Nack devolve para a fila e entra em loop.

- [ ] **Step 4: Rodar e ver passar**

Run: `go test ./internal/call/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/call/command.go internal/call/command_test.go
git commit -m "feat(call): dispatch call commands onto live calls"
```

---

### Task 6: Streaming de vídeo Annex-B a partir de URL

`video.play` recebe um `.h264` já codificado, fatia em access units e alimenta a chamada. É o único caminho de vídeo de saída, porque o gateway não codifica.

**Files:**
- Create: `internal/call/annexb.go`
- Modify: `internal/call/command.go`
- Test: `internal/call/annexb_test.go`

**Interfaces:**
- Consumes: `LiveCall.SendVideo`
- Produces:

```go
// SplitAnnexB slices an Annex-B H.264 stream into access units. Each returned
// unit keeps its leading start code, which is what SendVideo expects. A unit
// starts at an access-unit delimiter, SPS, PPS or the first slice NAL of a new
// picture; parameter sets are kept attached to the picture that follows them.
func SplitAnnexB(stream []byte) [][]byte

// PlayAnnexB feeds units into lc at frameRate, stopping on ctx cancellation or
// on the first send error.
func PlayAnnexB(ctx context.Context, lc LiveCall, units [][]byte, frameRate int) error
```

- [ ] **Step 1: Escrever os testes**

Crie `internal/call/annexb_test.go`:

```go
package call_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

func TestSplitAnnexBOnFourByteStartCodes(t *testing.T) {
	// SPS(0x67), PPS(0x68), IDR(0x65), then a non-IDR slice(0x41).
	stream := []byte{
		0, 0, 0, 1, 0x67, 0xAA,
		0, 0, 0, 1, 0x68, 0xBB,
		0, 0, 0, 1, 0x65, 0xCC,
		0, 0, 0, 1, 0x41, 0xDD,
	}
	units := call.SplitAnnexB(stream)
	if len(units) != 2 {
		t.Fatalf("got %d access units, want 2 (SPS+PPS+IDR together, then the P slice)", len(units))
	}
	wantFirst := []byte{
		0, 0, 0, 1, 0x67, 0xAA,
		0, 0, 0, 1, 0x68, 0xBB,
		0, 0, 0, 1, 0x65, 0xCC,
	}
	if !bytes.Equal(units[0], wantFirst) {
		t.Errorf("unit[0] = % x, want % x", units[0], wantFirst)
	}
	wantSecond := []byte{0, 0, 0, 1, 0x41, 0xDD}
	if !bytes.Equal(units[1], wantSecond) {
		t.Errorf("unit[1] = % x, want % x", units[1], wantSecond)
	}
}

func TestSplitAnnexBOnThreeByteStartCodes(t *testing.T) {
	stream := []byte{
		0, 0, 1, 0x65, 0xAA,
		0, 0, 1, 0x41, 0xBB,
	}
	units := call.SplitAnnexB(stream)
	if len(units) != 2 {
		t.Fatalf("got %d access units, want 2", len(units))
	}
	if !bytes.Equal(units[0], []byte{0, 0, 1, 0x65, 0xAA}) {
		t.Errorf("unit[0] = % x", units[0])
	}
}

func TestSplitAnnexBRejectsGarbage(t *testing.T) {
	if units := call.SplitAnnexB([]byte{1, 2, 3, 4}); len(units) != 0 {
		t.Errorf("got %d units from a stream with no start code, want 0", len(units))
	}
	if units := call.SplitAnnexB(nil); len(units) != 0 {
		t.Errorf("got %d units from nil, want 0", len(units))
	}
}

func TestPlayAnnexBSendsEveryUnit(t *testing.T) {
	lc := &fakeCall{id: "C1"}
	units := [][]byte{{0, 0, 0, 1, 0x65}, {0, 0, 0, 1, 0x41}}

	if err := call.PlayAnnexB(context.Background(), lc, units, 1000); err != nil {
		t.Fatalf("PlayAnnexB: %v", err)
	}
	if len(lc.videoOut) != 2 {
		t.Fatalf("sent %d units, want 2", len(lc.videoOut))
	}
	if !bytes.Equal(lc.videoOut[0], units[0]) {
		t.Errorf("unit[0] = % x, want % x", lc.videoOut[0], units[0])
	}
}

func TestPlayAnnexBStopsOnContextCancel(t *testing.T) {
	lc := &fakeCall{id: "C1"}
	units := make([][]byte, 100)
	for i := range units {
		units[i] = []byte{0, 0, 0, 1, 0x41}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := call.PlayAnnexB(ctx, lc, units, 30); err == nil {
		t.Fatal("PlayAnnexB err = nil, want the context error")
	}
	if len(lc.videoOut) >= len(units) {
		t.Errorf("sent %d units after cancellation, want fewer than %d", len(lc.videoOut), len(units))
	}
}
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `go test ./internal/call/ -run 'TestSplitAnnexB|TestPlayAnnexB' -v`
Expected: FAIL — `call.SplitAnnexB` não existe

- [ ] **Step 3: Implementar `internal/call/annexb.go`**

Varredura procurando `00 00 01` e `00 00 00 01`. O tipo do NAL é `b & 0x1F` no byte seguinte ao start code. Fronteira de access unit: tipos 9 (AUD), 7 (SPS), 8 (PPS) e 6 (SEI) **iniciam** uma unidade nova apenas se a unidade corrente já tiver uma fatia (tipos 1 ou 5); um NAL de fatia inicia unidade nova se a unidade corrente já tiver fatia. Assim SPS/PPS ficam grudados no quadro que descrevem, que é o que um decodificador espera.

`PlayAnnexB` respeita o `frameRate` com `time.Ticker` de `time.Second/time.Duration(frameRate)`, chama `lc.SendVideo(unit, tick)` e devolve `ctx.Err()` na cancelação.

- [ ] **Step 4: Rodar e ver passar**

Run: `go test ./internal/call/ -v`
Expected: PASS

- [ ] **Step 5: Ligar `video.play` no dispatcher**

Em `command.go`, a ação `video.play` busca `cmd.MediaURL` com o `MediaFetcher`, chama `SplitAnnexB` e dispara `PlayAnnexB` numa goroutine com o contexto da chamada — não bloqueie o consumidor AMQP durante o streaming. Stream sem nenhum access unit falha com `CodeMediaFetch`. Acrescente ao `command_test.go`:

```go
func TestDispatchVideoPlayStreamsTheFile(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	lc := &fakeCall{id: "C1"}
	caller.onIncoming(lc)

	fetch := func(context.Context, string) ([]byte, error) {
		return []byte{0, 0, 0, 1, 0x65, 0xAA, 0, 0, 0, 1, 0x41, 0xBB}, nil
	}
	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CallID: "C1", CommandID: "cmd-1",
		Action: "video.play", MediaURL: "https://example.test/clip.h264",
	}, fetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if acks := pub.typed(call.EventCommandAck); len(acks) != 1 {
		t.Errorf("acks = %+v, want one", acks)
	}
}

func TestDispatchVideoPlayRejectsAStreamWithNoAccessUnit(t *testing.T) {
	pub := &memPublisher{}
	m := newTestManager(t, pub, newMemStore(), time.Now)
	caller := &fakeCaller{}
	m.Attach("chan-a", caller)
	caller.onIncoming(&fakeCall{id: "C1"})

	fetch := func(context.Context, string) ([]byte, error) { return []byte("not h264"), nil }
	if err := m.Dispatch(context.Background(), caller, amqp.GatewayCallCommand{
		ChannelID: "chan-a", CallID: "C1", CommandID: "cmd-1",
		Action: "video.play", MediaURL: "https://example.test/bad.bin",
	}, fetch); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	failed := pub.typed(call.EventCommandFailed)
	if len(failed) != 1 || failed[0].Error.Code != call.CodeMediaFetch {
		t.Errorf("failures = %+v, want one media_fetch_failed", failed)
	}
}
```

- [ ] **Step 6: Rodar e ver passar**

Run: `go test ./internal/call/ -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/call/annexb.go internal/call/annexb_test.go internal/call/command.go internal/call/command_test.go
git commit -m "feat(call): stream Annex-B H.264 from a URL into an outgoing call"
```

---

### Task 7: Topologia, consumidor e publicador AMQP

Fila nova para comandos de chamada e routing key nova para eventos.

**Files:**
- Modify: `internal/amqp/topology.go`, `internal/amqp/consumer.go`, `internal/amqp/publisher.go`
- Test: `test/amqp_call_test.go`

**Interfaces:**
- Consumes: `GatewayCallCommand` (Task 2)
- Produces:

```go
// internal/amqp
const CallRoutingKey = "whatsapp.call.v1"

const (
	GatewayCallExchange = "whatsapp.gateway.call.v1"
	GatewayCallQueue    = "gateway.call"
	GatewayCallDLX      = "gateway.call.dlx"
	GatewayCallDLQ      = "gateway.call.dlq"
	GatewayCallConsumer = "whatsmeow-gateway.call"
)

type CallHandler func(ctx context.Context, cmd GatewayCallCommand) error

func (c *Consumer) StartCall(ctx context.Context, handler CallHandler) error
func (p *Publisher) PublishCall(ctx context.Context, evt any) error
```

- [ ] **Step 1: Escrever o teste de round-trip**

Crie `test/amqp_call_test.go`. Usa `startRabbitMQ` e `waitForDelivery`, que já existem em `test/amqp_test.go` e `test/gateway_e2e_test.go`:

```go
package test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	rabbitmq "github.com/rabbitmq/amqp091-go"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

func TestConsumerDeliversCallCommand(t *testing.T) {
	conn := startRabbitMQ(t)

	consumer, err := gatewayamqp.NewConsumer(conn, gatewayamqp.ConsumerConfig{Prefetch: 10})
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}
	t.Cleanup(func() {
		if err := consumer.Close(); err != nil {
			t.Errorf("consumer.Close failed: %v", err)
		}
	})

	received := make(chan gatewayamqp.GatewayCallCommand, 1)
	if err := consumer.StartCall(context.Background(), func(_ context.Context, cmd gatewayamqp.GatewayCallCommand) error {
		received <- cmd
		return nil
	}); err != nil {
		t.Fatalf("StartCall failed: %v", err)
	}

	want := gatewayamqp.GatewayCallCommand{
		TenantID:    "tenant-1",
		ChannelID:   "channel-1",
		CommandID:   "cmd-1",
		CallID:      "CALL1",
		Action:      "video.orientation",
		To:          "+5511888888888",
		Targets:     []string{"a@s.whatsapp.net", "b@s.whatsapp.net"},
		GroupID:     "120363000000000000@g.us",
		Video:       true,
		MediaURL:    "https://example.test/clip.h264",
		Emoji:       "👍",
		Orientation: 3,
		Enabled:     true,
		Raised:      true,
		Participant: "c@s.whatsapp.net",
		LinkToken:   "TOK",
	}
	body, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("failed to marshal call command: %v", err)
	}

	pubCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open publish channel: %v", err)
	}
	t.Cleanup(func() {
		if err := pubCh.Close(); err != nil {
			t.Errorf("failed to close publish channel: %v", err)
		}
	})

	publishCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pubCh.PublishWithContext(publishCtx, gatewayamqp.GatewayCallExchange, "0", false, false, rabbitmq.Publishing{
		ContentType: "application/json",
		Body:        body,
	}); err != nil {
		t.Fatalf("failed to publish call command: %v", err)
	}

	select {
	case got := <-received:
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("command =\n%+v\nwant\n%+v", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the call command")
	}
}

func TestPublisherPublishesCallEvent(t *testing.T) {
	conn := startRabbitMQ(t)

	publisher, err := gatewayamqp.NewPublisher(conn)
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("publisher.Close failed: %v", err)
		}
	})

	probeCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open probe channel: %v", err)
	}
	t.Cleanup(func() {
		if err := probeCh.Close(); err != nil {
			t.Errorf("failed to close probe channel: %v", err)
		}
	})

	q, err := probeCh.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("failed to declare probe queue: %v", err)
	}
	if err := probeCh.QueueBind(q.Name, gatewayamqp.CallRoutingKey, gatewayamqp.EventsExchange, false, nil); err != nil {
		t.Fatalf("failed to bind probe queue: %v", err)
	}
	deliveries, err := probeCh.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume probe queue: %v", err)
	}

	evt := call.Event{
		PhoneNumberID: "5511999999999",
		TenantID:      "tenant-1",
		ChannelID:     "channel-1",
		CallID:        "CALL1",
		Direction:     call.DirectionInbound,
		Type:          call.EventEnded,
		Timestamp:     "1754300100",
		Duration:      10,
		Media:         &call.Media{Key: "calls/channel-1/CALL1.wav", MimeType: "audio/wav"},
	}
	publishCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := publisher.PublishCall(publishCtx, evt); err != nil {
		t.Fatalf("PublishCall failed: %v", err)
	}

	delivery := waitForDelivery(t, deliveries, gatewayamqp.CallRoutingKey, 10*time.Second)
	var got call.Event
	if err := json.Unmarshal(delivery.Body, &got); err != nil {
		t.Fatalf("failed to unmarshal call event: %v", err)
	}
	if !reflect.DeepEqual(got, evt) {
		t.Fatalf("event =\n%+v\nwant\n%+v", got, evt)
	}
}
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `go test ./test/ -run TestCallCommandRoundTrip -v`
Expected: FAIL — as constantes e o `StartCall` não existem

- [ ] **Step 3: Implementar**

Em `topology.go`, acrescente as constantes e chame `declareCommandTopology(callCh, GatewayCallExchange, GatewayCallQueue, GatewayCallDLX, GatewayCallDLQ)` — a função genérica já existe e não muda. Em `consumer.go`, `StartCall` é cópia estrutural de `StartSend` com o tipo e as constantes de chamada, incluindo o `callStarted bool`, o `Qos` no `NewConsumer` e o `Cancel` no `Close`. Em `publisher.go`, `PublishCall` chama `p.publish(ctx, CallRoutingKey, evt)`.

- [ ] **Step 4: Rodar e ver passar**

Run: `go test ./test/ -run TestCallCommandRoundTrip -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/amqp/ test/amqp_call_test.go
git commit -m "feat(amqp): add the call command queue and the call event routing key"
```

---

### Task 8: Adaptador da meowcaller em `internal/session`

O único arquivo que toca os tipos concretos da lib. Constrói o `meowcaller.Client` antes do `Connect()` e converte os tipos.

**Files:**
- Create: `internal/session/caller.go`
- Modify: `internal/session/client.go`
- Test: `internal/session/caller_test.go`

**Interfaces:**
- Consumes: `call.Caller`, `call.LiveCall` (Task 1)
- Produces:

```go
// internal/session
// WAClient gains:
Calls() call.Caller
```

- [ ] **Step 1: Escrever o teste da conversão de tipos**

Crie `internal/session/caller_test.go`. Os tipos concretos da meowcaller não são construíveis sem uma stack VoIP real, então o teste cobre exatamente o que é testável sem ela: a conversão de valores.

```go
package session

import (
	"testing"

	"github.com/purpshell/meowcaller"
	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

func TestPhaseFromLibrary(t *testing.T) {
	cases := map[meowcaller.CallPhase]call.Phase{
		meowcaller.CallPhaseIdle:        call.PhaseIdle,
		meowcaller.CallPhaseCalling:     call.PhaseCalling,
		meowcaller.CallPhaseRinging:     call.PhaseRinging,
		meowcaller.CallPhaseConnecting:  call.PhaseConnecting,
		meowcaller.CallPhaseActive:      call.PhaseActive,
		meowcaller.CallPhaseEnded:       call.PhaseEnded,
		meowcaller.CallPhaseWaitingRoom: call.PhaseWaitingRoom,
	}
	for lib, want := range cases {
		if got := phaseFrom(lib); got != want {
			t.Errorf("phaseFrom(%v) = %v, want %v", lib, got, want)
		}
	}
}

func TestVideoStateFromLibrary(t *testing.T) {
	got := videoStateFrom(meowcaller.VideoState{Active: true, Upgrade: true, Orientation: 2, Raw: 11})
	want := call.VideoState{Active: true, Upgrade: true, Orientation: 2, Raw: 11}
	if got != want {
		t.Errorf("videoStateFrom = %+v, want %+v", got, want)
	}
}

func TestReactionFromLibraryFlattensJIDs(t *testing.T) {
	sender := types.NewJID("5511888888888", types.DefaultUserServer)
	got := reactionFrom(meowcaller.CallReaction{Emoji: "👍", Sender: sender, Removed: true})
	if got.Emoji != "👍" || got.Sender != sender.String() || !got.Removed {
		t.Errorf("reactionFrom = %+v, want the flattened reaction", got)
	}
}

func TestGroupStateFromLibraryFlattensParticipants(t *testing.T) {
	jid := types.NewJID("5511888888888", types.DefaultUserServer)
	pn := types.NewJID("5511888888888", types.DefaultUserServer)
	got := groupStateFrom(meowcaller.GroupCallState{
		TransactionID: 7,
		Participants: []meowcaller.GroupCallParticipant{
			{JID: jid, PN: pn, State: "connected", HandRaised: true},
		},
	})
	if got.TransactionID != 7 || len(got.Participants) != 1 {
		t.Fatalf("groupStateFrom = %+v", got)
	}
	p := got.Participants[0]
	if p.JID != jid.String() || p.State != "connected" || !p.HandRaised {
		t.Errorf("participant = %+v, want the flattened participant", p)
	}
}
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `go test ./internal/session/ -v`
Expected: FAIL — `phaseFrom` não existe

- [ ] **Step 3: Implementar `internal/session/caller.go`**

- `phaseFrom`, `videoStateFrom`, `reactionFrom`, `groupStateFrom`, `waitingRoomFrom`, `handStateFrom`, `screenShareFrom`: conversão pura, `types.JID` vira `String()`.
- `callerAdapter` embrulha `*meowcaller.Client` e implementa `call.Caller`; `liveCallAdapter` embrulha `*meowcaller.Call` e implementa `call.LiveCall`.
- `Receive(sink func([]float32))` vira `c.call.Receive(meowcaller.SinkFunc(sink))`; `ReceiveVideo` vira `meowcaller.VideoSinkFunc(sink)`.
- `Play(src io.ReadCloser)` vira `c.call.Play(meowcaller.PCMStream(src))`.
- `Peer()` retorna `c.call.Peer().String()`.

- [ ] **Step 4: Modificar `internal/session/client.go`**

```go
type waClient struct {
	client *whatsmeow.Client
	caller call.Caller
}

func NewWAClient(device *store.Device, log waLog.Logger) WAClient {
	client := whatsmeow.NewClient(device, log)
	ConfigureAutoReconnect(client)
	// meowcaller installs the low-level <call>/<ack> interception, so it must be
	// constructed before the receive loop starts — that is, before Connect.
	return &waClient{client: client, caller: newCallerAdapter(meowcaller.NewClient(client))}
}

func (w *waClient) Calls() call.Caller { return w.caller }
```

Acrescente `Calls() call.Caller` à interface `WAClient`.

- [ ] **Step 5: Atualizar o fake de teste**

Em `test/fake_test.go`, acrescente ao `fakeWAClient`:

```go
func (f *fakeWAClient) Calls() call.Caller { return f.caller }
```

com um campo `caller call.Caller` no struct, que os testes existentes deixam nil e o teste E2E preenche.

- [ ] **Step 6: Rodar e ver passar**

Run: `go vet ./... && go test ./internal/... -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/session/ test/fake_test.go
git commit -m "feat(session): build the meowcaller client before connecting and expose it as call.Caller"
```

---

### Task 9: Fiação no gateway e no main

Liga tudo: consumidor de comandos, `Attach` por sessão, teardown por canal, config e injeção no `main`.

**Files:**
- Modify: `internal/gateway/gateway.go`, `internal/config/config.go`, `cmd/gateway/main.go`
- Test: `test/gateway_call_test.go`

**Interfaces:**
- Consumes: tudo das tarefas anteriores
- Produces:

```go
// internal/gateway
func (g *gateway) CallHandler(ctx context.Context, cmd amqp.GatewayCallCommand) error

// Deps gains — Options, not a built Manager: the identity closure the Manager
// needs reads the gateway's tenant map and the session's device JID, neither of
// which main has access to. Run builds the Manager itself.
CallOptions call.Options

// callPublisher adapts the amqp publisher to call.Publisher. amqp.Publisher's
// methods take `any` (like PublishInbound and PublishStatus), so the amqp package
// stays unaware of the call package.
type callPublisher struct{ p *amqp.Publisher }

func (c callPublisher) PublishCall(ctx context.Context, evt call.Event) error {
	return c.p.PublishCall(ctx, evt)
}

// internal/config — Config gains:
CallTmpDir string // GATEWAY_CALL_TMPDIR, default os.TempDir()
CallRecord bool   // GATEWAY_CALL_RECORD, default true
```

- [ ] **Step 1: Escrever o teste E2E**

Crie `test/gateway_call_test.go`. Reaproveita `startRabbitMQ`, `startRedis`, `startPostgresForGateway`, `waitForDelivery`, `newFakeWAClient` e `rawS3Client`, todos já existentes no pacote `test`.

Os fakes de `internal/call` estão em arquivos `_test.go` e por isso não são importáveis. Este teste define os seus próprios, em `test/call_fake_test.go` — é o mesmo padrão do `fakeWAClient`, que já vive no pacote `test`:

```go
package test

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/w3nder/whatsmeow-gateway/internal/call"
)

// fakeCaller stands in for the meowcaller client: it records the incoming-call
// handler the gateway registers, so the test can fire a call at it.
type fakeCaller struct {
	mu       sync.Mutex
	incoming func(call.LiveCall)
}

func (f *fakeCaller) OnIncomingCall(fn func(call.LiveCall)) {
	f.mu.Lock()
	f.incoming = fn
	f.mu.Unlock()
}

func (f *fakeCaller) incomingHandler() func(call.LiveCall) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.incoming
}

func (f *fakeCaller) fireIncoming(lc call.LiveCall) { f.incomingHandler()(lc) }

func (f *fakeCaller) Call(_ context.Context, target string, video bool) (call.LiveCall, error) {
	return &fakeLiveCall{callID: "OUT1", peer: target, video: video}, nil
}
func (f *fakeCaller) GroupCall(context.Context, []string, call.GroupOptions) (call.LiveCall, error) {
	return &fakeLiveCall{callID: "GRP1"}, nil
}
func (f *fakeCaller) GroupCallByID(context.Context, string, call.GroupOptions) (call.LiveCall, error) {
	return &fakeLiveCall{callID: "GRP2"}, nil
}
func (f *fakeCaller) CreateCallLink(context.Context, bool) (call.Link, error) {
	return call.Link{Token: "TOK"}, nil
}
func (f *fakeCaller) PreviewCallLink(context.Context, string, bool) (call.LinkPreview, error) {
	return call.LinkPreview{Token: "TOK"}, nil
}
func (f *fakeCaller) JoinCallLink(context.Context, string, bool) (call.LiveCall, error) {
	return &fakeLiveCall{callID: "LINK1"}, nil
}

var _ call.Caller = (*fakeCaller)(nil)

// fakeLiveCall records the actions the gateway takes and lets the test drive the
// call's lifecycle and its inbound audio.
type fakeLiveCall struct {
	callID string
	peer   string
	video  bool

	mu      sync.Mutex
	actions []string
	onReady func()
	onEnd   func(string)
	audioIn func([]float32)
	videoIn func([]byte)
}

func (f *fakeLiveCall) record(action string) error {
	f.mu.Lock()
	f.actions = append(f.actions, action)
	f.mu.Unlock()
	return nil
}

func (f *fakeLiveCall) recordedActions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.actions...)
}

func (f *fakeLiveCall) fireReady() {
	f.mu.Lock()
	fn := f.onReady
	f.mu.Unlock()
	if fn != nil {
		fn()
	}
}

func (f *fakeLiveCall) fireEnd(reason string) {
	f.mu.Lock()
	fn := f.onEnd
	f.mu.Unlock()
	if fn != nil {
		fn(reason)
	}
}

func (f *fakeLiveCall) feedAudio(frame []float32) {
	f.mu.Lock()
	fn := f.audioIn
	f.mu.Unlock()
	if fn != nil {
		fn(frame)
	}
}

func (f *fakeLiveCall) ID() string    { return f.callID }
func (f *fakeLiveCall) Peer() string  { return f.peer }
func (f *fakeLiveCall) IsVideo() bool { return f.video }

func (f *fakeLiveCall) Answer() error     { return f.record("answer") }
func (f *fakeLiveCall) Reject() error     { return f.record("reject") }
func (f *fakeLiveCall) Hangup() error     { return f.record("hangup") }
func (f *fakeLiveCall) StartVideo() error { return f.record("video.start") }
func (f *fakeLiveCall) AcceptVideo() error { return f.record("video.accept") }
func (f *fakeLiveCall) StopVideo() error   { return f.record("video.stop") }
func (f *fakeLiveCall) SetVideoEnabled(bool) error    { return f.record("video.enable") }
func (f *fakeLiveCall) SetVideoOrientation(int) error { return f.record("video.orientation") }
func (f *fakeLiveCall) SendVideo([]byte, time.Duration) error { return f.record("video.send") }
func (f *fakeLiveCall) SendReaction(string) error      { return f.record("reaction") }
func (f *fakeLiveCall) SetHandRaised(bool) error       { return f.record("hand.raise") }
func (f *fakeLiveCall) StartScreenShare(*uint32) error { return f.record("screenshare.start") }
func (f *fakeLiveCall) StopScreenShare() error         { return f.record("screenshare.stop") }
func (f *fakeLiveCall) AddParticipant(context.Context, string) error  { return f.record("participant.add") }
func (f *fakeLiveCall) RingParticipant(context.Context, string) error { return f.record("participant.ring") }
func (f *fakeLiveCall) SetApprovalRequired(context.Context, bool) error { return f.record("approval.set") }
func (f *fakeLiveCall) AdmitParticipant(context.Context, string) error  { return f.record("participant.admit") }
func (f *fakeLiveCall) DenyParticipant(context.Context, string) error   { return f.record("participant.deny") }
func (f *fakeLiveCall) Play(io.ReadCloser) error { return f.record("play") }

func (f *fakeLiveCall) Receive(sink func([]float32)) {
	f.mu.Lock()
	f.audioIn = sink
	f.mu.Unlock()
}

func (f *fakeLiveCall) ReceiveVideo(sink func([]byte)) {
	f.mu.Lock()
	f.videoIn = sink
	f.mu.Unlock()
}

func (f *fakeLiveCall) OnReady(fn func()) {
	f.mu.Lock()
	f.onReady = fn
	f.mu.Unlock()
}

func (f *fakeLiveCall) OnEnd(fn func(string)) {
	f.mu.Lock()
	f.onEnd = fn
	f.mu.Unlock()
}

func (f *fakeLiveCall) OnStateChange(func(call.Phase))          {}
func (f *fakeLiveCall) OnPeerAccept(func())                     {}
func (f *fakeLiveCall) OnMuteState(func(bool))                  {}
func (f *fakeLiveCall) OnVideoState(func(call.VideoState))      {}
func (f *fakeLiveCall) OnReaction(func(call.Reaction))          {}
func (f *fakeLiveCall) OnGroupState(func(call.GroupState))      {}
func (f *fakeLiveCall) OnWaitingRoomState(func(call.WaitingRoom)) {}
func (f *fakeLiveCall) OnHandRaise(func(call.HandState))        {}
func (f *fakeLiveCall) OnScreenShare(func(call.ScreenShare))    {}

var _ call.LiveCall = (*fakeLiveCall)(nil)
```

```go
package test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	rabbitmq "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"

	gatewayamqp "github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/call"
	"github.com/w3nder/whatsmeow-gateway/internal/call/calltest"
	"github.com/w3nder/whatsmeow-gateway/internal/dedupe"
	"github.com/w3nder/whatsmeow-gateway/internal/gateway"
	"github.com/w3nder/whatsmeow-gateway/internal/logging"
	"github.com/w3nder/whatsmeow-gateway/internal/media"
	"github.com/w3nder/whatsmeow-gateway/internal/ownership"
	"github.com/w3nder/whatsmeow-gateway/internal/registry"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
	"github.com/w3nder/whatsmeow-gateway/internal/store"
)

// TestGatewayInboundCallRecordsAndPublishes drives a whole inbound call through a
// running gateway: the call arrives, the backend answers it over gateway.call, the
// peer's audio is captured, the call ends, and the recording must be in the bucket
// with its key on the ended event.
func TestGatewayInboundCallRecordsAndPublishes(t *testing.T) {
	ctx := context.Background()

	conn := startRabbitMQ(t)
	redisClient := startRedis(t)

	// MinIO, because this test asserts the recording actually lands in the bucket.
	minioContainer, err := minio.Run(ctx, minioImage)
	if err != nil {
		t.Fatalf("failed to start minio container: %v", err)
	}
	t.Cleanup(func() {
		if err := minioContainer.Terminate(context.Background()); err != nil {
			t.Errorf("failed to terminate minio container: %v", err)
		}
	})
	endpoint, err := minioContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get minio connection string: %v", err)
	}
	const bucket = "vectax-calls-e2e"
	rawS3, err := rawS3Client(ctx, endpoint, minioContainer.Username, minioContainer.Password)
	if err != nil {
		t.Fatalf("failed to build raw s3 client: %v", err)
	}
	if _, err := rawS3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}
	mediaStore, err := media.NewS3Store(ctx, media.S3Config{
		Bucket:          bucket,
		Region:          "us-east-1",
		Endpoint:        "http://" + endpoint,
		AccessKeyID:     minioContainer.Username,
		SecretAccessKey: minioContainer.Password,
	})
	if err != nil {
		t.Fatalf("NewS3Store failed: %v", err)
	}

	consumer, err := gatewayamqp.NewConsumer(conn, gatewayamqp.ConsumerConfig{Prefetch: 10})
	if err != nil {
		t.Fatalf("NewConsumer failed: %v", err)
	}
	publisher, err := gatewayamqp.NewPublisher(conn)
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("publisher.Close failed: %v", err)
		}
	})

	caller := &calltest.Caller{}
	fake := newFakeWAClient()
	fake.caller = caller
	mgr := session.NewManager(func(string, *types.JID) (session.WAClient, error) {
		return fake, nil
	})

	waLogger, logger := logging.New()
	dsn := startPostgresForGateway(t)
	if _, err := store.Open(ctx, dsn, waLogger); err != nil {
		t.Fatalf("store.Open failed: %v", err)
	}
	dedupeStore, err := dedupe.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("dedupe.Open failed: %v", err)
	}
	t.Cleanup(dedupeStore.Close)
	registryStore, err := registry.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("registry.Open failed: %v", err)
	}
	t.Cleanup(registryStore.Close)

	probeCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open probe channel: %v", err)
	}
	t.Cleanup(func() {
		if err := probeCh.Close(); err != nil {
			t.Errorf("failed to close probe channel: %v", err)
		}
	})
	probeQ, err := probeCh.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatalf("failed to declare probe queue: %v", err)
	}
	for _, rk := range []string{gatewayamqp.CallRoutingKey, gatewayamqp.ChannelStatusRoutingKey} {
		if err := probeCh.QueueBind(probeQ.Name, rk, gatewayamqp.EventsExchange, false, nil); err != nil {
			t.Fatalf("failed to bind probe queue to %s: %v", rk, err)
		}
	}
	deliveries, err := probeCh.Consume(probeQ.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume probe queue: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- gateway.Run(runCtx, gateway.Deps{
			Consumer:             consumer,
			Publisher:            publisher,
			Manager:              mgr,
			Ownership:            ownership.NewStore(redisClient, 4),
			Dedupe:               dedupeStore,
			Registry:             registryStore,
			MediaStore:           mediaStore,
			InstanceID:           "gateway-call-e2e",
			ShardLockTTL:         30 * time.Second,
			ShutdownDrainTimeout: 10 * time.Second,
			CallOptions: call.Options{
				TmpDir:      t.TempDir(),
				Record:      true,
			},
			Logger: logger,
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runErrCh:
		case <-time.After(20 * time.Second):
			t.Error("gateway.Run did not return after cancellation")
		}
	})

	const channelID = "channel-call-e2e"
	const tenantID = "tenant-call-e2e"

	// Pair, so the channel has a live session with an attached caller.
	fake.qrItems = []whatsmeow.QRChannelItem{whatsmeow.QRChannelSuccess}
	pairBody, err := json.Marshal(gatewayamqp.PairCommand{
		TenantID: tenantID, ChannelID: channelID, UserID: "user-call-e2e",
	})
	if err != nil {
		t.Fatalf("failed to marshal pair command: %v", err)
	}
	publishCtx, publishCancel := context.WithTimeout(ctx, 10*time.Second)
	defer publishCancel()
	if err := probeCh.PublishWithContext(publishCtx, gatewayamqp.GatewayPairExchange, "0", false, false, rabbitmq.Publishing{
		ContentType: "application/json",
		Body:        pairBody,
	}); err != nil {
		t.Fatalf("failed to publish pair command: %v", err)
	}
	waitForDelivery(t, deliveries, gatewayamqp.ChannelStatusRoutingKey, 15*time.Second)

	// The caller is attached only after the session is registered.
	waitFor(t, 10*time.Second, "incoming-call handler registered", func() bool {
		return caller.IncomingHandler() != nil
	})

	lc := &calltest.Call{CallID: "CALL1", PeerJID: "5511888888888@s.whatsapp.net"}
	caller.FireIncoming(lc)

	incoming := waitForCallEvent(t, deliveries, call.EventIncoming, 15*time.Second)
	if incoming.CallID != "CALL1" || incoming.Direction != call.DirectionInbound {
		t.Fatalf("incoming event = %+v, want inbound CALL1", incoming)
	}
	if incoming.TenantID != tenantID || incoming.ChannelID != channelID {
		t.Fatalf("incoming event identity = %+v, want %s/%s", incoming, tenantID, channelID)
	}

	answerBody, err := json.Marshal(gatewayamqp.GatewayCallCommand{
		TenantID: tenantID, ChannelID: channelID, CommandID: "cmd-answer", CallID: "CALL1", Action: "answer",
	})
	if err != nil {
		t.Fatalf("failed to marshal answer command: %v", err)
	}
	if err := probeCh.PublishWithContext(publishCtx, gatewayamqp.GatewayCallExchange, "0", false, false, rabbitmq.Publishing{
		ContentType: "application/json",
		Body:        answerBody,
	}); err != nil {
		t.Fatalf("failed to publish answer command: %v", err)
	}

	ack := waitForCallEvent(t, deliveries, call.EventCommandAck, 15*time.Second)
	if ack.CommandID != "cmd-answer" {
		t.Fatalf("ack = %+v, want cmd-answer", ack)
	}
	if actions := lc.Actions(); len(actions) != 1 || actions[0] != "answer" {
		t.Fatalf("call actions = %v, want [answer]", actions)
	}

	lc.FireReady()
	lc.FeedAudio([]float32{0.5, -0.5, 0.25})
	lc.FireEnd("hangup")

	ended := waitForCallEvent(t, deliveries, call.EventEnded, 15*time.Second)
	if ended.Reason != "hangup" {
		t.Fatalf("ended reason = %q, want hangup", ended.Reason)
	}
	wantKey := "calls/" + channelID + "/CALL1.wav"
	if ended.Media == nil || ended.Media.Key != wantKey {
		t.Fatalf("ended media = %+v, want key %s", ended.Media, wantKey)
	}

	obj, err := rawS3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(wantKey)})
	if err != nil {
		t.Fatalf("recording was not uploaded: %v", err)
	}
	defer func() {
		if err := obj.Body.Close(); err != nil {
			t.Errorf("failed to close object body: %v", err)
		}
	}()
	header := make([]byte, 12)
	if _, err := io.ReadFull(obj.Body, header); err != nil {
		t.Fatalf("failed to read the recording header: %v", err)
	}
	if !bytes.Equal(header[0:4], []byte("RIFF")) || !bytes.Equal(header[8:12], []byte("WAVE")) {
		t.Fatalf("recording header = %q, want a RIFF/WAVE header", header)
	}
}
```

Dois helpers de teste, no mesmo arquivo:

```go
// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// waitForCallEvent drains call events until one of the wanted type shows up.
// Other event types on the same routing key are skipped, not failed on.
func waitForCallEvent(t *testing.T, deliveries <-chan rabbitmq.Delivery, wantType string, timeout time.Duration) call.Event {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case d := <-deliveries:
			if d.RoutingKey != gatewayamqp.CallRoutingKey {
				continue
			}
			var evt call.Event
			if err := json.Unmarshal(d.Body, &evt); err != nil {
				t.Fatalf("failed to unmarshal call event: %v", err)
			}
			if evt.Type == wantType {
				return evt
			}
		case <-deadline:
			t.Fatalf("timed out waiting for a %q call event", wantType)
		}
	}
}
```

O `calltest.Call` precisa expor, além do que a Task 4 já usa: `Actions() []string`, `FireReady()`, `FireEnd(reason string)` e `FeedAudio(frame []float32)` — este último chamando o sink registrado por `Receive`, que é o que faz o gravador receber áudio.

- [ ] **Step 2: Rodar e ver falhar**

Run: `go test ./test/ -run TestGatewayInboundCall -v`
Expected: FAIL — o gateway ainda não consome `gateway.call`

- [ ] **Step 3: Adicionar a config**

Em `internal/config/config.go`, acrescente os dois campos com os defaults do bloco "Produces", lidos por helpers que aceitam env vazia. `GATEWAY_CALL_RECORD` só desliga com o valor literal `"false"`.

- [ ] **Step 4: Implementar `CallHandler` e a fiação**

Em `internal/gateway/gateway.go`:

- `Deps` ganha `CallOptions call.Options`; `gateway` ganha `calls *call.Manager`.
- `Run` constrói o manager, porque a closure de identidade precisa de coisas que só o gateway tem — o tenant vem de `g.tenantFor` e o `phoneNumberId` do `DeviceJID()` da sessão viva:

```go
g.calls = call.NewManager(
	callPublisher{deps.Publisher},
	deps.MediaStore,
	func(channelID string) call.Identity {
		id := call.Identity{TenantID: g.tenantFor(channelID)}
		if client, err := g.manager.Client(channelID); err == nil {
			if jid := client.DeviceJID(); jid != nil {
				id.PhoneNumberID = jid.User
			}
		}
		return id
	},
	deps.CallOptions,
	deps.Logger,
)
```

`deps.MediaStore` é o `*media.S3Store`, que satisfaz `call.RecordingStore` assim que ganha o `PutStream` da Task 3. Se `CallOptions.TmpDir` vier vazio, use `os.TempDir()`.

- `run` chama `g.consumer.StartCall(g.workCtx, g.CallHandler)` junto dos outros consumidores, com o mesmo tratamento de falha de boot.
- `CallHandler` faz `setTenant`, `ensureChannelConnected`, resolve o client, e chama `g.calls.Dispatch(ctx, client.Calls(), cmd, fetchMediaURL)`.
- Onde uma sessão é registrada (`resumeOwnedSessions` e `PairHandler`, depois do `manager.Resume`/`Pair` bem-sucedido), chame `g.calls.Attach(channelID, client.Calls())`.
- `handleSessionEvent`: em `*events.LoggedOut`, antes de limpar o tenant, chame `g.calls.AbortChannel(g.workCtx, channelID, "logged_out")`. Em `*events.Disconnected`, chame `g.calls.AbortChannel(g.workCtx, channelID, "disconnected")` — mídia não sobrevive a socket caído, e deixar a chamada registrada esconderia o fim dela do backend.
- No desligamento, antes de `g.manager.DisconnectAll()`, aborte as chamadas de todos os canais vivos.

Em `cmd/gateway/main.go`, só repasse a config — o manager é construído dentro do `Run`:

```go
CallOptions: call.Options{
	TmpDir: cfg.CallTmpDir,
	Record: cfg.CallRecord,
},
```

- [ ] **Step 5: Rodar o teste E2E e ver passar**

Run: `go test ./test/ -run TestGatewayInboundCall -v`
Expected: PASS

- [ ] **Step 6: Rodar a suíte inteira e o lint**

Run: `make vet && make lint && make test`
Expected: tudo limpo

- [ ] **Step 7: Commit**

```bash
git add internal/gateway/gateway.go internal/config/config.go cmd/gateway/main.go test/gateway_call_test.go
git commit -m "feat(gateway): consume call commands and publish call events"
```

---

### Task 10: Documentar o contrato para o backend e o front

O backend e o front vão consumir isso. Sem documento eles leem o código Go para descobrir o formato dos eventos.

**Files:**
- Create: `docs/call-contract.md`

**Interfaces:**
- Consumes: contratos da Task 2
- Produces: documento de referência do contrato AMQP de chamada

- [ ] **Step 1: Escrever o documento**

`docs/call-contract.md` com:

- Onde publicar comando: exchange `whatsapp.gateway.call.v1`, qualquer routing key.
- Onde ouvir evento: exchange `sender.events`, routing key `whatsapp.call.v1`.
- Tabela de todas as ações com os campos obrigatórios de cada uma.
- Tabela de todos os tipos de evento com os campos que cada um preenche.
- Um exemplo JSON completo de comando e de evento por fluxo: chamada recebida atendida com gravação, chamada de saída, upgrade para vídeo, chamada de grupo, call link com waiting room.
- Como resolver `media.key` — a mesma resolução já usada para mídia de mensagem.
- Os códigos de erro de `command.failed`: `call_not_found`, `unknown_action`, `invalid_target`, `action_failed`, `no_caller`, `media_fetch_failed`, `recording_upload_failed`.
- O aviso de que o caminho de mídia de vídeo é marcado como NOT VALIDATED pela biblioteca.

- [ ] **Step 2: Commit**

```bash
git add docs/call-contract.md
git commit -m "docs(call): document the call command and event contract for consumers"
```

---

## Limitações conhecidas ao fim do plano

- **Roteamento por shard.** `gateway.call` é fila compartilhada e chamada é estado preso à instância dona do socket. Com mais de uma instância ativa, um comando pode cair na instância errada e falhar com `call_not_found`. Hoje não acontece porque `ownership.ClaimAll` faz cada instância reivindicar todos os shards. Resolver isso exige publicar o comando com routing key derivada do shard e cada instância bindar só os seus — fora do escopo.
- **Mídia de vídeo não validada pela biblioteca**, tanto no envio quanto na recepção. A sinalização de vídeo é exercitada; o transporte de frames precisa de uma chamada real de ponta a ponta para ser confirmado.
- **Gravação de vídeo sai em `.h264` cru.** Toca em ffmpeg e VLC, não em browser. A conversão para mp4 é do backend.
- **Áudio gravado é o do peer.** O que o gateway toca com `play` não entra no WAV; para isso seria preciso mixar as duas pontas, o que a lib não entrega pronto.
- **Sem faixas separadas por participante** em chamada de grupo.
