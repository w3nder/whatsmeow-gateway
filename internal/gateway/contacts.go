package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"go.mau.fi/whatsmeow/types"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
	"github.com/w3nder/whatsmeow-gateway/internal/groups"
	"github.com/w3nder/whatsmeow-gateway/internal/session"
)

const (
	rpcContactCheck       = "contact.check"
	contactCheckMaxPhones = 50
)

type contactCheckRequest struct {
	TenantID  string   `json:"tenantId"`
	ChannelID string   `json:"channelId"`
	Phones    []string `json:"phones"`
}

type contactCheckResult struct {
	Phone      string  `json:"phone"`
	OnWhatsapp bool    `json:"onWhatsapp"`
	JID        *string `json:"jid"`
	LID        *string `json:"lid"`
}

type contactCheckResponse struct {
	Results []contactCheckResult `json:"results"`
}

func (g *gateway) registerContactRpc(ctx context.Context) error {
	if err := g.rpc.Handle(ctx, rpcContactCheck, g.rpcContactCheck); err != nil {
		return fmt.Errorf("gateway: register rpc %s: %w", rpcContactCheck, err)
	}
	return nil
}

func (g *gateway) rpcContactCheck(ctx context.Context, payload json.RawMessage) (any, error) {
	req, err := decodeRpc[contactCheckRequest](payload)
	if err != nil {
		return nil, err
	}
	queries, err := contactCheckQueries(req)
	if err != nil {
		return nil, err
	}
	client, err := g.groupClient(ctx, req.TenantID, req.ChannelID)
	if err != nil {
		return nil, err
	}
	accounts, err := client.IsOnWhatsApp(ctx, uniqueQueries(queries))
	if err != nil {
		if classified := groups.Classify(err); groups.IsUnavailable(classified) {
			return nil, classified
		}
		return nil, amqp.RpcBadGateway(err.Error())
	}
	byQuery := make(map[string]types.IsOnWhatsAppResponse, len(accounts))
	for _, account := range accounts {
		byQuery[phoneQuery(account.Query)] = account
	}
	out := contactCheckResponse{Results: make([]contactCheckResult, 0, len(req.Phones))}
	for i, phone := range req.Phones {
		out.Results = append(out.Results, g.contactCheckResultFor(ctx, client, phone, byQuery[queries[i]]))
	}
	return out, nil
}

func contactCheckQueries(req contactCheckRequest) ([]string, error) {
	if req.TenantID == "" {
		return nil, amqp.RpcInvalidRequest("tenantId is required")
	}
	if req.ChannelID == "" {
		return nil, amqp.RpcInvalidRequest("channelId is required")
	}
	if len(req.Phones) == 0 {
		return nil, amqp.RpcInvalidRequest("phones is required")
	}
	if len(req.Phones) > contactCheckMaxPhones {
		return nil, amqp.RpcInvalidRequest(fmt.Sprintf("at most %d phones per request, got %d", contactCheckMaxPhones, len(req.Phones)))
	}
	queries := make([]string, len(req.Phones))
	for i, phone := range req.Phones {
		digits := strings.TrimPrefix(phone, "+")
		if digits == "" || strings.Trim(digits, "0123456789") != "" {
			return nil, amqp.RpcInvalidRequest(fmt.Sprintf("phone %q is not a number", phone))
		}
		queries[i] = "+" + digits
	}
	return queries, nil
}

func uniqueQueries(queries []string) []string {
	seen := make(map[string]bool, len(queries))
	out := make([]string, 0, len(queries))
	for _, query := range queries {
		if !seen[query] {
			seen[query] = true
			out = append(out, query)
		}
	}
	return out
}

func phoneQuery(query string) string {
	return "+" + strings.TrimPrefix(query, "+")
}

func (g *gateway) contactCheckResultFor(ctx context.Context, client session.WAClient, phone string, account types.IsOnWhatsAppResponse) contactCheckResult {
	result := contactCheckResult{Phone: phone, OnWhatsapp: account.IsIn}
	if !account.IsIn {
		return result
	}
	pn := account.PhoneNumber
	if pn.Server != types.DefaultUserServer && account.JID.Server == types.DefaultUserServer {
		pn = account.JID
	}
	if pn.Server != types.DefaultUserServer {
		return result
	}
	result.JID = jidPointer(pn)
	if account.JID.Server == types.HiddenUserServer {
		result.LID = jidPointer(account.JID)
		return result
	}
	lid, found, err := client.KnownLIDForPN(ctx, pn)
	if err != nil {
		g.logger.Warn("gateway: known lid lookup failed", "chat", pn.String(), "err", err)
		return result
	}
	if found {
		result.LID = jidPointer(lid)
	}
	return result
}

func jidPointer(jid types.JID) *string {
	value := jid.String()
	return &value
}
