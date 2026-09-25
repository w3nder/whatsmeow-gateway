package groups

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/socket"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

const lockedMessage = "group is locked (423)"

func Classify(err error) error {
	var rpcErr *amqp.RpcError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &rpcErr):
		return err
	case errors.Is(err, whatsmeow.ErrInvalidImageFormat):
		return amqp.RpcInvalidRequest(err.Error())
	case errors.Is(err, whatsmeow.ErrIQTimedOut), errors.Is(err, whatsmeow.ErrNotConnected), errors.Is(err, whatsmeow.ErrNotLoggedIn), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled), isTransientIQError(err), isDisconnected(err):
		return amqp.RpcUnavailable(err.Error())
	case isIQError(err):
		code, _ := iqCode(err)
		return classifyGroupIQCode(code, err)
	case errors.Is(err, whatsmeow.ErrGroupNotFound):
		return classifyGroupIQCode(http.StatusNotFound, err)
	case errors.Is(err, whatsmeow.ErrNotInGroup):
		return classifyGroupIQCode(http.StatusForbidden, err)
	case errors.Is(err, whatsmeow.ErrGroupInviteLinkUnauthorized):
		return classifyGroupIQCode(http.StatusUnauthorized, err)
	default:
		return &amqp.RpcError{Code: amqp.RpcCodeInternal, Message: err.Error()}
	}
}

func IsUnavailable(err error) bool {
	var rpcErr *amqp.RpcError
	return errors.As(err, &rpcErr) && rpcErr.Code == amqp.RpcCodeUnavailable
}

func isDisconnected(err error) bool {
	return errors.As(err, new(*whatsmeow.DisconnectedError)) || errors.Is(err, socket.ErrSocketClosed)
}

func isTransientIQError(err error) bool {
	code, ok := iqCode(err)
	return ok && (code == http.StatusTooManyRequests || code >= http.StatusInternalServerError)
}

func classifyGroupIQCode(code int, err error) error {
	switch code {
	case http.StatusLocked:
		return amqp.RpcLocked(lockedMessage)
	case http.StatusUnauthorized, http.StatusForbidden:
		return amqp.RpcForbidden(fmt.Sprintf("not an admin of the group (%d)", code))
	case http.StatusNotFound, http.StatusGone:
		return amqp.RpcNotFound(fmt.Sprintf("group not found (%d)", code))
	default:
		return amqp.RpcBadGateway(err.Error())
	}
}

func isIQError(err error) bool {
	_, ok := iqCode(err)
	return ok
}

func iqCode(err error) (int, bool) {
	var iq *whatsmeow.IQError
	if !errors.As(err, &iq) {
		return 0, false
	}
	return iq.Code, true
}
