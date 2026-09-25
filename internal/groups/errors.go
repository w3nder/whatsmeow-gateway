package groups

import (
	"context"
	"errors"
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
	case isLockedIQError(err):
		return amqp.RpcLocked(lockedMessage)
	case errors.Is(err, whatsmeow.ErrGroupNotFound), errors.Is(err, whatsmeow.ErrNotInGroup), errors.Is(err, whatsmeow.ErrGroupInviteLinkUnauthorized), isIQError(err):
		return amqp.RpcBadGateway(err.Error())
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

func isLockedIQError(err error) bool {
	code, ok := iqCode(err)
	return ok && code == http.StatusLocked
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
