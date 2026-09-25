package groups

import (
	"context"
	"errors"
	"net/http"

	"go.mau.fi/whatsmeow"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

func Classify(err error) error {
	var rpcErr *amqp.RpcError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &rpcErr):
		return err
	case errors.Is(err, whatsmeow.ErrInvalidImageFormat):
		return amqp.RpcInvalidRequest(err.Error())
	case errors.Is(err, whatsmeow.ErrIQTimedOut), errors.Is(err, whatsmeow.ErrNotConnected), errors.Is(err, whatsmeow.ErrNotLoggedIn), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled), isTransientIQError(err):
		return amqp.RpcUnavailable(err.Error())
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

func isTransientIQError(err error) bool {
	var iq *whatsmeow.IQError
	if !errors.As(err, &iq) {
		return false
	}
	switch iq.Code {
	case http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusServiceUnavailable:
		return true
	default:
		return false
	}
}

func isIQError(err error) bool {
	var iq *whatsmeow.IQError
	return errors.As(err, &iq)
}
