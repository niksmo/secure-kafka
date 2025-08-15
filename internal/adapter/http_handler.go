package adapter

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/niksmo/secure-kafka/internal/core/domain"
	"github.com/niksmo/secure-kafka/internal/core/port"
)

type HTTPHandler struct {
	log     *slog.Logger
	service port.MessageRegistrar
}

func RegisterHTTPHandler(log *slog.Logger, mux *http.ServeMux, s port.MessageRegistrar) {
	h := HTTPHandler{log, s}
	mux.HandleFunc("POST /v1/message", h.Message)
}

func (h HTTPHandler) Message(w http.ResponseWriter, r *http.Request) {
	const op = "HTTPHandler.Message"
	log := h.log.With("op", op)

	var data map[string]any
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		h.writeErr(log, w,
			"invalid JSON value", http.StatusBadRequest)
		return
	}

	msg := h.toDomain(data)
	msgID, err := h.service.RegisterMessage(r.Context(), msg)
	if err != nil {
		h.writeErr(log, w,
			"internal error", http.StatusServiceUnavailable)
		log.Error("failed to register message", "err", err)
		return
	}

	statusCode := http.StatusCreated
	resultMsg := "accept"
	w.WriteHeader(statusCode)
	w.Write([]byte(resultMsg))
	log.Info(resultMsg, "statusCode", statusCode, "messageID", msgID.String())
}

func (h HTTPHandler) toDomain(data map[string]any) domain.Message {
	return domain.NewMessage(data)
}

func (h HTTPHandler) writeErr(
	log *slog.Logger, w http.ResponseWriter, resultMsg string, statusCode int,
) {
	http.Error(w, resultMsg, statusCode)
	log.Info(resultMsg, "statusCode", statusCode)
}

func AcceptJSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const JSONmime = "application/json"

		isJSON := strings.EqualFold(
			r.Header.Get("content-type"), JSONmime,
		)
		if !isJSON {
			r.Header.Set("Accept", JSONmime)
			http.Error(
				w, "invalid content type", http.StatusUnsupportedMediaType,
			)
			return
		}
		next.ServeHTTP(w, r)
	})
}
