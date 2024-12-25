package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

type AppHandler interface {
	Serve(ctx context.Context, w http.ResponseWriter, r *http.Request) *HTTPResponse
}

type HTTPResponse struct {
	StatusCode int
	Header     map[string]string
	Body       any
}

type BasicErrorMessage struct {
	Err error `json:"error"`
}

func (m *BasicErrorMessage) MarshalJSON() ([]byte, error) {
	if m == nil || m.Err == nil {
		return []byte(`{"error":""}`), nil
	}
	return []byte(fmt.Sprintf(`{"error":"%s"}`, m.Err.Error())), nil
}

func BaseHandler(handler AppHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resp := handler.Serve(ctx, w, r)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		for k, v := range resp.Header {
			w.Header().Set(k, v)
		}
		w.WriteHeader(resp.StatusCode)
		if resp.Body != nil {
			body, err := json.Marshal(resp.Body)
			if err != nil {
				log.Printf("failed json.Marshal Body\n")
				return
			}
			_, err = w.Write(body)
			if err != nil {
				log.Printf("failed write to http response\n")
				return
			}
		}
	})
}
