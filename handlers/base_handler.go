package handlers

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
)

type AppHandler interface {
	Serve(ctx context.Context, w http.ResponseWriter, r *http.Request) *HTTPResponse
}

type HTTPResponse struct {
	StatusCode int
	Header     map[string]string
	Body       interface{}
}

type BasicErrorMessage struct {
	Err error `json:"error"`
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
