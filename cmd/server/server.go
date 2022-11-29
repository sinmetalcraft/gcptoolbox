package server

import (
	"log"
	"net/http"

	"github.com/sinmetalcraft/gcptoolbox/bq2gcs"
	"github.com/sinmetalcraft/gcptoolbox/handlers"
)

func Run(port string) {
	http.Handle("/bq2gcs/export", handlers.BaseHandler(&bq2gcs.ExportHandler{}))

	// Start HTTP server.
	log.Printf("listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}
