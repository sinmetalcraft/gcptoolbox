package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	dataflow "cloud.google.com/go/dataflow/apiv1beta3"
	cloudtasksbox "github.com/sinmetalcraft/gcpbox/cloudtasks"
	metadatabox "github.com/sinmetalcraft/gcpbox/metadata/cloudrun"
	"github.com/sinmetalcraft/gcptoolbox/bq2gcs"
	"github.com/sinmetalcraft/gcptoolbox/dfrun"
	dataflowbox "github.com/sinmetalcraft/gcptoolbox/dfrun/dataflow"
	"github.com/sinmetalcraft/gcptoolbox/handlers"
)

func Run(ctx context.Context, port string) error {
	fmt.Println("gcptoolbox server ignition")

	if os.Getenv("GCPTOOLBOX_BQ2GCS") != "" {
		fmt.Println("bq2gcs ignition")
		http.Handle("/bq2gcs/export", handlers.BaseHandler(&bq2gcs.ExportHandler{}))
	}

	if os.Getenv("GCPTOOLBOX_DFRUN") != "" {
		fmt.Println("dfrun ignition")
		templateCli, err := dataflow.NewTemplatesClient(ctx)
		if err != nil {
			return fmt.Errorf("failed to create dataflow template client: %v", err)
		}
		jobsCli, err := dataflow.NewJobsV1Beta3Client(ctx)
		if err != nil {
			return fmt.Errorf("failed to create dataflow jobs client: %v", err)
		}
		classicTemplateRunner, err := dataflowbox.NewClassicTemplateRunner(ctx, templateCli, jobsCli)
		if err != nil {
			return fmt.Errorf("failed to create dataflowbox classic template runner: %v", err)
		}
		tasksCli, err := cloudtasks.NewClient(ctx)
		if err != nil {
			return fmt.Errorf("failed to create cloudtasks client: %v", err)
		}

		saEmail, err := metadatabox.ServiceAccountEmail()
		if err != nil {
			return fmt.Errorf("failed to get service account email: %v", err)
		}
		tasksService, err := cloudtasksbox.NewService(ctx, tasksCli, saEmail)
		if err != nil {

		}
		cloudRunURI := os.Getenv("GCPTOOLBOX_CLOUD_RUN_URI")
		if cloudRunURI == "" {
			return fmt.Errorf("$GCPTOOLBOX_CLOUD_RUN_URI env var not set")
		}

		h, err := dfrun.NewHandler(ctx, classicTemplateRunner, tasksService, cloudRunURI)
		if err != nil {
			return fmt.Errorf("failed to create dfrun handler: %v", err)
		}
		http.Handle("/dfrun/", handlers.BaseHandler(h))
	}

	// Start HTTP server.
	log.Printf("listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}

	return nil
}
