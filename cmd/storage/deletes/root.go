package deletes

import (
	"fmt"

	"cloud.google.com/go/storage"
	adts "github.com/apstndb/adcplus/tokensource"
	"github.com/spf13/cobra"
	"google.golang.org/api/option"
)

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete",
		RunE:  RunE,
	}
	return cmd
}

func RunE(cmd *cobra.Command, args []string) (err error) {
	ctx := cmd.Context()

	objectListFilePath := args[0]

	ts, err := adts.SmartAccessTokenSource(ctx)
	if err != nil {
		return err
	}

	gcs, err := storage.NewClient(ctx, option.WithTokenSource(ts))
	if err != nil {
		return err
	}

	s, err := NewService(ctx, gcs)
	if err != nil {
		return err
	}

	if err := s.DeleteObjectsFromObjectListFilePath(ctx, objectListFilePath); err != nil {
		return err
	}

	fmt.Println("DONE")
	return nil
}
