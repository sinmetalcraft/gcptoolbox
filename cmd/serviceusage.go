package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/k0kubun/pp"
	"github.com/sinmetalcraft/gcpbox/serviceusage"
	"github.com/spf13/cobra"
	crmv1 "google.golang.org/api/cloudresourcemanager/v1"
	orgsus "google.golang.org/api/serviceusage/v1"
)

var ServiceUsageCmd = &cobra.Command{
	Use:   "serviceusage",
	Short: "command line serviceusage",
	RunE: func(cmd *cobra.Command, args []string) error {
		return fmt.Errorf("Command name argument expected.")
	},
}

func serviceUsageDiffCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diff",
		Short: "Diff of the Service Usage of the two Projects.",
		Args:  cobra.RangeArgs(2, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			if len(args) == 0 {
				return nil
			}

			projectID1 := args[0]
			projectID2 := args[1]

			orgSusService, err := orgsus.NewService(ctx)
			if err != nil {
				return err
			}

			crmv1Service, err := crmv1.NewService(ctx)
			if err != nil {
				return err
			}

			sus, err := serviceusage.NewService(ctx, orgSusService, crmv1Service)
			if err != nil {
				return err
			}

			diff, err := sus.ListByDiff(ctx, projectID1, projectID2)
			if err != nil {
				return err
			}
			if len(diff) > 0 {
				pp.Println(diff)
				return errors.New("exists serviceusage diff")
			}

			return nil
		},
	}

	return cmd
}
