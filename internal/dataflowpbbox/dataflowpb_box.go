package dataflowpbbox

import "cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"

func JobStateText(jobState dataflowpb.JobState) string {
	switch jobState {
	case dataflowpb.JobState_JOB_STATE_UNKNOWN:
		return "UNKNOWN"
	case dataflowpb.JobState_JOB_STATE_STOPPED:
		return "STOPPED"
	case dataflowpb.JobState_JOB_STATE_RUNNING:
		return "RUNNING"
	case dataflowpb.JobState_JOB_STATE_DONE:
		return "DONE"
	case dataflowpb.JobState_JOB_STATE_FAILED:
		return "FAILED"
	case dataflowpb.JobState_JOB_STATE_CANCELLED:
		return "CANCELLED"
	case dataflowpb.JobState_JOB_STATE_UPDATED:
		return "UPDATED"
	case dataflowpb.JobState_JOB_STATE_DRAINING:
		return "DRAINING"
	case dataflowpb.JobState_JOB_STATE_DRAINED:
		return "DRAINED"
	case dataflowpb.JobState_JOB_STATE_PENDING:
		return "PENDING"
	case dataflowpb.JobState_JOB_STATE_CANCELLING:
		return "CANCELLING"
	case dataflowpb.JobState_JOB_STATE_QUEUED:
		return "QUEUED"
	case dataflowpb.JobState_JOB_STATE_RESOURCE_CLEANING_UP:
		return "RESOURCE_CLEANING_UP"
	default:
		return "UNKNOWN"
	}
}
