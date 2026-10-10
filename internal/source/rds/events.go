package rds

import (
	"context"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
)

// EventsAPI is the RDS DescribeEvents call, used to learn about reboots and
// Multi-AZ failovers. *awsrds.Client satisfies it.
type EventsAPI interface {
	DescribeEvents(ctx context.Context, in *awsrds.DescribeEventsInput, opts ...func(*awsrds.Options)) (*awsrds.DescribeEventsOutput, error)
}

var _ EventsAPI = (*awsrds.Client)(nil)

// RestartEvent reports that an instance restarted or failed over at At.
type RestartEvent struct {
	InstanceID string
	At         time.Time
	Message    string
}

// IsRestartEvent reports whether an RDS instance event means the database
// server restarted — a reboot or a Multi-AZ failover. Both lose the last few
// KB of unflushed log writes (observed live), so log positions must be
// re-verified.
func IsRestartEvent(e rdstypes.Event) bool {
	msg := strings.ToLower(aws.ToString(e.Message))
	for _, c := range e.EventCategories {
		switch strings.ToLower(c) {
		case "failover":
			return true
		case "availability":
			if strings.Contains(msg, "restart") || strings.Contains(msg, "reboot") || strings.Contains(msg, "shutdown") {
				return true
			}
		}
	}
	return false
}

// PollRestartEvents lists restart/failover events for all DB instances in the
// client's region between since and until — one paginated call covers the
// whole fleet.
func PollRestartEvents(ctx context.Context, api EventsAPI, since, until time.Time) ([]RestartEvent, error) {
	var out []RestartEvent
	in := &awsrds.DescribeEventsInput{
		SourceType: rdstypes.SourceTypeDbInstance,
		StartTime:  aws.Time(since),
		EndTime:    aws.Time(until),
	}
	for {
		resp, err := api.DescribeEvents(ctx, in)
		if err != nil {
			return nil, err
		}
		for _, e := range resp.Events {
			if IsRestartEvent(e) {
				out = append(out, RestartEvent{
					InstanceID: aws.ToString(e.SourceIdentifier),
					At:         aws.ToTime(e.Date),
					Message:    aws.ToString(e.Message),
				})
			}
		}
		if aws.ToString(resp.Marker) == "" {
			return out, nil
		}
		in.Marker = resp.Marker
	}
}
