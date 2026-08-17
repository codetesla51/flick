package flick

import (
	"context"
	"log"

	syncv1 "github.com/codetesla51/flick/gen/flagd/sync/v1"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// SyncService implements flagd.sync.v1.FlagSyncService backed by the flags table.
type SyncService struct {
	syncv1.UnimplementedFlagSyncServiceServer
	pool *pgxpool.Pool
	hub  *Hub
}

// NewSyncService returns a FlagSyncService server ready for registration.
func NewSyncService(pool *pgxpool.Pool, hub *Hub) *SyncService {
	return &SyncService{pool: pool, hub: hub}
}

// FetchAllFlags loads every flag, translates each through the flick→flagd
// translator, wraps them into one {"flags": {...}} document, and returns it.
func (s *SyncService) FetchAllFlags(ctx context.Context, _ *syncv1.FetchAllFlagsRequest) (*syncv1.FetchAllFlagsResponse, error) {
	rows, err := loadFlags(ctx, s.pool)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load flags: %v", err)
	}

	doc, err := BuildSnapshot(rows)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build snapshot: %v", err)
	}

	return &syncv1.FetchAllFlagsResponse{FlagConfiguration: doc}, nil
}

// SyncFlags streams the flag configuration: an initial snapshot of every flag,
// then one full-configuration message per live outbox delivery.
//
// Ordering: subscribe before any snapshot work (nothing is missed while the
// snapshot is built — events in that window sit buffered), send the snapshot,
// then flush the buffered events and forward every new delivery.
func (s *SyncService) SyncFlags(_ *syncv1.SyncFlagsRequest, stream syncv1.FlagSyncService_SyncFlagsServer) error {
	ctx := stream.Context()

	// 1. Register before anything else so no delta is missed mid-snapshot.
	id, ch := s.hub.Subscribe()
	defer s.hub.Unsubscribe(id)

	// 2+3. Build and send the initial snapshot; arrivals buffer in ch meanwhile.
	rows, err := loadFlags(ctx, s.pool)
	if err != nil {
		return status.Errorf(codes.Internal, "load flags: %v", err)
	}
	current := flagEventsToState(rows)
	if err := s.sendSnapshot(stream, current); err != nil {
		return err
	}

	// 4+5. Flush buffered deltas, then forward every new delivery.
	for {
		select {
		case <-ctx.Done():
			return nil
		case payload, ok := <-ch:
			if !ok {
				return nil
			}
			updated, err := ApplyDelta(current, payload)
			if err != nil {
				log.Printf("sync: skipping bad delta: %v", err)
				continue
			}
			current = updated
			if err := s.sendSnapshot(stream, current); err != nil {
				return err
			}
		}
	}
}

// sendSnapshot marshals the flag state and streams it as one response.
func (s *SyncService) sendSnapshot(stream syncv1.FlagSyncService_SyncFlagsServer, flags map[string]flagdFlag) error {
	doc, err := marshalFlags(flags)
	if err != nil {
		return status.Errorf(codes.Internal, "marshal flags: %v", err)
	}
	return stream.Send(&syncv1.SyncFlagsResponse{FlagConfiguration: doc})
}

// GetMetadata is deprecated upstream; return an empty metadata struct.
func (s *SyncService) GetMetadata(context.Context, *syncv1.GetMetadataRequest) (*syncv1.GetMetadataResponse, error) {
	return &syncv1.GetMetadataResponse{Metadata: &structpb.Struct{}}, nil
}

// loadFlags reads every flag row, newest key order first.
func loadFlags(ctx context.Context, pool *pgxpool.Pool) ([]flagEvent, error) {
	rows, err := pool.Query(ctx, `
		SELECT key, state, default_variant, variants, targeting, metadata
		FROM flags
		ORDER BY key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []flagEvent
	for rows.Next() {
		var f flagEvent
		if err := rows.Scan(&f.Key, &f.State, &f.DefaultVariant, &f.Variants, &f.Targeting, &f.Metadata); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}
