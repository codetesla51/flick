package flick

import (
	"context"

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
}

// NewSyncService returns a FlagSyncService server ready for registration.
func NewSyncService(pool *pgxpool.Pool) *SyncService {
	return &SyncService{pool: pool}
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

// SyncFlags is not implemented yet.
func (s *SyncService) SyncFlags(*syncv1.SyncFlagsRequest, syncv1.FlagSyncService_SyncFlagsServer) error {
	return status.Error(codes.Unimplemented, "SyncFlags not implemented yet")
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