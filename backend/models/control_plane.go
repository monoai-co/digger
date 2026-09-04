package models

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/diggerhq/digger/libs/operation"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ControlPlaneMode string

const (
	ControlPlaneModeNormal ControlPlaneMode = "normal"
	ControlPlaneModeHold   ControlPlaneMode = "hold"
	ControlPlaneModeDrain  ControlPlaneMode = "drain"
	ControlPlaneModeFenced ControlPlaneMode = "fenced"
)

const ControlPlaneFenceSingletonID int16 = 1

var (
	ErrControlPlaneUnconfigured = errors.New("control-plane database identity and writer epoch are required")
	ErrControlPlaneFenced       = errors.New("control-plane writer is fenced")
	ErrControlPlaneHold         = errors.New("control-plane execution is on hold")
	ErrControlPlaneDrain        = errors.New("control-plane writer is draining")
	ErrControlPlaneProtocol     = errors.New("control-plane protocol version is below the active floor")
)

type ControlPlaneFence struct {
	ID               int16            `gorm:"primaryKey;autoIncrement:false;check:control_plane_fence_singleton_check,id = 1"`
	DatabaseIdentity string           `gorm:"type:text;not null"`
	WriterEpoch      int64            `gorm:"not null;check:control_plane_fence_writer_epoch_check,writer_epoch > 0"`
	Mode             ControlPlaneMode `gorm:"type:text;not null;check:control_plane_fence_mode_check,mode IN ('normal','hold','drain','fenced')"`
	ProtocolFloor    int              `gorm:"type:integer;not null;default:1;check:control_plane_fence_protocol_floor_check,protocol_floor > 0"`
	UpdatedAt        time.Time        `gorm:"not null"`
}

func (ControlPlaneFence) TableName() string {
	return "control_plane_fence"
}

// WithAuthoritativeWriteTx serializes application writes against epoch changes.
// The cutover controller advances the singleton row under FOR UPDATE; ordinary
// writers hold FOR SHARE until their transaction commits.
func (db *Database) WithAuthoritativeWriteTx(
	ctx context.Context,
	databaseIdentity string,
	writerEpoch int64,
	requireNormal bool,
	write func(*gorm.DB, *ControlPlaneFence) error,
) error {
	if strings.TrimSpace(databaseIdentity) == "" || writerEpoch <= 0 {
		return ErrControlPlaneUnconfigured
	}
	return db.GormDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var fence ControlPlaneFence
		query := tx
		if tx.Dialector.Name() == "postgres" {
			query = query.Clauses(clause.Locking{Strength: "SHARE"})
		}
		if err := query.First(&fence, "id = ?", ControlPlaneFenceSingletonID).Error; err != nil {
			return fmt.Errorf("load control-plane fence: %w", err)
		}
		if fence.DatabaseIdentity != databaseIdentity || fence.WriterEpoch != writerEpoch {
			return fmt.Errorf("%w: expected database %q epoch %d, active database %q epoch %d", ErrControlPlaneFenced, databaseIdentity, writerEpoch, fence.DatabaseIdentity, fence.WriterEpoch)
		}
		if fence.ProtocolFloor > operation.ProtocolVersion {
			return fmt.Errorf("%w: binary=%d floor=%d", ErrControlPlaneProtocol, operation.ProtocolVersion, fence.ProtocolFloor)
		}
		if fence.Mode == ControlPlaneModeFenced {
			return ErrControlPlaneFenced
		}
		if requireNormal {
			switch fence.Mode {
			case ControlPlaneModeNormal:
			case ControlPlaneModeHold:
				return ErrControlPlaneHold
			case ControlPlaneModeDrain:
				return ErrControlPlaneDrain
			default:
				return ErrControlPlaneFenced
			}
		}
		return write(tx, &fence)
	})
}

func (db *Database) CheckAuthoritativeWriter(ctx context.Context, databaseIdentity string, writerEpoch int64, requireNormal bool) error {
	return db.WithAuthoritativeWriteTx(ctx, databaseIdentity, writerEpoch, requireNormal, func(*gorm.DB, *ControlPlaneFence) error {
		return nil
	})
}
