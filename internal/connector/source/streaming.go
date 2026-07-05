// Package source contains source connector interfaces and implementations.
package source

import (
	"context"

	"github.com/rkshvish/vortaraos/pkg/config"
	"github.com/rkshvish/vortaraos/pkg/row"
)

// StreamingSource is implemented by all real-time streaming source connectors.
// Unlike BatchSource, streaming sources push events continuously rather than
// being polled on a schedule.
//
// The contract for exactly-once delivery is:
//  1. Event arrives via Subscribe -> sent to out channel
//  2. Engine processes the event (transform -> route -> load)
//  3. Engine calls Ack() only after successful delivery
//  4. Ack() commits the offset (Kafka) or sends HTTP 200 (Webhook)
//
// If the engine crashes between step 1 and 3, the event will be redelivered on
// restart because Ack was never called.
type StreamingSource interface {
	// Connect opens a connection to the streaming source.
	// Must be called before Subscribe.
	Connect(ctx context.Context, cfg config.StreamingConfig) error

	// Subscribe starts consuming events from the source and sends each event as
	// a Row to the out channel.
	// Subscribe blocks until ctx is cancelled or a fatal error occurs.
	// The out channel must NOT be closed by Subscribe - the caller owns it.
	Subscribe(ctx context.Context, out chan<- row.Row) error

	// Ack signals that a Row has been successfully processed and delivered.
	// The source should commit the offset or send an acknowledgement to prevent
	// redelivery.
	// rowID matches Row.ID set during Subscribe.
	Ack(ctx context.Context, rowID string) error

	// Nack signals that processing failed. The source should NOT commit the
	// offset so the event will be redelivered.
	// rowID matches Row.ID set during Subscribe.
	Nack(ctx context.Context, rowID string) error

	// Close shuts down the streaming source gracefully.
	Close() error
}
