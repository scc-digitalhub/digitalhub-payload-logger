// SPDX-FileCopyrightText: © 2026 DSLab - Fondazione Bruno Kessler
//
// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"fmt"
	"log"
)

// FallbackSink implements Sink with fallback mechanism.
// It tries to send to the primary sink, and if that fails, falls back to the secondary sink.
type FallbackSink struct {
	Primary  Sink // Primary sink to try first
	Fallback Sink // Fallback sink if primary fails
}

// NewFallbackSink creates a new FallbackSink with primary and fallback sinks.
func NewFallbackSink(primary, fallback Sink) *FallbackSink {
	return &FallbackSink{Primary: primary, Fallback: fallback}
}

// Send attempts to send to the primary sink, and falls back to the secondary if it fails.
func (f *FallbackSink) Send(ctx RequestContext) error {
	if err := f.Primary.Send(ctx); err != nil {
		log.Printf("Primary sink failed, falling back: %v", err)
		return f.Fallback.Send(ctx)
	}
	return nil
}

// Close closes both primary and fallback sinks.
func (f *FallbackSink) Close() error {
	var errs []error
	if err := f.Primary.Close(); err != nil {
		errs = append(errs, fmt.Errorf("primary close error: %w", err))
	}
	if err := f.Fallback.Close(); err != nil {
		errs = append(errs, fmt.Errorf("fallback close error: %w", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	return nil
}
