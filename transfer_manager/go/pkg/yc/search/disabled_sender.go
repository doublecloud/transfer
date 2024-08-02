package search

import "context"

type DisabledSender struct{}

func (s *DisabledSender) Send(_ context.Context, _ *SearchResource) error { return nil }
func (s *DisabledSender) Close() error                                    { return nil }
