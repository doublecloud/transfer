package ydb

import (
	"testing"
	"time"
)

func TestAbsDuration(t *testing.T) {
	for _, test := range []struct {
		d time.Duration
	}{
		{
			d: time.Minute,
		},
		{
			d: -time.Minute,
		},
	} {
		t.Run(test.d.String(), func(t *testing.T) {
			abs := absDuration(test.d)
			switch {
			case test.d >= 0 && abs+test.d == 2*test.d:
				return
			case abs+test.d == 0:
				return
			}
			t.Errorf("unexpected abs value")
		})
	}
}

func TestChooseByState(t *testing.T) {
	c := connRuntimeCriterion{}
	c1, c2 := &connListElement{
		index: 1,
	}, &connListElement{
		index: 2,
	}
	for _, test := range [...]struct {
		name   string
		s1     ConnState
		s2     ConnState
		choice *connListElement
	}{
		{
			s1:     ConnStateUnknown,
			s2:     ConnStateUnknown,
			choice: nil,
		},
		{
			s1:     ConnStateUnknown,
			s2:     ConnOnline,
			choice: c2,
		},
		{
			s1:     ConnOnline,
			s2:     ConnStateUnknown,
			choice: c1,
		},
		{
			s1:     ConnStateUnknown,
			s2:     ConnOffline,
			choice: c1,
		},
		{
			s1:     ConnOffline,
			s2:     ConnStateUnknown,
			choice: c2,
		},
		{
			s1:     ConnStateUnknown,
			s2:     ConnBanned,
			choice: c1,
		},
		{
			s1:     ConnBanned,
			s2:     ConnStateUnknown,
			choice: c2,
		},
		{
			s1:     ConnOnline,
			s2:     ConnOnline,
			choice: nil,
		},
		{
			s1:     ConnOnline,
			s2:     ConnOffline,
			choice: c1,
		},
		{
			s1:     ConnOnline,
			s2:     ConnBanned,
			choice: c1,
		},
		{
			s1:     ConnOffline,
			s2:     ConnOnline,
			choice: c2,
		},
		{
			s1:     ConnBanned,
			s2:     ConnOnline,
			choice: c2,
		},
		{
			s1:     ConnOffline,
			s2:     ConnOffline,
			choice: nil,
		},
		{
			s1:     ConnOffline,
			s2:     ConnBanned,
			choice: c2,
		},
		{
			s1:     ConnBanned,
			s2:     ConnOffline,
			choice: c1,
		},
		{
			s1:     ConnBanned,
			s2:     ConnBanned,
			choice: nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if choise := c.chooseByState(c1, c2, test.s1, test.s2); choise != test.choice {
				t.Errorf("unexpected choice (%d vs %d): %T(%+v), expected: %T(%+v)", test.s1, test.s2, choise, choise, test.choice, test.choice)
			}
		})
	}
}
