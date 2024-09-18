package main

import (
	"fmt"
	"sort"
	"time"
)

type MyStat struct {
	durations []time.Duration
	configs   []interface{}
}

func (s *MyStat) AddResult(config interface{}, duration time.Duration) {
	s.durations = append(s.durations, duration)
	s.configs = append(s.configs, config)
}

func (s *MyStat) Print(topN int) {
	type durationAndIndex struct {
		durations time.Duration
		index     int
	}

	durationAndIndexArr := make([]durationAndIndex, 0)
	for i, currDurations := range s.durations {
		durationAndIndexArr = append(durationAndIndexArr, durationAndIndex{
			durations: currDurations,
			index:     i,
		})
	}

	sort.Slice(durationAndIndexArr, func(i, j int) bool {
		return durationAndIndexArr[i].durations < durationAndIndexArr[j].durations
	})

	if topN > len(durationAndIndexArr) {
		topN = len(durationAndIndexArr)
	}

	durationAndIndexArr = durationAndIndexArr[0:topN]

	fmt.Println("-----------------------------------")
	for i, currDurationAndIndex := range durationAndIndexArr {
		fmt.Printf("top#%d duration:%v config:%v\n", i, s.durations[currDurationAndIndex.index], s.configs[currDurationAndIndex.index])
	}
	fmt.Println("-----------------------------------")
}

func NewMyStat() *MyStat {
	return &MyStat{
		durations: make([]time.Duration, 0),
		configs:   make([]interface{}, 0),
	}
}
