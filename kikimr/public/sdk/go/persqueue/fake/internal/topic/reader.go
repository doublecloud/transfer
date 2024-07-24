// Copyright (c) 2019 Yandex LLC. All rights reserved.
// Author: Andrey Khaliullin <avhaliullin@yandex-team.ru>

package topic

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/fake/data"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/fake/internal/model"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/fake/internal/queue"
	"github.com/doublecloud/tross/kikimr/public/sdk/go/persqueue/log"
)

// Reader is structure holding state of consumer reading topic. Reader holds partition commit offsets, and
// also responsible for assigning partitions to leave reading sessions
type Reader struct {
	ctx    context.Context
	logger log.Logger

	t             *Topic
	readOffsets   map[uint32]uint64
	readOffsetsMu sync.RWMutex

	sessions   map[*readSession]partAssignmentMap
	sessionsMu sync.RWMutex

	freeParts  map[uint32]struct{}
	genCounter uint64

	repartition chan struct{}
}

func NewReader(ctx context.Context, logger log.Logger, t *Topic) *Reader {
	res := &Reader{
		ctx:         ctx,
		logger:      logger,
		t:           t,
		readOffsets: make(map[uint32]uint64),
		sessions:    make(map[*readSession]partAssignmentMap),
		freeParts:   make(map[uint32]struct{}),
		repartition: make(chan struct{}, 1),
	}
	for part := 0; part < t.GetPartCount(); part++ {
		res.freeParts[uint32(part)] = struct{}{}
	}
	go res.partitionsLoop()
	return res
}

func (r *Reader) notifyRepartition() {
	select {
	case r.repartition <- struct{}{}:
	default:
	}
}

// CreateSession prepares new ReadSession, which is inactive right after creation. ReadSession.Start()
// should be called in order to start session.
func (r *Reader) CreateSession(d ReadSessionDispatcher, waitCommits bool) ReadSession {
	return &readSession{
		r:                 r,
		disp:              d,
		waitCommits:       waitCommits,
		activeParts:       make(map[uint32]*queue.Reader),
		waitCommitParts:   make(map[uint32]*releaseOnCommit),
		partConfirmations: make(map[uint64]struct{}),
		assignments:       make(map[uint64]*model.PartitionInfo),
	}
}

func (r *Reader) partitionsLoop() {
	for r.ctx.Err() == nil {
		r.doRepartition()
		select {
		case <-r.ctx.Done():
			return
		case <-r.repartition:
		}
	}
}

// doRepartition observing read session updates:
//   - new sessions
//   - closed sessions
//   - lock or release partition confirmations
//
// Depending on current sessions state, doRepartition will reassign some partitions. One extra responsibility of
// this method - is to do final cleanup for closed sessions. For some reasons it is better to cleanup in same goroutine
//
// Current implementation tries to implement even parts distribution between all readers with few restrictions:
//   - pending partitions (with unconfirmed lock or release) cannot be reassigned
//   - session will got one assigning partition at time - that is, it should confirm lock requests one by one
func (r *Reader) doRepartition() {
	activeSessions := make(map[*readSession]partAssignmentMap)
	toCloseSessions := make(map[*readSession]partAssignmentMap)
	totalParts := r.t.GetPartCount()
	r.sessionsMu.RLock()
	// first, separate sessions that should be cleaned up and active sessions
	for s, p := range r.sessions {
		if s.isClosed() {
			toCloseSessions[s] = p
		} else {
			activeSessions[s] = p
		}
	}
	r.sessionsMu.RUnlock()

	if len(toCloseSessions) > 0 {
		r.sessionsMu.Lock()
		for s := range toCloseSessions {
			delete(r.sessions, s)
		}
		r.sessionsMu.Unlock()
		for _, parts := range toCloseSessions {
			// closed sessions release their partitions in all states without need to wait confirmations
			for part := range parts {
				r.freeParts[part] = struct{}{}
			}
		}
	}
	if len(activeSessions) == 0 {
		return
	}

	// in honest distribution every session got totalParts/activeSessions partitions at least, and this value +1 at most
	minPerReader := totalParts / len(activeSessions)
	maxPerReader := minPerReader + 1

	for s, parts := range activeSessions {
		assignedCount := 0
		assigningCount := 0
		releasingCount := 0
		// in first pass we just actualizing assignment statuses
		for part, state := range parts {
			if state.status == partAssigning && s.pollConfirmedGen(state.genID) {
				state.status = partAssigned
			}
			if state.status == partReleasing && s.pollConfirmedGen(state.genID) {
				delete(parts, part)
				r.freeParts[part] = struct{}{}
				continue
			}
			switch state.status {
			case partAssigning:
				assigningCount++
			case partAssigned:
				assignedCount++
			case partReleasing:
				releasingCount++
			}
		}
		// will release some parts if:
		// 1. too many assigned parts for session
		// 2. releasable (assigned) partitions exist
		if assignedCount > 0 && assignedCount+assigningCount > maxPerReader {
			toReleaseCount := maxPerReader - assignedCount - assigningCount
			if assignedCount < toReleaseCount {
				toReleaseCount = assignedCount
			}
			for part, state := range parts {
				if state.status != partAssigned {
					continue
				}
				toReleaseCount--
				r.releasePart(s, state, part)
				if toReleaseCount == 0 {
					break
				}
			}
		}
		// will assign part if:
		// 1. too few assigned parts for session
		// 2. no pending assignments
		// 3. we have free parts
		if assignedCount < maxPerReader && assigningCount == 0 && len(r.freeParts) > 0 {
			var part uint32
			// take first
			for part = range r.freeParts {
				break
			}
			r.assignPart(s, parts, part)
			delete(r.freeParts, part)
		}
	}
}

func (r *Reader) getReadOffset(part uint32) uint64 {
	r.readOffsetsMu.RLock()
	defer r.readOffsetsMu.RUnlock()
	return r.readOffsets[part]
}

func (r *Reader) getPartInfo(part uint32) *model.PartitionInfo {
	endOffset := r.t.GetQueue(part).Size()
	readOffset := r.getReadOffset(part)
	return &model.PartitionInfo{
		Topic:      r.t.GetName(),
		Partition:  part,
		ReadOffset: readOffset,
		EndOffset:  uint64(endOffset),
	}
}

func (r *Reader) releasePart(s *readSession, state *partAssignment, part uint32) {
	genID := atomic.AddUint64(&r.genCounter, 1)
	state.status = partReleasing
	state.genID = genID
	s.release(genID, part)
}

func (r *Reader) assignPart(s *readSession, parts partAssignmentMap, part uint32) {
	genID := atomic.AddUint64(&r.genCounter, 1)
	parts[part] = &partAssignment{
		status: partAssigning,
		genID:  genID,
	}
	s.lockPart(genID, r.getPartInfo(part))
}

func (r *Reader) commitUntil(part uint32, offset uint64) {
	r.readOffsetsMu.Lock()
	if offset > r.readOffsets[part] {
		r.readOffsets[part] = offset
	}
	r.readOffsetsMu.Unlock()
}

var _ ReadSession = &readSession{}

type readSession struct {
	r           *Reader
	disp        ReadSessionDispatcher
	waitCommits bool

	activeParts     map[uint32]*queue.Reader
	waitCommitParts map[uint32]*releaseOnCommit

	partConfirmations map[uint64]struct{}
	assignments       map[uint64]*model.PartitionInfo
	closed            bool
	mu                sync.RWMutex
}

// pollConfirmedGen tests if specific lock/release generation is confirmed by read session and cleans up this flag
func (s *readSession) pollConfirmedGen(genID uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.partConfirmations[genID]
	if ok {
		delete(s.partConfirmations, genID)
	}
	return ok
}

func (s *readSession) release(genID uint64, part uint32) {
	s.mu.Lock()
	queueReader := s.activeParts[part]
	delete(s.activeParts, part)
	if s.waitCommits {
		s.waitCommitParts[part] = &releaseOnCommit{
			offset: queueReader.GetReadOffset(),
			genID:  genID,
		}
	} else {
		s.confirmRelease(genID)
	}
	s.mu.Unlock()
	if queueReader != nil {
		queueReader.Close()
	}
	s.disp.ReleasePartition(genID, s.r.t.GetName(), part, s.waitCommits)
}

func (s *readSession) lockPart(genID uint64, p *model.PartitionInfo) {
	s.mu.Lock()
	s.assignments[genID] = p
	s.mu.Unlock()
	s.disp.LockPartition(genID, p)
}

func (s *readSession) notifyRepartition() {
	s.r.notifyRepartition()
}

func (s *readSession) isClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

func (s *readSession) Start() {
	s.r.sessionsMu.Lock()
	s.r.sessions[s] = make(partAssignmentMap)
	s.r.sessionsMu.Unlock()
	s.r.notifyRepartition()
}

func (s *readSession) Commit(part uint32, offset uint64) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return
	}
	if _, ok := s.activeParts[part]; ok {
		s.r.commitUntil(part, offset)
		s.mu.RUnlock()
		return
	}

	waitRelease, ok := s.waitCommitParts[part]
	if !ok {
		s.mu.RUnlock()
		return
	}
	s.r.commitUntil(part, offset)
	s.mu.RUnlock()
	if offset >= uint64(waitRelease.offset) {
		s.mu.Lock()
		s.confirmRelease(waitRelease.genID)
		delete(s.waitCommitParts, part)
		s.mu.Unlock()
	}
}

func (s *readSession) ConfirmLock(genID uint64, readOff uint64, commitUntilOff uint64, verify bool) error {
	if commitUntilOff > readOff {
		return fmt.Errorf("commit offset %d cannot be bigger than read offset %d", commitUntilOff, readOff)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	partInfo, ok := s.assignments[genID]
	if !ok {
		return fmt.Errorf("lockPart with generation %d not found", genID)
	}
	part := partInfo.Partition
	if verify && readOff > partInfo.ReadOffset {
		return fmt.Errorf("verification failed: %d > %d", readOff, partInfo.ReadOffset)
	}
	delete(s.assignments, genID)
	s.partConfirmations[genID] = struct{}{}
	s.r.commitUntil(part, commitUntilOff)
	s.activeParts[part] = s.r.t.GetQueue(part).CreateReader(s.disp.ReadSignal(), int(readOff))
	s.notifyRepartition()
	return nil
}

func (s *readSession) confirmRelease(genID uint64) {
	s.partConfirmations[genID] = struct{}{}
	s.notifyRepartition()
}

func (s *readSession) ReadTo(acc []*data.Message, limit int) []*data.Message {
	s.mu.RLock()
	for _, reader := range s.activeParts {
		acc = reader.Read(acc, limit)
		if len(acc) == limit {
			break
		}
	}
	s.mu.RUnlock()
	return acc
}

func (s *readSession) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for _, reader := range s.activeParts {
		reader.Close()
	}
	s.notifyRepartition()
}

// ReadSession interface used to read and commit from specific topic in specific consumer session.
type ReadSession interface {
	// Start used to start read session - before start it will be inactive
	Start()
	// Commit used to commit offset in partition
	Commit(part uint32, commitUntil uint64)
	// ConfirmLock used to confirm new partition assignment. After partition assignment confirmation
	// this ReadSession starts read new partition
	ConfirmLock(genID uint64, readOff uint64, commitUntil uint64, verify bool) error
	// ReadTo reads currently available messages from locked partitions
	ReadTo(acc []*data.Message, limit int) []*data.Message
	// Close stops reading and releases all locked partitions
	Close()
}

// ReadSessionDispatcher used by Reader to notify ReadSession implementer about partition reassignment events and new
// messages
type ReadSessionDispatcher interface {
	LockPartition(genID uint64, p *model.PartitionInfo)
	ReleasePartition(genID uint64, topic string, partition uint32, mustCommit bool)
	ReadSignal() chan<- struct{}
}

type partAssignmentMap = map[uint32]*partAssignment
type partAssignment struct {
	status partStatus
	genID  uint64
}

type partStatus int

const (
	partAssigning partStatus = iota
	partAssigned
	partReleasing
)

type releaseOnCommit struct {
	offset int
	genID  uint64
}
