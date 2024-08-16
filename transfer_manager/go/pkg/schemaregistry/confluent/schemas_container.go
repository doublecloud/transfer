package confluent

import (
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/util"
)

type referenceObj struct {
	referenceName string
	subject       string
	version       int32
}

type schemasContainer struct {
	inQueue util.Set[referenceObj]
	schemas map[referenceObj]Schema
}

func (s *schemasContainer) addTask(referenceName, subject string, version int32) {
	if _, ok := s.schemas[referenceObj{referenceName: referenceName, subject: subject, version: version}]; ok {
		return // if we already got this schema
	}
	s.inQueue.Add(referenceObj{ // if we occur >1 times this schema - util.Set deduplicate them
		referenceName: referenceName,
		subject:       subject,
		version:       version,
	})
}

func (s *schemasContainer) getTask() (string, string, int32) {
	slice := s.inQueue.Slice()
	if len(slice) == 0 {
		return "", "", int32(0)
	}
	return slice[0].referenceName, slice[0].subject, slice[0].version
}

func (s *schemasContainer) doneTask(referenceName, subject string, version int32, schema Schema) error {
	if !s.inQueue.Contains(referenceObj{referenceName: referenceName, subject: subject, version: version}) {
		return xerrors.Errorf("trying to complete task, which is not in the queue, subject:%s, version:%d", subject, version)
	}
	s.inQueue.Remove(referenceObj{
		referenceName: referenceName,
		subject:       subject,
		version:       version,
	})

	if _, ok := s.schemas[referenceObj{referenceName: referenceName, subject: subject, version: version}]; ok {
		return xerrors.Errorf("trying to complete task, which is already completed, subject:%s, version:%d", subject, version)
	}
	s.schemas[referenceObj{
		referenceName: referenceName,
		subject:       subject,
		version:       version,
	}] = schema
	return nil
}

func (s *schemasContainer) references() map[string]Schema {
	result := make(map[string]Schema)
	for k, v := range s.schemas {
		result[k.referenceName] = v
	}
	return result
}

func newSchemasContainer(references []SchemaReference) *schemasContainer {
	set := util.NewSet[referenceObj]()
	for _, reference := range references {
		set.Add(referenceObj{
			referenceName: reference.Name,
			subject:       reference.SubjectName,
			version:       reference.Version,
		})
	}
	return &schemasContainer{
		inQueue: *set,
		schemas: make(map[referenceObj]Schema),
	}
}
