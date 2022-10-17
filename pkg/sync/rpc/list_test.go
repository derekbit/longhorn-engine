package rpc

import (
	"strconv"
	"testing"

	. "gopkg.in/check.v1"

	"github.com/longhorn/longhorn-engine/pkg/replica"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) TestSnapshotHashListCRUD(c *C) {
	list := &SnapshotHashList{}

	snapshotNames := []string{"snapshot0"}
	task := replica.NewSnapshotHashTask(snapshotNames, false)
	list.Add(snapshotNames[0], task)

	_, err := list.Get(snapshotNames[0])
	c.Assert(err, IsNil)

	_, err = list.Get("nonexistence")
	c.Assert(err, NotNil)

	err = list.Delete(snapshotNames[0])
	c.Assert(err, IsNil)

	_, err = list.Get(snapshotNames[0])
	c.Assert(err, NotNil)
}

func (s *TestSuite) TestSnapshotHashListRefreshTriggerByAdd(c *C) {
	list := &SnapshotHashList{}

	numSnapshots := MaxSnapshotHashTaskSize + 2
	for i := 0; i < numSnapshots; i++ {
		snapshotNames := []string{"snapshot" + strconv.Itoa(i)}

		task := replica.NewSnapshotHashTask(snapshotNames, false)
		task.State = replica.ProgressStateComplete

		list.Add(snapshotNames[0], task)

		size := list.GetSize()
		if i < MaxSnapshotHashTaskSize {
			c.Assert(size, Equals, i+1)
		} else {
			c.Assert(size, Equals, MaxSnapshotHashTaskSize)
		}
	}
}

func (s *TestSuite) TestSnapshotHashListRefreshTriggerByGet(c *C) {
	list := &SnapshotHashList{}

	numSnapshots := MaxSnapshotHashTaskSize + 1
	for i := 0; i < numSnapshots; i++ {
		snapshotNames := []string{"snapshot" + strconv.Itoa(i)}
		task := replica.NewSnapshotHashTask(snapshotNames, false)
		list.Add(snapshotNames[0], task)
	}

	for i := 0; i < numSnapshots; i++ {
		status, err := list.Get("snapshot" + strconv.Itoa(i))
		c.Assert(err, IsNil)

		status.State = replica.ProgressStateComplete

		// Try to trigger refresh
		_, err = list.Get("snapshot" + strconv.Itoa(i))
		c.Assert(err, IsNil)

		size := list.GetSize()
		if i < MaxSnapshotHashTaskSize {
			c.Assert(size, Equals, MaxSnapshotHashTaskSize+1)
		} else {
			c.Assert(size, Equals, MaxSnapshotHashTaskSize)
		}
	}
}
