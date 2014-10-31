package command

import (
	"fmt"
	"io"
	"math/rand"
	"os"

	"github.com/coreos/etcd/Godeps/_workspace/src/github.com/codegangsta/cli"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/wal"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
)

func NewBackupCommand() cli.Command {
	return cli.Command{
		Name:  "backup",
		Usage: "backup an etcd directory",
		Action: handleBackup,
	}
}

func cp(dst, src string) error {
	s, err := os.Open(src)
	if err != nil {
		return err
	}
	// no need to check errors on read only file, we already got everything
	// we need from the filesystem, so nothing can go wrong now.
	defer s.Close()
	d, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(d, s); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}

/*
[]pb.Entry{{Type: pb.EntryConfChange, Data: data}}

	cc := raftpb.ConfChange{
		ID:     GenID(),
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: id,
	}

*/



// handleLs handles a request that intends to do ls-like operations.
func handleBackup(c *cli.Context) {
	ids := map[uint64]bool{}
	newNodeID := uint64(rand.Int63())
	w, err := wal.OpenAtIndex("default.etcd/wal", 0)
	wmetadata, st, ents, err := w.ReadAll()

	var metadata pb.Metadata
	pbutil.MustUnmarshal(&metadata, wmetadata)
	fmt.Printf("member=%x cluster=%x", metadata.NodeID, metadata.ClusterID, st.Commit)

	term := uint64(0)
	index := uint64(0)
	for i := range ents {
		e := ents[i]
		term = e.Term
		index = e.Term
		switch e.Type {
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			pbutil.MustUnmarshal(&cc, e.Data)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				ids[cc.NodeID] = true
			case raftpb.ConfChangeRemoveNode:
				delete(ids, cc.NodeID)
			}
		}
	}

	removals := []raftpb.Entry{}
	for id := range ids {
		cc := raftpb.ConfChange{
			ID:     0,
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: id,
		}
		d, err := cc.Marshal()
		if err != nil {
			panic("unexpected marshal error")
		}
		index = index + 1
		e := raftpb.Entry{Type: raftpb.EntryConfChange, Term: term, Index: index, Data: d}
		removals = append(removals, e)
	}

	index = index + 1
	cc := raftpb.ConfChange{
		ID:     0,
		Type:   raftpb.ConfChangeAddNode,
		NodeID: newNodeID,
	}
	d, err := cc.Marshal()
	if err != nil {
		panic("unexpected marshal error")
	}
	index = index + 1
	e := raftpb.Entry{Type: raftpb.EntryConfChange, Term: term, Index: index, Data: d}
	removals = append(removals, e)

	metabytes := pbutil.MustMarshal(&pb.Metadata{NodeID: newNodeID, ClusterID: uint64(rand.Int63())})
	wnew, err := wal.Create("backup.etcd/wal", metabytes)
	wnew.Save(st, append(ents, removals...))
	if err != nil {
		panic(err)
	}

}
