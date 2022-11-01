package discovery

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type handler struct {
	joins  chan map[string]string
	leaves chan string
}

func (h *handler) Join(id, addr string) error {
	if h.joins != nil {
		h.joins <- map[string]string{
			"id":   id,
			"addr": addr,
		}
	}
	return nil
}

func (h *handler) Leave(id string) error {
	if h.leaves != nil {
		h.leaves <- id
	}
	return nil
}

func TestMembership(t *testing.T) {
	m, h := setupMember(t, nil)
	m, _ = setupMember(t, m)
	m, _ = setupMember(t, m)

	require.Eventually(t, func() bool {
		return 2 == len(h.joins) && 0 == len(h.leaves) && 3 == len(m[0].Members())
	}, 3*time.Second, 250*time.Millisecond)

}

func setupMember(t *testing.T, members []*Membership) ([]*Membership, *handler) {
	t.Helper()
	id := len(members)
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", 9001+id-1)
	tags := map[string]string{
		"rpc_addr": addr,
	}

	c := Config{
		NodeName: fmt.Sprintf("%d", id),
		BindAddr: addr,
		Tags:     tags,
	}
	h := &handler{}

	if len(members) == 0 {
		h.joins = make(chan map[string]string, 3)
		h.leaves = make(chan string, 3)
	} else {
		c.StartJoinAddr = []string{members[0].Config.BindAddr}
	}

	member, err := New(h, c)
	require.NoError(t, err)
	members = append(members, member)
	return members, h
}
