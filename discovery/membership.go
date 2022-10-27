package discovery

import (
	"fmt"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
	"net"
)

type Config struct {
	NodeName      string
	BindAddr      string
	Tags          map[string]string
	StartJoinAddr []string
	Logger        *zap.Logger
}

type Membership struct {
	Config
	Handler Handler
	Serf    *serf.Serf
	eventCh chan serf.Event
}

type Handler interface {
	Join(name string, addr string) error
	Leave(name string) error
}

func New(handler Handler, config Config) (*Membership, error) {
	m := &Membership{
		Config:  config,
		Handler: handler,
		eventCh: make(chan serf.Event),
	}
	if err := m.setupSerf(); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *Membership) setupSerf() error {
	addr, err := net.ResolveTCPAddr("tcp", m.Config.BindAddr)
	if err != nil {
		return err
	}

	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port

	config.EventCh = m.eventCh
	config.Tags = m.Config.Tags
	config.NodeName = m.Config.NodeName

	m.Serf, err = serf.Create(config)

	go m.eventHandler()
	if m.StartJoinAddr != nil {
		_, err := m.Serf.Join(m.StartJoinAddr, true)
		if err != nil {
			return err
		}
	}
	return err
}

func (m *Membership) eventHandler() {
	for event := range m.eventCh {
		fmt.Println("event ")
		switch event.EventType() {
		case serf.EventMemberJoin:
			for _, member := range event.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberFailed, serf.EventMemberLeave:
			for _, member := range event.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					return
				}
				m.handelLeave(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	if err := m.Handler.Join(member.Name, member.Tags["rpc_addr"]); err != nil {
		m.Config.Logger.Error(
			"failed join.",
			zap.Error(err),
			zap.String("name", member.Name),
			zap.String("rpc_addr", member.Tags["rpc_addr"]),
		)
	}
}

func (m *Membership) handelLeave(member serf.Member) {
	if err := m.Handler.Leave(member.Name); err != nil {
		m.Config.Logger.Error(
			"leave failed",
			zap.Error(err),
			zap.String("name", member.Name),
		)
	}
}

func (m *Membership) isLocal(member serf.Member) bool {
	return m.Serf.LocalMember().Name == member.Name
}

func (m *Membership) Members() []serf.Member {
	return m.Serf.Members()
}

func (m *Membership) Leave() error {
	return m.Serf.Leave()
}
