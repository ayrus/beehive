package beehive

import (
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	etcdraft "github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/coreos/etcd/raft/raftpb"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
	bhflag "github.com/kandoo/beehive/flag"
	"github.com/kandoo/beehive/raft"
)

// Hive represents is the main active entity of beehive. It mananges all
// messages, apps and bees.
type Hive interface {
	// ID of the hive. Valid only if the hive is started.
	ID() uint64
	// Config returns the hive configuration.
	Config() HiveConfig

	// Start starts the hive. This function blocks.
	Start() error
	// Stop stops the hive and all its apps. It blocks until the hive is actually
	// stopped.
	Stop() error

	// Creates an app with the given name and the provided options.
	// Note that apps are not active until the hive is started.
	NewApp(name string, options ...AppOption) App

	// Emits a message containing msgData from this hive.
	Emit(msgData interface{})
	// Sends a message to a specific bee that owns a specific dictionary key.
	SendToCellKey(msgData interface{}, to string, dk CellKey)
	// Sends a message to a sepcific bee.
	SendToBee(msgData interface{}, to uint64)
	// Replies to a message.
	ReplyTo(msg Msg, replyData interface{}) error

	// Registers a message for encoding/decoding. This method should be called
	// only on messages that have no active handler. Such messages are almost
	// always replies to some detached handler.
	RegisterMsg(msg interface{})
}

// HiveConfig represents the configuration of a hive.
type HiveConfig struct {
	Addr      string   // listening address of the hive.
	PeerAddrs []string // peer addresses.
	RegAddrs  []string // reigstery service addresses.
	StatePath string   // where to store state data.

	DataChBufSize int // buffer size of the data channels.
	CmdChBufSize  int // buffer size of the control channels.
	BatchSize     int // number of messages to batch.

	Instrument     bool // whether to instrument apps on the hive.
	OptimizeThresh uint // when to notify the optimizer (in msg/s).

	RegLockTimeout time.Duration // when to retry to lock an entry in a registry.
	RaftTick       time.Duration // the raft tick interval.
	RaftHBTicks    int           // number of raft ticks that fires a heartbeat.
	RaftElectTicks int           // number of raft ticks that fires election.

	MaxConnPerHost int           // max parallel data connections to a host.
	ConnTimeout    time.Duration // timeout for connections between hives.
	BatcherPerHost int           // number of parallel batchers per host.
	BatcherTimeout time.Duration // timeout used in the batchers.
}

// RaftElectTimeout returns the raft election timeout as
// RaftTick*RaftElectTicks.
func (c HiveConfig) RaftElectTimeout() time.Duration {
	return time.Duration(c.RaftElectTicks) * c.RaftTick
}

// NewHiveWithConfig creates a new hive based on the given configuration.
func NewHiveWithConfig(cfg HiveConfig) Hive {
	if !flag.Parsed() {
		flag.Parse()
	}

	os.MkdirAll(cfg.StatePath, 0700)
	m := meta(cfg)
	h := &hive{
		id:     m.Hive.ID,
		meta:   m,
		status: hiveStopped,
		config: cfg,
		dataCh: newMsgChannel(cfg.DataChBufSize),
		ctrlCh: make(chan cmdAndChannel),
		apps:   make(map[string]*app, 0),
		qees:   make(map[string][]qeeAndHandler),
		ticker: time.NewTicker(cfg.RaftTick),
		client: newHTTPClient(cfg.ConnTimeout),
	}

	h.streamer = newLoadBalancer(h, cfg.BatcherPerHost)
	h.registry = newRegistry(h.String())
	h.replStrategy = newRndReplication(h)
	h.server = newServer(h, cfg.Addr)

	if h.config.Instrument {
		h.collector = newAppStatCollector(h)
	} else {
		h.collector = &noOpStatCollector{}
	}
	return h
}

// DefaultCfg is the default configration for hives in beehive.
var DefaultCfg = HiveConfig{}

// NewHive creates a new hive and load its configuration from command line
// flags.
func NewHive() Hive {
	if !flag.Parsed() {
		flag.Parse()
	}

	return NewHiveWithConfig(DefaultCfg)
}

func init() {
	flag.StringVar(&DefaultCfg.Addr, "laddr", "localhost:7767",
		"the listening address used to communicate with other nodes")
	flag.Var(&bhflag.CSV{S: &DefaultCfg.PeerAddrs}, "paddrs",
		"address of peers. Seperate entries with a comma")
	flag.Var(&bhflag.CSV{S: &DefaultCfg.RegAddrs}, "raddrs",
		"address of etcd machines. Separate entries with a comma ','")
	flag.IntVar(&DefaultCfg.DataChBufSize, "chsize", 1024,
		"buffer size of data channels")
	flag.IntVar(&DefaultCfg.CmdChBufSize, "cmdchsize", 128,
		"buffer size of command channels")
	flag.IntVar(&DefaultCfg.BatchSize, "batch", 1024,
		"number of messages to batch per transaction")
	flag.BoolVar(&DefaultCfg.Instrument, "instrument", false,
		"whether to insturment apps")
	flag.UintVar(&DefaultCfg.OptimizeThresh, "optthresh", 10,
		"when the local stat collector should notify the optimizer (in msg/s).")
	flag.StringVar(&DefaultCfg.StatePath, "statepath", "/tmp/beehive",
		"where to store persistent state data")
	flag.DurationVar(&DefaultCfg.RegLockTimeout, "reglocktimeout",
		10*time.Millisecond, "timeout to retry locking an entry in the registry")
	flag.DurationVar(&DefaultCfg.RaftTick, "rafttick", 100*time.Millisecond,
		"raft tick period")
	flag.IntVar(&DefaultCfg.RaftElectTicks, "raftelectionticks", 5,
		"number of raft ticks to start an election (ie, election timeout)")
	flag.IntVar(&DefaultCfg.RaftHBTicks, "rafthbticks", 1,
		"number of raft ticks to fire a heartbeat (ie, heartbeat timeout)")
	flag.IntVar(&DefaultCfg.MaxConnPerHost, "maxconn", 32,
		"maximum number of parallel data connectons to a remote host")
	flag.DurationVar(&DefaultCfg.ConnTimeout, "conntimeout", 60*time.Second,
		"timeout for trying to connect to other hives")
	flag.IntVar(&DefaultCfg.BatcherPerHost, "batchers", 1,
		"number of parallel batchers per host")
	flag.DurationVar(&DefaultCfg.BatcherTimeout, "batchertimeout",
		1*time.Millisecond, "timeout used for batching")
}

type qeeAndHandler struct {
	q *qee
	h Handler
}

// hiveStatus represents the status of a hive.
type hiveStatus int

// Valid values for HiveStatus.
const (
	hiveStopped hiveStatus = iota
	hiveStarted
)

// The internal implementation of Hive.
type hive struct {
	sync.Mutex

	id     uint64
	meta   hiveMeta
	config HiveConfig

	status hiveStatus

	dataCh *msgChannel
	ctrlCh chan cmdAndChannel
	sigCh  chan os.Signal

	apps map[string]*app
	qees map[string][]qeeAndHandler

	server   *server
	listener net.Listener

	node     *raft.Node
	registry *registry
	ticker   *time.Ticker
	client   *http.Client
	streamer streamer

	replStrategy replicationStrategy
	collector    collector
}

func (h *hive) ID() uint64 {
	return h.id
}

func (h *hive) String() string {
	return fmt.Sprintf("hive %v@%v", h.id, h.config.Addr)
}

func (h *hive) Config() HiveConfig {
	return h.config
}

func (h *hive) RegisterMsg(msg interface{}) {
	gob.Register(msg)
}

func (h *hive) app(name string) (*app, bool) {
	a, ok := h.apps[name]
	return a, ok
}

func (h *hive) hiveAddr(id uint64) (string, error) {
	i, err := h.registry.hive(id)
	return i.Addr, err
}

func (h *hive) stopListener() {
	glog.Infof("%v closes listener...", h)
	if h.listener != nil {
		h.listener.Close()
	}
}

func (h *hive) stopQees() {
	glog.Infof("%v is stopping qees...", h)
	qs := make(map[*qee]bool)
	for _, mhs := range h.qees {
		for _, mh := range mhs {
			qs[mh.q] = true
		}
	}

	stopCh := make(chan cmdResult)
	for q := range qs {
		q.ctrlCh <- newCmdAndChannel(cmdStop{}, q.app.Name(), 0, stopCh)
		glog.V(3).Infof("Waiting on a qee: %v", q)
		stopped := false
		tries := 5
		for !stopped {
			select {
			case res := <-stopCh:
				_, err := res.get()
				if err != nil {
					glog.Errorf("Error in stopping a qee: %v", err)
				}
				stopped = true
			case <-time.After(1 * time.Second):
				if tries--; tries < 0 {
					glog.Infof("Giving up on qee %v", q)
					stopped = true
					continue
				}
				glog.Infof("Still waiting for a qee %v...", q)
			}
		}
	}
}

func (h *hive) handleCmd(cc cmdAndChannel) {
	glog.V(2).Infof("%v handles cmd %+v", h, cc.cmd)
	switch d := cc.cmd.Data.(type) {
	case cmdStop:
		// TODO(soheil): This has a race with Stop(). Use atomics here.
		h.status = hiveStopped
		h.stopListener()
		h.stopQees()
		h.node.Stop()
		cc.ch <- cmdResult{}

	case cmdPing:
		cc.ch <- cmdResult{}

	case cmdSync:
		err := h.raftBarrier()
		cc.ch <- cmdResult{Err: err}

	case cmdNewHiveID:
		r, err := h.node.Process(context.TODO(), newHiveID{d.Addr})
		cc.ch <- cmdResult{
			Data: r,
			Err:  err,
		}

	case cmdAddHive:
		err := h.node.AddNode(context.TODO(), d.Info.ID, d.Info.Addr)
		cc.ch <- cmdResult{
			Err: err,
		}

	case cmdLiveHives:
		cc.ch <- cmdResult{
			Data: h.registry.hives(),
		}

	default:
		cc.ch <- cmdResult{
			Err: ErrInvalidCmd,
		}
	}
}

func (h *hive) processCmd(data interface{}) (interface{}, error) {
	ch := make(chan cmdResult)
	h.ctrlCh <- newCmdAndChannel(data, "", 0, ch)
	return (<-ch).get()
}

func (h *hive) stepRaft(ctx context.Context, msg raftpb.Message) error {
	return h.node.Step(ctx, msg)
}

func (h *hive) raftBarrier() error {
	ctx, _ := context.WithTimeout(context.Background(), 300*h.config.RaftTick)
	_, err := h.node.Process(ctx, noOp{})
	return err
}

func (h *hive) registerApp(a *app) {
	h.apps[a.Name()] = a
}

func (h *hive) registerHandler(t string, q *qee, l Handler) {
	for i, qh := range h.qees[t] {
		if qh.q == q {
			h.qees[t][i].h = l
			return
		}
	}

	h.qees[t] = append(h.qees[t], qeeAndHandler{q, l})
}

func (h *hive) bee(id uint64) (BeeInfo, error) {
	return h.registry.bee(id)
}

func (h *hive) handleMsg(m *msg) {
	switch {
	case m.IsUnicast():
		i, err := h.bee(m.MsgTo)
		if err != nil {
			glog.Errorf("no such bee %v", m.MsgTo)
		}
		a, ok := h.app(i.App)
		if !ok {
			glog.Fatalf("no such application %s", i.App)
		}
		if i.Detached {
			a.qee.enqueMsg(msgAndHandler{msg: m})
			return
		}
		a.qee.enqueMsg(msgAndHandler{msg: m, handler: a.handler(m.Type())})
	default:
		for _, qh := range h.qees[m.Type()] {
			qh.q.enqueMsg(msgAndHandler{m, qh.h})
		}
	}
}

func (h *hive) startQees() {
	for _, a := range h.apps {
		go a.qee.start()
	}
}

func (h *hive) startRaftNode() {
	peers := make([]etcdraft.Peer, 0, 1)
	if len(h.meta.Peers) != 0 {
		h.registry.initHives(h.meta.Peers)
	} else {
		peers = append(peers, raft.NodeInfo(h.info()).Peer())
	}
	h.node = raft.NewNode(h.String(), h.id, peers, h.sendRaft, h,
		h.config.StatePath, h.registry, 1024, h.ticker.C, h.config.RaftElectTicks,
		h.config.RaftHBTicks)
}

func (h *hive) delBeeFromRegistry(id uint64) error {
	_, err := h.node.Process(context.TODO(), delBee(id))
	if err != nil {
		glog.Errorf("%v cannot delete bee %v from registory", h, id)
	}
	return err
}

func (h *hive) reloadState() {
	for _, b := range h.registry.beesOfHive(h.id) {
		if b.Detached || b.Colony.IsNil() {
			glog.V(1).Infof(
				"%v will not reload detached bee %v (detached=%v, colony=%#v)", h, b.ID,
				b.Detached, b.Colony)
			h.delBeeFromRegistry(b.ID)
			continue
		}
		a, ok := h.app(b.App)
		if !ok {
			glog.Errorf("app %v is not registered but has a bee", b.App)
			continue
		}
		_, err := a.qee.processCmd(cmdReloadBee{ID: b.ID, Colony: b.Colony})
		if err != nil {
			glog.Errorf("cannot reload bee %v on %v", b.ID, h.id)
			continue
		}
	}
}

func (h *hive) ProcessStatusChange(sch interface{}) {
	switch ev := sch.(type) {
	case raft.LeaderChanged:
		if ev.New != h.ID() {
			return
		}
		glog.V(2).Infof("%v is the new leader", h)
	}
}

func (h *hive) Start() error {
	h.status = hiveStarted
	h.registerSignals()
	h.startRaftNode()
	if err := h.listen(); err != nil {
		glog.Errorf("%v cannot start listener: %v", h, err)
		h.Stop()
		return err
	}
	if err := h.raftBarrier(); err != nil {
		glog.Fatalf("error when joining the cluster: %v", err)
	}
	glog.V(2).Infof("%v is in sync with the cluster", h)
	h.startQees()
	h.reloadState()

	glog.V(2).Infof("%v starts message loop", h)
	dataCh := h.dataCh.out()
	for h.status == hiveStarted {
		select {
		case m := <-dataCh:
			h.handleMsg(m.msg)

		case cmd := <-h.ctrlCh:
			h.handleCmd(cmd)
		}
	}

	return nil
}

func (h *hive) info() HiveInfo {
	return HiveInfo{
		ID:   h.id,
		Addr: h.config.Addr,
	}
}

func (h *hive) Stop() error {
	glog.Infof("stopping %v", h)
	if h.ctrlCh == nil {
		return errors.New("control channel is closed")
	}

	if h.status == hiveStopped {
		return errors.New("hive is already stopped")
	}

	_, err := h.sendCmd(cmdStop{})
	return err
}

func (h *hive) waitUntilStarted() {
	h.sendCmd(cmdPing{})
}

func (h *hive) sendCmd(cmd interface{}) (interface{}, error) {
	ch := make(chan cmdResult)
	h.ctrlCh <- newCmdAndChannel(cmd, "", 0, ch)
	return (<-ch).get()
}

func (h *hive) NewApp(name string, options ...AppOption) App {
	a := &app{
		name:     name,
		hive:     h,
		handlers: make(map[string]Handler),
	}
	a.initQee()
	h.registerApp(a)

	if len(options) == 0 {
		options = defaultAppOptions
	}

	for _, opt := range options {
		opt(a)
	}

	return a
}

func (h *hive) Emit(msgData interface{}) {
	h.enqueMsg(&msg{MsgData: msgData})
}

func (h *hive) enqueMsg(msg *msg) {
	h.dataCh.in() <- msgAndHandler{msg: msg}
}

func (h *hive) SendToCellKey(msgData interface{}, to string, k CellKey) {
	// TODO(soheil): Implement this hive.SendTo.
	glog.Fatalf("FIXME implement SendToCellKey")
}

func (h *hive) SendToBee(msgData interface{}, to uint64) {
	h.enqueMsg(newMsgFromData(msgData, 0, to))
}

// Reply to thatMsg with the provided replyData.
func (h *hive) ReplyTo(thatMsg Msg, replyData interface{}) error {
	m := thatMsg.(*msg)
	if m.NoReply() {
		return errors.New("cannot reply to this message")
	}

	h.enqueMsg(newMsgFromData(replyData, 0, m.From()))
	return nil
}

func (h *hive) registerSignals() {
	h.sigCh = make(chan os.Signal, 1)
	signal.Notify(h.sigCh,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		<-h.sigCh
		h.Stop()
	}()
}

func (h *hive) listen() error {
	l, e := net.Listen("tcp", h.config.Addr)
	if e != nil {
		glog.Errorf("%v cannot listen: %v", h, e)
		return e
	}
	glog.Infof("%v listens", h)
	h.listener = l

	go func() {
		h.server.Serve(l)
		glog.Infof("%v closed listener", h)
	}()
	return nil
}

func (h *hive) newProxyToHive(to uint64) (*proxy, error) {
	// TODO(soheil): maybe we should use the cache here.
	return h.newProxyToHiveWithRetry(to, 0, 1)
}

func (h *hive) newProxyToHiveWithRetry(to uint64, backoffStep time.Duration,
	maxRetries uint32) (*proxy, error) {
	// TODO(soheil): maybe we should use the cache here.
	a, err := h.hiveAddr(to)
	if err != nil {
		return nil, err
	}
	return newProxyWithRetry(h.client, a, backoffStep, maxRetries), nil
}

func (h *hive) sendRaft(msgs []raftpb.Message) {
	if len(msgs) == 0 {
		return
	}

	if err := h.streamer.sendRaft(msgs); err != nil {
		glog.Errorf("%v cannot send raft messages: %v", h, err)
	}
}
