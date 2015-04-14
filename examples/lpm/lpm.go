package lpm

import (
	"encoding/gob"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/OneOfOne/xxhash"
	bh "github.com/kandoo/beehive"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/gorilla/mux"
	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
)

const (
	dict = "store"
)

const L = 0 // Verbosity level

var (
	errKeyNotFound = errors.New("lpm: key not found")
	errInternal    = errors.New("lpm: internal error")
	errInvalid     = errors.New("lpm: invalid parameter")
)

type Put Route

type Get net.IP

type Del struct {
	Dest  net.IP `json:"dest"`
	Len   int    `json:"len"`
	Exact bool   `json:"exact"`
}

type getKey string

type delKey string

type warmup struct {
	bnum int
}

type Route struct {
	Dest     net.IP `json:"dest"`
	Len      int    `json:"len"`
	Name     string `json:"name"`
	Priority int    `json:"priority"`
}

type baseHandler struct {
	*bh.Sync
	Buckets uint64
}

func (s *baseHandler) hash(str string) string {
	return strconv.FormatUint(xxhash.Checksum64([]byte(str))%s.Buckets, 16)
}

func iplen(ip net.IP) int {
	if ip.To4() != nil {
		return net.IPv4len
	} else {
		return net.IPv6len
	}
}

func getRouteKey(rt Route) string {
	msk := net.CIDRMask(rt.Len, iplen(rt.Dest)*8)
	k := rt.Dest.Mask(msk).String() + "/" + strconv.FormatInt(int64(rt.Len), 10)
	return k
}

func getDelKey(dl Del) string {
	msk := net.CIDRMask(dl.Len, iplen(dl.Dest)*8)
	k := dl.Dest.Mask(msk).String() + "/" + strconv.FormatInt(int64(dl.Len), 10)
	return k
}

type putHandler struct {
	*baseHandler
}

func (s *putHandler) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	data := msg.Data().(Put)
	rt1 := Route(data)
	var rt2 Route
	err := ctx.Dict(dict).GetGob(getRouteKey(rt1), &rt2)
	if err == nil {
		if rt1.Priority > rt2.Priority {
			glog.V(L).Infof("Inserted %s\n", getRouteKey(rt1))
			return ctx.Dict(dict).PutGob(getRouteKey(rt1), &rt1)
		} else {
			return nil
		}
	} else {
		glog.V(L).Infof("Inserted %s\n", getRouteKey(rt1))
		return ctx.Dict(dict).PutGob(getRouteKey(rt1), &rt1)
	}
}

func (s *putHandler) Map(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	return bh.MappedCells{
		{
			Dict: dict,
			Key:  s.hash(getRouteKey(Route(msg.Data().(Put)))),
		},
	}
}

type getHandler struct {
	*baseHandler
}

func (s *getHandler) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	data := msg.Data().(Get)

	glog.V(L).Infoln("Received Get request")

	netctx, cnl := context.WithCancel(context.Background())
	defer cnl()

	ip := net.IP(data)
	ln := iplen(ip) * 8
	chnl := make(chan interface{})

	for i := ln; i >= 0; i-- {
		mask := net.CIDRMask(i, ln)
		req := ip.Mask(mask).String() + "/" + strconv.FormatInt(int64(i), 10)
		//TODO: Remove goroutines later
		go func(req string) {
			res, err := s.Process(netctx, getKey(req))

			if err == nil {
				chnl <- res
			} else {
				chnl <- nil
			}
		}(req)
	}

	var res interface{}

	best_pri := -1
	best_len := -1

	for i := 0; i < ln; i++ {
		x := <-chnl
		if x != nil {
			rt := x.(Route)
			if rt.Priority > best_pri || (rt.Priority == best_pri && rt.Len > best_len) {
				res = rt
				best_pri = rt.Priority
				best_len = rt.Len
			}
			glog.V(L).Infof("Candidate: %s\n", rt.Name)
		}
	}

	ctx.ReplyTo(msg, res)

	return nil

}

func (s *getHandler) Map(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	return bh.MappedCells{
		{
			Dict: dict,
			Key:  strconv.FormatInt(int64(rand.Intn(int(s.Buckets)))+int64(s.Buckets), 16),
		},
	}
}

type delHandler struct {
	*baseHandler
}

func (s *delHandler) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	data := msg.Data().(Del)

	dl := Del(data)

	glog.V(L).Infof("Received Delete Request")

	if !dl.Exact {
		for i := dl.Len; i <= iplen(dl.Dest)*8; i++ {
			msk := net.CIDRMask(i, iplen(dl.Dest)*8)
			ck := dl.Dest.Mask(msk).String() + "/" + strconv.FormatInt(int64(i), 10)
			ctx.Emit(delKey(ck))
		}
	} else {
		ctx.Emit(delKey(getDelKey(dl)))
	}
	return nil

}

type getKeyHandler struct {
	*baseHandler
}

func (s *getKeyHandler) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	data := msg.Data().(getKey)
	var rt Route
	err := ctx.Dict(dict).GetGob(string(data), &rt)
	glog.V(L+1).Infof("Looking up - %s\n", data)
	if err == nil {
		ctx.ReplyTo(msg, rt)
	} else {
		ctx.ReplyTo(msg, nil)
	}

	return nil
}

func (s *getKeyHandler) Map(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	return bh.MappedCells{
		{
			Dict: dict,
			Key:  s.hash(string(msg.Data().(getKey))),
		},
	}
}

func (s *delHandler) Map(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	return bh.MappedCells{
		{
			Dict: dict,
			Key:  s.hash(getDelKey(Del(msg.Data().(Del)))),
		},
	}
}

type delKeyHandler struct {
	*baseHandler
}

func (s *delKeyHandler) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	data := msg.Data().(delKey)

	dk := string(data)
	glog.V(L).Infoln("Deleting!", dk)

	return ctx.Dict(dict).Del(dk)
}

func (s *delKeyHandler) Map(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	return bh.MappedCells{
		{
			Dict: dict,
			Key:  s.hash(string(msg.Data().(delKey))),
		},
	}
}

type warmupHandler struct {
	*baseHandler
}

func (s *warmupHandler) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	data := msg.Data().(warmup)
	glog.V(L).Infof("Created bee #%d", data.bnum)
	return nil
}

func (s *warmupHandler) Map(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	return bh.MappedCells{
		{
			Dict: dict,
			Key:  strconv.FormatUint(uint64(msg.Data().(warmup).bnum), 16),
		},
	}
}

func (s *baseHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	glog.V(L).Infoln("Received HTTP request")

	k, ok := mux.Vars(r)["key"]
	if !ok {
		http.Error(w, "no key in the url", http.StatusBadRequest)
		return
	}

	ctx, cnl := context.WithCancel(context.Background())
	defer cnl()

	var res interface{}
	var err error

	switch r.Method {
	case "GET":
		res, err = s.Process(ctx, Get(net.ParseIP(k)))
		glog.V(L).Infoln(res)
	case "PUT":
		var v []byte
		var rt Route
		v, err = ioutil.ReadAll(r.Body)
		if err != nil {
			break
		}

		glog.V(L).Infoln("HTTP Received Put")
		err = json.Unmarshal(v, &rt)
		if err != nil {
			break
		}

		res, err = s.Process(ctx, Put(rt))
	case "DELETE":
		var v []byte
		var dl Del
		v, err = ioutil.ReadAll(r.Body)
		if err != nil {
			break
		}

		err = json.Unmarshal(v, &dl)
		if err != nil {
			break
		}

		res, err = s.Process(ctx, Del(dl))
	}

	if err != nil {
		glog.V(L).Infof("HTTP err: %s\n", err.Error())
		switch {
		case err.Error() == errKeyNotFound.Error():
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		case err.Error() == errInternal.Error():
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		case err.Error() == errInvalid.Error():
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		default:
			http.Error(w, err.Error(), 400)
			return
		}
	}

	if res == nil {
		return
	}

	js, err := json.Marshal(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

type LPMOptions struct {
	ReplFactor int  // Replication factor
	Buckets    int  // Number of buckets
	Raftlog    bool // Whether to print raft log
	Lg         bool // Whether to print lpm log TODO
	Random     bool // Whether to use random placement
	Warmup     bool // Whether to warm up beehive before processing requests
}

func NewLPMOptions() *LPMOptions {
	return &LPMOptions{ReplFactor: 3, Buckets: 5, Raftlog: false, Lg: true, Random: false, Warmup: true}
}

func Install(hive bh.Hive, options LPMOptions) *bh.Sync {
	rand.Seed(time.Now().UnixNano())

	if !options.Raftlog {
		log.SetOutput(ioutil.Discard)
	}

	opts := []bh.AppOption{bh.Persistent(options.ReplFactor)}
	if options.Random {
		rp := bh.RandomPlacement{
			Rand: rand.New(rand.NewSource(time.Now().UnixNano())),
		}
		opts = append(opts, bh.AppWithPlacement(rp))
	}
	a := hive.NewApp("lpm", opts...)
	s := bh.NewSync(a)

	buckets := uint64(options.Buckets)

	handler := &baseHandler{
		Sync:    s,
		Buckets: buckets,
	}

	s.Handle(warmup{}, &warmupHandler{handler})
	s.Handle(Get{}, &getHandler{handler})
	s.Handle(Put{}, &putHandler{handler})
	s.Handle(getKey(""), &getKeyHandler{handler})
	s.Handle(Del{}, &delHandler{handler})
	s.Handle(delKey(""), &delKeyHandler{handler})

	a.Handle(warmup{}, &warmupHandler{handler})
	a.Handle(Get{}, &getHandler{handler})
	a.Handle(Put{}, &putHandler{handler})
	a.Handle(getKey(""), &getKeyHandler{handler})
	a.Handle(Del{}, &delHandler{handler})
	a.Handle(delKey(""), &delKeyHandler{handler})

	a.HandleHTTP("/{key}", handler)

	go func() {
		ctx, cnl := context.WithCancel(context.Background())
		defer cnl()

		if options.Warmup && options.Buckets > 0 {
			for i := 0; i < options.Buckets*2; i++ {
				s.Process(ctx, warmup{i})
			}

		}
	}()

	return s
}

func init() {
	gob.Register(Route{})
}
