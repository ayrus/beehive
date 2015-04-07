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
	"os"
	"strconv"
	"time"

	"github.com/OneOfOne/xxhash"
	bh "github.com/kandoo/beehive"
	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/gorilla/mux"
	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
)

const (
	dict = "store"
)

var (
	errKeyNotFound = errors.New("lpm: key not found")
	errInternal    = errors.New("lpm: internal error")
	errInvalid     = errors.New("lpm: invalid parameter")
)

var lpmlog *log.Logger

type Put Route

type get string

type Del string

type lpm struct {
	*bh.Sync
	buckets uint64
}

type warmup struct {
	bnum int
}

type CalcLPM net.IP

type Route struct {
	Dest     net.IP `json:"dest"`
	Len      int    `json:"len"`
	Name     string `json:"name"`
	Priority int    `json:"priority"`
}

func unmarshal(data []byte) Route {
	var rt Route
	var terr error

	terr = json.Unmarshal(data, &rt)
	if terr != nil {
		lpmlog.Println("Unmarshal error: ", terr)
	}

	return rt
}

func iplen(ip net.IP) int {
	if ip.To4() != nil {
		return net.IPv4len
	} else {
		return net.IPv6len
	}
}

func getKey(rt Route) string {
	msk := net.CIDRMask(rt.Len, iplen(rt.Dest)*8)
	k := rt.Dest.Mask(msk).String() + "/" + strconv.FormatInt(int64(rt.Len), 10)
	return k
}

func (s *lpm) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	switch data := msg.Data().(type) {

	case Put:
		rt := Route(data)
		lpmlog.Printf("Inserted %s\n", getKey(rt))
		return ctx.Dict(dict).PutGob(getKey(rt), &rt)

	case get:
		var rt Route
		err := ctx.Dict(dict).GetGob(string(data), &rt)
		lpmlog.Printf("Looking up - %s\n", data)
		if err == nil {
			ctx.ReplyTo(msg, rt)
		} else {
			ctx.ReplyTo(msg, nil)
		}

		return nil

	case Del:
		lpmlog.Printf("Delete %s\n", data)
		return ctx.Dict(dict).Del(string(data))

	case warmup:
		lpmlog.Printf("Created bee #%d", data.bnum)
		return nil

	case CalcLPM:
		lpmlog.Println("Received CalcLPM request")

		netctx, cnl := context.WithCancel(context.Background())
		var res interface{}
		var err error

		ip := net.IP(data)
		ln := iplen(ip) * 8
		chnl := make(chan interface{})

		for i := ln; i >= 0; i-- {
			mask := net.CIDRMask(i, ln)
			req := ip.Mask(mask).String() + "/" + strconv.FormatInt(int64(i), 10)
			go func(req string) {
				res, err = s.Process(netctx, get(req))

				if err == nil {
					chnl <- res
				} else {
					chnl <- nil
				}
			}(req)
		}

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
				lpmlog.Printf("Candidate: %s\n", rt)
			}
		}

		ctx.ReplyTo(msg, res)

		cnl()

		return nil
	}
	return errInvalid
}

func (s *lpm) Map(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	var k string

	switch data := msg.Data().(type) {
	case Put:
		k = getKey(Route(data))
		k = strconv.FormatUint(xxhash.Checksum64([]byte(k))%s.buckets, 16)
	case get:
		k = string(data)
		k = strconv.FormatUint(xxhash.Checksum64([]byte(k))%s.buckets, 16)
	case Del:
		k = string(data)
		k = strconv.FormatUint(xxhash.Checksum64([]byte(k))%s.buckets, 16)
	case warmup:
		k = strconv.FormatUint(uint64(data.bnum), 16)
	case CalcLPM:
		k = strconv.FormatInt(int64(rand.Intn(int(s.buckets)))+int64(s.buckets), 16)
	}

	cells := bh.MappedCells{
		{
			Dict: dict,
			Key:  k,
		},
	}
	return cells
}

func (s *lpm) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	lpmlog.Println("Received HTTP request")

	k, ok := mux.Vars(r)["key"]
	if !ok {
		http.Error(w, "no key in the url", http.StatusBadRequest)
		return
	}

	ctx, cnl := context.WithCancel(context.Background())

	var res interface{}
	var err error

	switch r.Method {
	case "GET":
		res, err = s.Process(ctx, CalcLPM(net.ParseIP(k)))
		lpmlog.Println(res)
	case "PUT":
		var v []byte
		v, err = ioutil.ReadAll(r.Body)
		res, err = s.Process(ctx, Put(unmarshal(v)))
	case "DELETE":
		var v []byte
		v, err = ioutil.ReadAll(r.Body)
		rt := unmarshal(v)

		res, err = s.Process(ctx, Del(getKey(rt)))
	}

	cnl()

	if err != nil {
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
	replFactor int  //= flag.Int("lpm.rf", 3, "replication factor")
	buckets    int  //= flag.Int("lpm.b", 50, "number of buckets")
	raftlog    bool //= flag.Bool("lpm.raftlog", false, "whether to print raft log")
	lg         bool //            = flag.Bool("lpm.log", false, "whether to print lpm log")
	random     bool //= flag.Bool("lpm.rand", false, "whether to use random placement")
	warmup     bool //= flag.Bool("lpm.warmup", false, "whether to warm up beehive before processing requests")
}

func NewLPMOptions() *LPMOptions {
	return &LPMOptions{replFactor: 3, buckets: 5, raftlog: false, lg: false, random: false, warmup: true}
}

func Install(hive bh.Hive, options LPMOptions) {
	rand.Seed(time.Now().UnixNano())

	if !options.raftlog {
		log.SetOutput(ioutil.Discard)
	}

	lpmlog = log.New(os.Stderr, "LPM: ", 0)

	opts := []bh.AppOption{bh.Persistent(options.replFactor)}
	if options.random {
		rp := bh.RandomPlacement{
			Rand: rand.New(rand.NewSource(time.Now().UnixNano())),
		}
		opts = append(opts, bh.AppWithPlacement(rp))
	}
	a := hive.NewApp("lpm", opts...)
	s := bh.NewSync(a)

	kv := &lpm{
		Sync:    s,
		buckets: uint64(options.buckets),
	}

	s.Handle(warmup{}, kv)
	s.Handle(CalcLPM{}, kv)
	s.Handle(Put{}, kv)
	s.Handle(get(""), kv)
	s.Handle(Del(""), kv)

	a.Handle(CalcLPM{}, kv)
	a.Handle(Put{}, kv)
	a.Handle(get(""), kv)
	a.Handle(Del(""), kv)
	a.HandleHTTP("", kv)
	a.HandleHTTP("/{key}", kv)

	go func() {
		ctx, cnl := context.WithCancel(context.Background())

		if options.warmup && options.buckets > 0 {
			for i := 0; i < options.buckets*2; i++ {
				s.Process(ctx, warmup{i})
			}

		}

		cnl()
	}()
}

func init() {
	gob.Register(Route{})
}
