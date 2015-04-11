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

type Del struct {
	Dest  net.IP `json:"dest"`
	Len   int    `json:len`
	Exact bool   `json:exact`
}

type delKey string

type Lpm struct {
	*bh.Sync
	Buckets uint64
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

func unmarshal(data []byte) (Route, error) {
	var rt Route
	var terr error

	terr = json.Unmarshal(data, &rt)
	if terr != nil {
		lpmlog.Println("Unmarshal error: ", terr)
		return rt, errors.New(errInvalid.Error())
	}

	return rt, nil
}

func unmarshalDel(data []byte) (Del, error) {
	var dl Del
	var terr error

	terr = json.Unmarshal(data, &dl)
	if terr != nil {
		lpmlog.Println("Unmarshal error: ", terr)
		return dl, errors.New(errInvalid.Error())
	}

	return dl, nil
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

func getDelKey(dl Del) string {
	msk := net.CIDRMask(dl.Len, iplen(dl.Dest)*8)
	k := dl.Dest.Mask(msk).String() + "/" + strconv.FormatInt(int64(dl.Len), 10)
	return k
}

func (s *Lpm) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	switch data := msg.Data().(type) {

	case Put:
		rt1 := Route(data)
		var rt2 Route
		err := ctx.Dict(dict).GetGob(getKey(rt1), &rt2)
		if err == nil {
			if rt1.Priority > rt2.Priority {
				lpmlog.Printf("Inserted %s\n", getKey(rt1))
				return ctx.Dict(dict).PutGob(getKey(rt1), &rt1)
			} else {
				return nil
			}
		} else {
			lpmlog.Printf("Inserted %s\n", getKey(rt1))
			return ctx.Dict(dict).PutGob(getKey(rt1), &rt1)
		}

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
		dl := Del(data)

		lpmlog.Println("Received Delete Request")

		var err error
		netctx, cnl := context.WithCancel(context.Background())

		if !dl.Exact {
			for i := dl.Len; i <= iplen(dl.Dest)*8; i++ {
				msk := net.CIDRMask(i, iplen(dl.Dest)*8)
				ck := dl.Dest.Mask(msk).String() + "/" + strconv.FormatInt(int64(i), 10)
				go func(req string) {
					_, err = s.Process(netctx, delKey(req))
					if err != nil {
						lpmlog.Println(err)
					}
				}(ck)
			}
		} else {
			go func(req string) {
				_, err = s.Process(netctx, delKey(req))
				if err != nil {
					lpmlog.Println(err)
				}
			}(getDelKey(dl))
		}
		cnl()
		return nil

	case delKey:
		dk := string(data)
		lpmlog.Println("Deleting!", dk)

		return ctx.Dict(dict).Del(dk)

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

func (s *Lpm) Map(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	var k string

	switch data := msg.Data().(type) {
	case Put:
		k = getKey(Route(data))
		k = strconv.FormatUint(xxhash.Checksum64([]byte(k))%s.Buckets, 16)
	case get:
		k = string(data)
		k = strconv.FormatUint(xxhash.Checksum64([]byte(k))%s.Buckets, 16)
	case Del:
		k = getDelKey(Del(data))
		k = strconv.FormatUint(xxhash.Checksum64([]byte(k))%s.Buckets, 16)
	case delKey:
		k = string(data)
		k = strconv.FormatUint(xxhash.Checksum64([]byte(k))%s.Buckets, 16)
	case warmup:
		k = strconv.FormatUint(uint64(data.bnum), 16)
	case CalcLPM:
		k = strconv.FormatInt(int64(rand.Intn(int(s.Buckets)))+int64(s.Buckets), 16)
	}

	cells := bh.MappedCells{
		{
			Dict: dict,
			Key:  k,
		},
	}
	return cells
}

func (s *Lpm) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
		var rt Route
		v, err = ioutil.ReadAll(r.Body)
		if err != nil {
			break
		}

		lpmlog.Println("HTTP Received Put")
		rt, err = unmarshal(v)
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

		dl, err = unmarshalDel(v)
		if err != nil {
			break
		}

		res, err = s.Process(ctx, Del(dl))
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
	ReplFactor int  //= flag.Int("lpm.rf", 3, "replication factor")
	Buckets    int  //= flag.Int("lpm.b", 50, "number of Buckets")
	Raftlog    bool //= flag.Bool("lpm.Raftlog", false, "whether to print raft log")
	Lg         bool //= flag.Bool("lpm.log", false, "whether to print lpm log")
	Random     bool //= flag.Bool("lpm.rand", false, "whether to use Random placement")
	Warmup     bool //= flag.Bool("lpm.warmup", false, "whether to warm up beehive before processing requests")
}

func NewLPMOptions() *LPMOptions {
	return &LPMOptions{ReplFactor: 3, Buckets: 5, Raftlog: false, Lg: true, Random: false, Warmup: true}
}

func Install(hive bh.Hive, options LPMOptions) *Lpm {
	rand.Seed(time.Now().UnixNano())

	if !options.Raftlog {
		log.SetOutput(ioutil.Discard)
	}


	if !options.Lg{
		lpmlog = log.New(ioutil.Discard, "LPM: ", 0)
	} else {
		lpmlog = log.New(os.Stderr, "LPM: ", 0)
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

	kv := &Lpm{
		Sync:    s,
		Buckets: uint64(options.Buckets),
	}

	s.Handle(warmup{}, kv)
	s.Handle(CalcLPM{}, kv)
	s.Handle(Put{}, kv)
	s.Handle(get(""), kv)
	s.Handle(Del{}, kv)
	s.Handle(delKey(""), kv)

	a.Handle(CalcLPM{}, kv)
	a.Handle(Put{}, kv)
	a.Handle(get(""), kv)
	a.Handle(Del{}, kv)
	a.Handle(delKey(""), kv)
	a.HandleHTTP("", kv)
	a.HandleHTTP("/{key}", kv)

	go func() {
		ctx, cnl := context.WithCancel(context.Background())

		if options.Warmup && options.Buckets > 0 {
			for i := 0; i < options.Buckets*2; i++ {
				s.Process(ctx, warmup{i})
			}

		}

		cnl()
	}()

	return kv
}

func init() {
	gob.Register(Route{})
}
