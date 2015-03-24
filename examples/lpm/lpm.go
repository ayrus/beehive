package main

import (
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"runtime/pprof"
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

var (
	errKeyNotFound = errors.New("lpm: key not found")
	errInternal    = errors.New("lpm: internal error")
	errInvalid     = errors.New("lpm: invalid parameter")
)

type put struct {
	Key string
	Val []byte
}

type get string
type result struct {
	Key string `json:"request"`
	Val Route  `json:"route"`
}

type del string

type lpm struct {
	*bh.Sync
	buckets uint64
}

type warmup struct {
	bnum int
}

type Route struct {
	Dest     net.IP `json:"dest"`
	Len      int    `json:"len"`
	Name     string `json:"name"`
	Priority int    `json:"priority"`
}

func Unmarshal(data []byte) Route {
	var rt Route
	var terr error

	terr = json.Unmarshal(data, &rt)
	if terr != nil {
		fmt.Println("Unmarshal error: ", terr)
	}

	return rt
}

func GetKey(data []byte) string {
	rt := Unmarshal(data)
	msk := net.CIDRMask(rt.Len, 32)
	k := rt.Dest.Mask(msk).String() + "/" + strconv.FormatInt(int64(rt.Len), 10)
	return k
}

func (s *lpm) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	switch data := msg.Data().(type) {
	case put:
		fmt.Printf("Inserted %s\n", GetKey(data.Val))
		return ctx.Dict(dict).Put(GetKey(data.Val), data.Val)
	case get:
		res, err := ctx.Dict(dict).Get(string(data))
		fmt.Printf("Looking up %s\n", data)
		if err == nil && len(res) > 0 {
			fmt.Println("Found")
			ctx.ReplyTo(msg, result{Key: string(data), Val: Unmarshal(res)})
		} else {
			ctx.ReplyTo(msg, nil)
		}

		return nil

	case del:
        fmt.Printf("Delete %s\n", data);
        return ctx.Dict(dict).Del(string(data))
        // deleteKey := string(data) 
        // return ctx.Dict(dict).Del(deleteKey)
	case warmup:
		fmt.Printf("Created bee\n")
		return nil
	}
	return errInvalid
}

func (s *lpm) Map(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	var k string

	switch data := msg.Data().(type) {
	case put:
		k = GetKey(data.Val)
		k = strconv.FormatUint(xxhash.Checksum64([]byte(k))%s.buckets, 16)
	case get:
		k = string(data)
		k = strconv.FormatUint(xxhash.Checksum64([]byte(k))%s.buckets, 16)
	case del:
		k = string(data)
		k = strconv.FormatUint(xxhash.Checksum64([]byte(k))%s.buckets, 16)
	case warmup:
		k = strconv.FormatUint(uint64(data.bnum), 16)
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
	k, ok := mux.Vars(r)["key"]
	if !ok {
		http.Error(w, "no key in the url", http.StatusBadRequest)
		return
	}

	ctx, cnl := context.WithTimeout(context.Background(), 30*time.Second)
	var res interface{}
	var err error

	switch r.Method {
	case "GET":
		chnl := make(chan interface{})
		for i := net.IPv4len * 8; i >= 0; i-- {
			mask := net.CIDRMask(i, net.IPv4len*8)
			ip := net.ParseIP(string(k))

			k = ip.Mask(mask).String()
			req := k + "/" + strconv.FormatInt(int64(i), 10)

			go func(req string) {
				res, err = s.Process(ctx, get(req))

				if err == nil {
					chnl <- res
				} else {
					chnl <- nil
				}
			}(req)
		}

		best_pri := -1
		best_len := -1

		for i := 0; i < 32; i++ {
			x := <-chnl
			if x != nil {
				ret := x.(result)
				rt := ret.Val
				if rt.Priority > best_pri || (rt.Priority == best_pri && rt.Len > best_len) {
					res = rt
					best_pri = rt.Priority
					best_len = rt.Len
				}
				fmt.Println(x)
			}
		}

		fmt.Println("Served LPM")
	case "PUT":
		var v []byte
		v, err = ioutil.ReadAll(r.Body)
		fmt.Println("PUT")
		res, err = s.Process(ctx, put{Key: k, Val: v})
	case "DELETE":
		var v []byte
		v, err = ioutil.ReadAll(r.Body)

        res, err = s.Process(ctx, del(GetKey(v)))
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

var (
	replFactor    = flag.Int("kv.rf", 3, "replication factor")
	buckets       = flag.Int("kv.b", 50, "number of buckets")
	cpuprofile    = flag.String("kv.cpuprofile", "", "write cpu profile to file")
	quiet         = flag.Bool("kv.quiet", true, "no raft log")
	random        = flag.Bool("kv.rand", false, "whether to use random placement")
	should_warmup = flag.Bool("lpm.warmup", false, "whether to warm up beehive before processing requests")
)

func main() {
	flag.Parse()
	// mode := flag.String("profile.mode", "", "enable profiling mode, one of [cpu, mem, block]")
	// switch *mode {
	// case "cpu":
	// 	defer profile.Start(profile.CPUProfile).Stop()
	// case "mem":
	// 	defer profile.Start(profile.MemProfile).Stop()
	// case "block":
	// 	defer profile.Start(profile.BlockProfile).Stop()
	// default:
	// 	// do nothing
	// }
	// defer profile.Start(profile.MemProfile).Stop()
	rand.Seed(time.Now().UnixNano())
	if *quiet {
		log.SetOutput(ioutil.Discard)
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			glog.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	opts := []bh.AppOption{bh.Persistent(*replFactor)}
	if *random {
		rp := bh.RandomPlacement{
			Rand: rand.New(rand.NewSource(time.Now().UnixNano())),
		}
		opts = append(opts, bh.AppWithPlacement(rp))
	}
	a := bh.NewApp("lpm", opts...)
	s := bh.NewSync(a)
	kv := &lpm{
		Sync:    s,
		buckets: uint64(*buckets),
	}

	s.Handle(warmup{}, kv)
	s.Handle(put{}, kv)
	s.Handle(get(""), kv)
	s.Handle(del(""), kv)
	a.HandleHTTP("/{key}", kv)

	fmt.Println("Hi")
	go func() {
		ctx, cnl := context.WithTimeout(context.Background(), 30*time.Second)
		if *should_warmup && *buckets > 0 {
			fmt.Println("Entered")
			for i := 0; i < *buckets; i++ {
				s.Process(ctx, warmup{i})
			}
		}

		cnl()
	}()

	bh.Start()

}

func init() {
	gob.Register(result{})
}
