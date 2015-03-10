package main

import (
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net"
	"os"
	"runtime/pprof"
	"strconv"
	"time"
	"fmt"

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
	Val Route `json:"route"`
}

type del string

type lpm struct {
	*bh.Sync
	buckets uint64
}

type Route struct {
	Dest net.IP `json:"dest"`
	Mask net.IP `json:"mask"`
	Gateway net.IP `json:"gateway"`
	Iface string `json:"iface"`
	//Priority string `json:"priority"`
}

func (s *lpm) Rcv(msg bh.Msg, ctx bh.RcvContext) error {
	switch data := msg.Data().(type) {
	case put:
		return ctx.Dict(dict).Put(data.Key, data.Val)
	case get:

		var rt Route
		var terr error


		reqIP := net.ParseIP(string(data))

		var bestRt Route

		// dummy
		bestRt.Mask = net.ParseIP("0.0.0.0")
		bestMask := net.IPMask(bestRt.Mask.To4())

		ctx.Dict(dict).ForEach(func(ik string, iv []byte){
			
			terr = json.Unmarshal(iv, &rt)
			if terr != nil {
				fmt.Println("Unmarshal error: ", terr)
			}

			// fmt.Printf("%s | %s - %s - %s - %s\n", 
			// 	ik, rt.Dest, rt.Mask, rt.Gateway, rt.Iface)

			
			msk := net.IPMask(rt.Mask.To4())
			reqIPmsk := reqIP.Mask(msk)

			if reqIPmsk.Equal(rt.Dest){
				s1, _ := msk.Size()
				s2, _ := bestMask.Size();
				if (s1 > s2) {
					bestMask = msk
					bestRt = rt 
				}
			}
			
			})
		// fmt.Printf("LPM | %s - %s - %s - %s\n", 
		// 		bestRt.Dest, bestRt.Mask, bestRt.Gateway, bestRt.Iface)

		ctx.ReplyTo(msg, result{Key: string(data), Val: bestRt})
		return nil
	case del:
		return ctx.Dict(dict).Del(string(data))
	}
	return errInvalid
}

func (s *lpm) Map(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	var k string
	var rt Route
	var terr error

	switch data := msg.Data().(type) {
	case put:
		//k = string(data.Key)
		//fmt.Println(data.Key)
		//fmt.Println(data.Val)

		terr = json.Unmarshal(data.Val, &rt)
		if terr != nil {
			fmt.Println("Unmarshal error: ", terr)
		}

		//fmt.Printf("%s | %s - %s - %s - %s\n", 
		//	data.Key, rt.Dest, rt.Mask, rt.Gateway, rt.Iface)

		msk := net.IPMask(rt.Mask.To4())
		k = rt.Dest.Mask(msk).String()

		//rt.Dest.String().split(".")
		// arr := rt.Dest.To4()
		// fmt.Printf("%d %d %d %d\n", arr[0], arr[1], arr[2], arr[3]);
		// x := (uint64(arr[0]) << 24) + (uint64(arr[1]) << 16) + (uint64(arr[2]) << 8) + uint64(arr[3]);

		// fmt.Printf("%s\n", strconv.FormatUint(x, 2)) 
		// fmt.Printf("%s | %s - %s - %s - %s\n", 
		// 	k, rt.Dest, rt.Mask, rt.Gateway, rt.Iface)




	case get:
		k = string(data)
		//for i := net.IPv4len * 8; i >= 0; i-- {
			//mask := net.CIDRMask(i, net.IPv4len * 8);
			//ip := net.ParseIP(string(data))

			//k = ip.Mask(mask).String()
			//k = "nothing"
		//}


	case del:
		k = string(data)
	}




	cells := bh.MappedCells{
		{
			Dict: dict,
			Key:  strconv.FormatUint(xxhash.Checksum64([]byte(k))%s.buckets, 16),
		},
	}
	return cells
	// 1.2.3.4/8 -> 1
	// 1,2,3,4/16 -> 1.2

	// 1.2.3.4 & mask
	// (1.2.3.4 >> 31) << 31

	// 1,0,0,0/4 -> 1.0.0.0
	// 1.0.0.0/32 -> 1.0.0.0

	// 2.0.0.0/4
	// 2.0.0.0/32
	//

	// Query: 1.0.0.0
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

		for i := net.IPv4len * 8; i >= 0; i-- {
			mask := net.CIDRMask(i, net.IPv4len * 8);
			ip := net.ParseIP(string(k))

			k = ip.Mask(mask).String()
			res, err = s.Process(ctx, get(k+":"+strconv.(i)))			
		}


		fmt.Println("Served LPM")
	case "PUT":
		var v []byte
		v, err = ioutil.ReadAll(r.Body)
		fmt.Println("PUT")
		res, err = s.Process(ctx, put{Key: k, Val: v})
	case "DELETE":
		res, err = s.Process(ctx, del(k))
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
	replFactor = flag.Int("kv.rf", 3, "replication factor")
	buckets    = flag.Int("kv.b", 50, "number of buckets")
	cpuprofile = flag.String("kv.cpuprofile", "", "write cpu profile to file")
	quiet      = flag.Bool("kv.quiet", true, "no raft log")
	random     = flag.Bool("kv.rand", false, "whether to use random placement")
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
	s.Handle(put{}, kv)
	s.Handle(get(""), kv)
	s.Handle(del(""), kv)
	a.HandleHTTP("/{key}", kv)

	bh.Start()
}

func init() {
	gob.Register(result{})
}
