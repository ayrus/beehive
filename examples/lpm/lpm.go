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
	bh "github.com/ayrus/beehive"
	"github.com/ayrus/beehive/Godeps/_workspace/src/github.com/gorilla/mux"
	"github.com/ayrus/beehive/Godeps/_workspace/src/golang.org/x/net/context"
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

type CalcLPM net.IP

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
		lpmlog.Println("Unmarshal error: ", terr)
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
		lpmlog.Printf("Inserted %s\n", GetKey(data.Val))
		return ctx.Dict(dict).Put(GetKey(data.Val), data.Val)
	case get:
		res, err := ctx.Dict(dict).Get(string(data))
		lpmlog.Printf("Looking up %s\n", data)
		if err == nil && len(res) > 0 {
			ctx.ReplyTo(msg, result{Key: string(data), Val: Unmarshal(res)})
		} else {
			ctx.ReplyTo(msg, nil)
		}

		return nil

	case del:
		lpmlog.Printf("Delete %s\n", data)
		return ctx.Dict(dict).Del(string(data))
	case warmup:
		lpmlog.Printf("Created bee #%d", data.bnum)
		return nil
	case CalcLPM:
		lpmlog.Println("Received CalcLPM request")
		k := net.IP(data).String()
        

		ctx, cnl := context.WithTimeout(context.Background(), 30*time.Second)
		var res interface{}
		var err error
        
        //from
        ipLen := net.IPv4len
        l := 32
        ipL := net.IP(data)
        if p4 := ipL.To4(); len(p4) == net.IPv4len {
            ipLen = net.IPv4len
            l = 32
        } else {
            ipLen = net.IPv6len
            l = 128
        }
        //to

		chnl := make(chan interface{})
		//for i := net.IPv4len * 8; i >= 0; i-- {
        for i := ipLen * 8; i >= 0; i-- {
			//mask := net.CIDRMask(i, net.IPv4len*8)
            mask := net.CIDRMask(i, ipLen*8)
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

		//for i := 0; i < 32; i++ {
        for i := 0; i < l; i++ {
			x := <-chnl
			if x != nil {
				ret := x.(result)
				rt := ret.Val
				if rt.Priority > best_pri || (rt.Priority == best_pri && rt.Len > best_len) {
					res = rt
					best_pri = rt.Priority
					best_len = rt.Len
				}
				lpmlog.Printf("Candidate: %s\n", ret)
			}
		}

		cnl()

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
	case CalcLPM:
		k = strconv.FormatInt(int64(rand.Intn(int(s.buckets))), 16)
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
	lpmlog.Println("Received HTTP")
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
        //change from here
        //length is variable
        ipLen := net.IPv4len
        l := 32
        ipL := net.ParseIP(string(k))
        if p4 := ipL.To4(); len(p4) == net.IPv4len {
            ipLen = net.IPv4len
            l = 32
        } else {
            ipLen = net.IPv6len
            l = 128
        }
        //to here
		//for i := net.IPv4len * 8; i >= 0; i-- {
        for i := ipLen * 8; i >= 0; i-- {
			//mask := net.CIDRMask(i, net.IPv4len*8)
            mask := net.CIDRMask(i, ipLen*8)
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
        
		//for i := 0; i < 32; i++ {
        for i := 0; i < l; i++ {
			x := <-chnl
			if x != nil {
				ret := x.(result)
				rt := ret.Val
				if rt.Priority > best_pri || (rt.Priority == best_pri && rt.Len > best_len) {
					res = rt
					best_pri = rt.Priority
					best_len = rt.Len
				}
				lpmlog.Printf("Candidate: %s\n", ret)
			}
		}

		lpmlog.Println("HTTP Served LPM")
	case "PUT":
		var v []byte
		v, err = ioutil.ReadAll(r.Body)
		lpmlog.Println("HTTP Received Put")
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

type LPMOptions struct {
	replFactor int //= flag.Int("lpm.rf", 3, "replication factor")
	buckets    int //= flag.Int("lpm.b", 50, "number of buckets")
	//cpuprofile  //= flag.String("lpm.cpuprofile", "", "write cpu profile to file")
	raftlog bool //= flag.Bool("lpm.raftlog", false, "whether to print raft log")
	lg      bool //            = flag.Bool("lpm.log", false, "whether to print lpm log")
	random  bool //= flag.Bool("lpm.rand", false, "whether to use random placement")
	warmup  bool //= flag.Bool("lpm.warmup", false, "whether to warm up beehive before processing requests")
}

func NewLPMOptions() *LPMOptions {
	return &LPMOptions{replFactor: 3, buckets: 5, raftlog: false, lg: false, random: false, warmup: true}
}

func Install(hive bh.Hive, options LPMOptions) *bh.Sync {
	//func main() {
	//	flag.Parse()
	//options := NewLPMOptions()
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
	s.Handle(put{}, kv)
	s.Handle(get(""), kv)
	s.Handle(del(""), kv)
	a.HandleHTTP("/{key}", kv)

	go func() {
		ctx, cnl := context.WithTimeout(context.Background(), 30*time.Second)

		if options.warmup && options.buckets > 0 {
			for i := 0; i < options.buckets; i++ {
				//go func(i int) {
				//					fmt.Printf("%d\n", i)
				s.Process(ctx, warmup{i})
				//}(i)
			}
			//go func() {
			//s.Process(ctx, CalcLPM{})

			//}()
		}
		//fmt.Println("Done warming up")

		cnl()
	}()

	//hive.Start()
	//	fmt.Println("Done wagfrming up")

	return s

}

func init() {
	gob.Register(result{})
}
