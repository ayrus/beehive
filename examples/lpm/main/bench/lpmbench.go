// Benchmarks for the key value store.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"
)

const (
	keyLen     = 16
	minBodyLen = 16
	maxBodyLen = 512
)

var client *http.Client

type target struct {
	method string
	url    string
	body   []byte
}

type Route struct {
	Dest     net.IP `json:"dest"`
	Len      int    `json:"len"`
	Name     string `json:"name"`
	Priority int    `json:"priority"`
}

// func generateRandomRoute() (string, Route) {
func generateRandomRoute() string {
	//rand.Seed(time.Now().Unix())
	block1 := rand.Intn(254) + 1 // just so the IP starts with atleast 1
	block2 := rand.Intn(255)
	block3 := rand.Intn(255)
	block4 := rand.Intn(255)
	key := strconv.Itoa(block1) + "." +
		strconv.Itoa(block2) + "." +
		strconv.Itoa(block3) + "." +
		strconv.Itoa(block4)
		// dest := net.ParseIP(key);
		// prefixLen := rand.Intn(32);
		// name := randString(32);
		// p := rand.Intn(10);
		// route := Route{dest, prefixLen, name, p}

	// return key, route
	return key
}

func newTarget(method, url string, body []byte) target {
	return target{
		method: method,
		url:    url,
		body:   body,
	}
}

func newRoute(d net.IP, l int, n string, p int) Route {

	return Route{d, l, n, p}
}

func (t *target) do() (d time.Duration, in int64, out int64, err error) {
	req, err := http.NewRequest(t.method, t.url, bytes.NewBuffer(t.body))
	if err != nil {
		return 0, 0, 0, err
	}
	in = req.ContentLength
	start := time.Now()
	res, err := client.Do(req)
	d = time.Since(start)
	if err != nil {
		return
	}
	defer func() {
		io.Copy(ioutil.Discard, res.Body)
		res.Body.Close()
	}()
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	out = int64(len(b))
	return
}

type result struct {
	Method string        `json:"method"`
	URL    string        `json:"url"`
	In     int64         `json:"in"`
	Out    int64         `json:"out"`
	Dur    time.Duration `json:"dur"`
	Err    string        `json:"err"`
}

func randRange(from, to int) int {
	return rand.Intn(to-from) + from
}

func randChar() byte {
	a := byte('a')
	z := byte('z')
	return byte(randRange(int(a), int(z)))
}

func randString(length int) string {
	b := make([]byte, length)
	for i := 0; i < length; i++ {
		b[i] = randChar()
	}
	return string(b)
}

func generateTargets(addr string, writes, localReads,
	randReads int) []target {

	rand.Seed(time.Now().UTC().UnixNano())
	var targets []target
	var keys []string
	for i := 0; i < writes/5; i++ {
		// k, v := generateRandomRoute()
		k := generateRandomRoute()
		keys = append(keys, k)

		for j := 0; j < 5; j++ {
			// put requests here, modify
			// dest := net.ParseIP(key);
			dest := net.ParseIP(k)
			prefixLen := rand.Intn(32)
			name := randString(32)
			p := rand.Intn(10)
			route := Route{dest, prefixLen, name, p}
			data, err := json.Marshal(route)
			//data, err := json.Marshal(v);
			if err == nil {
				t := newTarget("PUT", "http://"+addr+"/apps/lpm/"+k,
					[]byte(data))
				fmt.Println(route)
				targets = append(targets, t)
			} else {
				fmt.Println("ERROR")
			}
		}
	}
	for i := 0; i < localReads; i++ {

		// modify with getting IP
		t := newTarget("GET",
			"http://"+addr+"/apps/lpm/"+keys[rand.Intn(len(keys))], []byte{})
		targets = append(targets, t)
	}
	for i := 0; i < randReads; i++ {

		// modify with ip
		t := newTarget("GET",
			"http://"+addr+"/apps/lpm/"+randString(keyLen), []byte{})
		targets = append(targets, t)
	}
	return targets
}

func readTargets(addr string, filename string) []target {
	var targets []target
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanWords)

	for scanner.Scan() {
		typ := scanner.Text()
		if typ == "GET" {
			scanner.Scan()
			ip := scanner.Text()
			targets = append(targets, target{"GET", "http://" + addr + "/apps/lpm/" + ip, []byte{}})
		} else if typ == "PUT" {
			scanner.Scan()
			ip := net.ParseIP(scanner.Text())
			scanner.Scan()
			len, err := strconv.ParseInt(scanner.Text(), 10, 32)
			scanner.Scan()
			prio, err := strconv.ParseInt(scanner.Text(), 10, 32)
			scanner.Scan()
			name := scanner.Text()
			route := Route{ip, int(len), name, int(prio)}
			data, err := json.Marshal(route)
			if err != nil {
				panic(err)
			}
			targets = append(targets, target{"PUT", "http://" + addr + "/apps/lpm/" + string(ip), []byte(data)})
		} else {
			panic("Unknown instruction type")
		}
	}
	return targets
}

func run(id int, targets []target, rounds int) []result {
	results := make([]result, 0, len(targets)*rounds)
	for i := 0; i < rounds; i++ {
		for j, t := range targets {
			fmt.Printf("%v-%v/%v-%s ", id, i, j, t.method)
			var err error
			res := result{
				Method: t.method,
				URL:    t.url,
			}
			res.Dur, res.In, res.Out, err = t.do()
			if err != nil {
				res.Err = err.Error()
			}
			results = append(results, res)
		}
	}
	return results
}

func mustSave(results []result, w io.Writer) {
	b, err := json.Marshal(results)
	if err != nil {
		panic(err)
	}
	w.Write(b)
}

var (
	addr     = flag.String("addr", "localhost:7767", "server address")
	writes   = flag.Int("writes", 50, "number of random keys to writes per round")
	localr   = flag.Int("localreads", 250, "number of reads from written keys")
	randr    = flag.Int("randomreads", 0, "number of random keys to read")
	rounds   = flag.Int("rounds", 1, "number of rounds")
	workers  = flag.Int("workers", 1, "number of parallel clients")
	timeout  = flag.Duration("timeout", 60*time.Second, "request timeout")
	output   = flag.String("out", "bench.out", "benchmark output file")
	filename = flag.String("file", "", "where to read instructions from")
)

func main() {
	flag.Parse()

	f, err := os.Create(*output)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	client = &http.Client{Timeout: *timeout}

	ch := make(chan []result)
	for w := 0; w < *workers; w++ {
		if *filename == "" {
			go func(w int) {
				t := generateTargets(*addr, *writes, *localr, *randr)
				ch <- run(w, t, *rounds)
			}(w)
		} else {
			go func(w int) {
				t := readTargets(*addr, *filename)
				ch <- run(w, t, *rounds)
			}(w)
		}
	}

	var res []result
	for i := 0; i < *workers; i++ {
		res = append(res, <-ch...)
	}
	mustSave(res, f)
}
