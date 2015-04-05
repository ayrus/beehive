package main

import (
	"fmt"
	bh "github.com/ayrus/beehive"
	"github.com/ayrus/beehive/Godeps/_workspace/src/golang.org/x/net/context"
	lpm "github.com/ayrus/beehive/examples/lpm"
	"net"
	"time"
)

func main() {
	hive := bh.NewHive()
	options := lpm.NewLPMOptions()
	//go func() {
	s := lpm.Install(hive, *options)
	//}()
	ip := net.ParseIP("1.1.1.1")
	go func() {
		for i := 0; i < 100; i++ {
			fmt.Printf("%d\n", i)
		}
		ctx, cnl := context.WithTimeout(context.Background(), 30*time.Second)

		s.Process(ctx, lpm.CalcLPM(ip))

		cnl()
	}()

	hive.Start()
	fmt.Printf("Here")
	//bh.Start()
}
