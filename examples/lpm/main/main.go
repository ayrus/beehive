package main

import (
	lpm "github.com/ayrus/beehive/examples/lpm"
	bh "github.com/kandoo/beehive"
	//"net"
)

func main() {
	hive := bh.NewHive()
	options := lpm.NewLPMOptions()
	lpm.Install(hive, *options)

	//TODO: This code does not belong here and should be removed.
	// ip := net.ParseIP("1.1.1.1")
	// go func() {
	// 	hive.Emit(lpm.CalcLPM(ip))
	// 	rt := lpm.Route{ip, 16, "test", 1}
	// 	hive.Emit(lpm.Put(rt))
	// }()

	hive.Start()
}
