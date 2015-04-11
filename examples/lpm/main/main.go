package main

import (
	lpm "github.com/kandoo/beehive/examples/lpm"
	bh "github.com/kandoo/beehive"
)

func main() {
	hive := bh.NewHive()
	options := lpm.NewLPMOptions()
	lpm.Install(hive, *options)

	hive.Start()
}
