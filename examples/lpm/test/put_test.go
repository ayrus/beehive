package lpm

import (
	"testing"
    "net"
    "os"
    "log"
    bh "github.com/kandoo/beehive"
    lpm "github.com/ayrus/beehive/examples/lpm"
    "github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"

)

var hive bh.Hive
var lpmlog *log.Logger
var kv *lpm.Lpm

func compareRoute(r1 lpm.Route, r2 lpm.Route) bool {
    return (r1.Dest.Equal(r2.Dest) && r1.Len == r2.Len && r1.Name == r2.Name && r1.Priority == r2.Priority)
}

func TestSimplePut(t *testing.T){
    go hive.Start()

    r := lpm.Route{
        net.ParseIP("1.1.1.1"),
         3,
         "hello",
         2,
    }

    kv.Process(context.Background(), lpm.Put(r))

    res, err := kv.Process(context.Background(), lpm.CalcLPM(net.ParseIP("1.1.1.1")))
    if (err == nil){
        if !(compareRoute(r, res.(lpm.Route))){
            t.Fail()
        }
    } else {
        t.Fail()
    }

    hive.Stop()
}

// Setup code
func TestMain(m *testing.M) {
    hive = bh.NewHive()
    options := lpm.NewLPMOptions()
    kv = lpm.Install(hive, *options)
    lpmlog = log.New(os.Stderr, "Testing LPM: ", 0)


    os.Exit(m.Run())
}
