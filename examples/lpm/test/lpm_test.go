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


func setupTest(){
    hive = bh.NewHive()
    options := lpm.NewLPMOptions()
    kv = lpm.Install(hive, *options)
    lpmlog = log.New(os.Stderr, "Testing LPM: ", 0)
}

func teardownTest(){
    os.RemoveAll(hive.Config().StatePath)
}

/*
Test a simple LPM where there is only one entry in the database and it is a hit
*/
func TestSimpleLPM1(t *testing.T){
    setupTest()

    go hive.Start()

    r := lpm.Route{
        net.ParseIP("1.1.1.1"),
         16,
         "hello",
         2,
    }

    kv.Process(context.Background(), lpm.Put(r))

    res, err := kv.Process(context.Background(), lpm.CalcLPM(net.ParseIP("1.1.1.1")))
    if (err == nil){
        if res == nil || !(compareRoute(r, res.(lpm.Route))){
            t.Fail()
        }
    } else {
        t.Fail()
    }

    hive.Stop()
    teardownTest()
}

/*
Test a simple LPM where there is a miss
*/
func TestMiss(t *testing.T){
    setupTest()
    go hive.Start()

    r := lpm.Route{
        net.ParseIP("255.255.255.255"),
         16,
         "hello",
         2,
    }

    kv.Process(context.Background(), lpm.Put(r))
    res, err := kv.Process(context.Background(), lpm.CalcLPM(net.ParseIP("4.4.4.4")))
    if (err == nil){
        if (err == nil){
            if res != nil{
                t.Fail()
            }
        }
    } else {
        t.Fail()
    }

    hive.Stop()
    teardownTest()
}

/*
Test that LPM returns the prefix with the highest priority
*/
func TestHighPriority1(t *testing.T){
    setupTest()
    go hive.Start()

    r1 := lpm.Route{
        net.ParseIP("123.123.123.123"),
        32,
        "test",
        1,
    }

    r2 := lpm.Route{
        net.ParseIP("123.123.123.123"),
        24,
        "test",
        2,
    }

    r3 := lpm.Route{
        net.ParseIP("123.123.123.123"),
        16,
        "test",
        3,
    }

    r4 := lpm.Route{
        net.ParseIP("123.123.123.123"),
        8,
        "test",
        10,
    }

    kv.Process(context.Background(), lpm.Put(r1))
    kv.Process(context.Background(), lpm.Put(r2))
    kv.Process(context.Background(), lpm.Put(r3))
    kv.Process(context.Background(), lpm.Put(r4))

    res, err := kv.Process(context.Background(), lpm.CalcLPM(net.ParseIP("123.123.123.123")))
    if (err == nil){
        if res == nil || !(compareRoute(r4, res.(lpm.Route))){
            t.Fail()
        }
    } else {
        t.Fail()
    }

    hive.Stop()
    teardownTest()
}

func TestMain(m *testing.M) {
    os.Exit(m.Run())
}
