package lpm

import (
	lpm "github.com/ayrus/beehive/examples/lpm"
	bh "github.com/kandoo/beehive"
	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
	"log"
	"net"
	"os"
	"testing"
)

var hive bh.Hive
var testLog *log.Logger
var kv *lpm.Lpm

func compareRoute(r1 lpm.Route, r2 lpm.Route) bool {
	return (r1.Dest.Equal(r2.Dest) && r1.Len == r2.Len && r1.Name == r2.Name && r1.Priority == r2.Priority)
}

func setupTest() {
	hive = bh.NewHive()
	options := &lpm.LPMOptions{
		ReplFactor: 3,
		Buckets:    5,
		Raftlog:    false,
		Lg:         false,
		Random:     false,
		Warmup:     true,
	}
	kv = lpm.Install(hive, *options)
	testLog = log.New(os.Stderr, "Testing LPM: ", 0)
}

func teardownTest() {
	os.RemoveAll(hive.Config().StatePath)
}

/*
Test a simple LPM where there is only one entry in the database and it is a hit
*/
func TestSimpleLPM(t *testing.T) {
	setupTest()
	testLog.Println("TestSimpleLPM")

	go hive.Start()

	r := lpm.Route{
		net.ParseIP("1.1.1.1"),
		16,
		"hello",
		2,
	}

	_, err := kv.Process(context.Background(), lpm.Put(r))
	if err != nil {
		t.Error("Error inserting route: ", r)
	}

	res, err := kv.Process(context.Background(), lpm.CalcLPM(net.ParseIP("1.1.1.1")))
	if err == nil {
		if res == nil || !(compareRoute(r, res.(lpm.Route))) {
			t.Error("Result does not match expected")
		}
	} else {
		t.Error("Error calculating LPM: ", err, r)
	}

	hive.Stop()
	teardownTest()
}

/*
Test a simple LPM where there is a miss
*/
func TestMiss1(t *testing.T) {
	setupTest()
	testLog.Println("TestMiss1")

	go hive.Start()

	r := lpm.Route{
		net.ParseIP("255.255.255.255"),
		16,
		"hello",
		2,
	}

	_, err := kv.Process(context.Background(), lpm.Put(r))
	if err != nil {
		t.Error("Error inserting route: ", err, r)
	}

	res, err := kv.Process(context.Background(), lpm.CalcLPM(net.ParseIP("4.4.4.4")))
	if err == nil {
		if err == nil {
			if res != nil {
				t.Error("Found a result when there should not have been one: ", res)
			}
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	hive.Stop()
	teardownTest()
}

/*
Test where the prefix does not match
*/
func TestMiss2(t *testing.T) {
	setupTest()
	testLog.Println("TestMiss2")

	go hive.Start()

	r := lpm.Route{
		net.ParseIP("0.0.255.255"),
		24,
		"hello",
		2,
	}

	_, err := kv.Process(context.Background(), lpm.Put(r))
	if err != nil {
		t.Error("Error inserting route: ", err, r)
	}

	res, err := kv.Process(context.Background(), lpm.CalcLPM(net.ParseIP("255.0.0.0")))
	if err == nil {
		if res != nil {
			t.Error("Found a result when there should not have been one: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	hive.Stop()
	teardownTest()
}

/*
Test the db is empty
*/
func TestMissEmpty(t *testing.T) {
	setupTest()
	testLog.Println("TestMissEmpty")

	go hive.Start()

	res, err := kv.Process(context.Background(), lpm.CalcLPM(net.ParseIP("255.0.0.0")))

	if err == nil {
		if res != nil {
			t.Error("Found a result when there should not have been one: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	hive.Stop()
	teardownTest()
}

/*
Test that LPM returns the prefix with the highest priority
*/
func TestHighPriority1(t *testing.T) {
	setupTest()
	testLog.Println("TestHighPriority1")

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

	_, err := kv.Process(context.Background(), lpm.Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r3))
	if err != nil {
		t.Error("Error inserting route: ", err, r3)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r4))
	if err != nil {
		t.Error("Error inserting route: ", err, r4)
	}

	res, err := kv.Process(context.Background(), lpm.CalcLPM(net.ParseIP("123.123.123.123")))
	if err == nil {
		if res == nil || !(compareRoute(r4, res.(lpm.Route))) {
			t.Error("Returned the wrong result: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	hive.Stop()
	teardownTest()
}

/*
Tests that when all the matching entries have same priority, return the one with the longest length
*/
func TestHighPriority2(t *testing.T) {
	setupTest()
	testLog.Println("TestHighPriority2")

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
		1,
	}

	r3 := lpm.Route{
		net.ParseIP("123.123.123.123"),
		16,
		"test",
		1,
	}

	r4 := lpm.Route{
		net.ParseIP("123.123.123.123"),
		8,
		"test",
		1,
	}

	_, err := kv.Process(context.Background(), lpm.Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r3))
	if err != nil {
		t.Error("Error inserting route: ", err, r3)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r4))
	if err != nil {
		t.Error("Error inserting route: ", err, r4)
	}

	res, err := kv.Process(context.Background(), lpm.CalcLPM(net.ParseIP("123.123.123.123")))
	if err == nil {
		if res == nil || !(compareRoute(r1, res.(lpm.Route))) {
			t.Error("Returned the wrong result: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	hive.Stop()
	teardownTest()
}

/*
Tests that it will return the one with the highest priority when it is the longest
*/
func TestHighPriority3(t *testing.T) {
	setupTest()
	testLog.Println("TestHighPriority3")

	go hive.Start()

	r1 := lpm.Route{
		net.ParseIP("123.123.123.123"),
		32,
		"test",
		5,
	}

	r2 := lpm.Route{
		net.ParseIP("123.123.123.123"),
		24,
		"test",
		3,
	}

	r3 := lpm.Route{
		net.ParseIP("123.123.123.123"),
		16,
		"test",
		2,
	}

	r4 := lpm.Route{
		net.ParseIP("123.123.123.123"),
		8,
		"test",
		1,
	}

	_, err := kv.Process(context.Background(), lpm.Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r3))
	if err != nil {
		t.Error("Error inserting route: ", err, r3)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r4))
	if err != nil {
		t.Error("Error inserting route: ", err, r4)
	}

	res, err := kv.Process(context.Background(), lpm.CalcLPM(net.ParseIP("123.123.123.123")))
	if err == nil {
		if res == nil || !(compareRoute(r1, res.(lpm.Route))) {
			t.Error("Returned the wrong result: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	hive.Stop()
	teardownTest()
}

/*
Test where the prefix that matches is the shortest one
*/
func TestPrefixMatchShort(t *testing.T) {
	setupTest()
	testLog.Println("TestPrefixMatchShort")

	go hive.Start()

	r1 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		32,
		"test",
		5,
	}

	r2 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		24,
		"test",
		3,
	}

	r3 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		16,
		"test",
		2,
	}

	r4 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		8,
		"test",
		1,
	}

	_, err := kv.Process(context.Background(), lpm.Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r3))
	if err != nil {
		t.Error("Error inserting route: ", err, r3)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r4))
	if err != nil {
		t.Error("Error inserting route: ", err, r4)
	}

	res, err := kv.Process(context.Background(), lpm.CalcLPM(net.ParseIP("255.0.0.0")))
	if err == nil {
		if res == nil || !(compareRoute(r4, res.(lpm.Route))) {
			t.Error("Returned the wrong result: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	hive.Stop()
	teardownTest()
}

/*
Test where the prefix that matches is the longest one
*/
func TestPrefixMatchLong(t *testing.T) {
	setupTest()
	testLog.Println("TestPrefixMatchLong")

	go hive.Start()

	r1 := lpm.Route{
		net.ParseIP("255.255.240.0"),
		20,
		"test",
		5,
	}

	r2 := lpm.Route{
		net.ParseIP("255.255.0.0"),
		16,
		"test",
		3,
	}

	r3 := lpm.Route{
		net.ParseIP("255.0.0.0"),
		8,
		"test",
		2,
	}

	r4 := lpm.Route{
		net.ParseIP("240.0.0.0"),
		4,
		"test",
		1,
	}

	_, err := kv.Process(context.Background(), lpm.Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r3))
	if err != nil {
		t.Error("Error inserting route: ", err, r3)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r4))
	if err != nil {
		t.Error("Error inserting route: ", err, r4)
	}

	res, err := kv.Process(context.Background(), lpm.CalcLPM(net.ParseIP("255.255.255.255")))
	if err == nil {
		if res == nil || !(compareRoute(r1, res.(lpm.Route))) {
			t.Error("Returned the wrong result: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	hive.Stop()
	teardownTest()
}

/*
Test a delete, result should be null
*/
func TestExactDelete1(t *testing.T) {
	setupTest()
	testLog.Println("TestExactDelete1")

	go hive.Start()

	r := lpm.Route{
		net.ParseIP("255.255.255.255"),
		16,
		"hello",
		2,
	}

	d := lpm.Del{
		net.ParseIP("255.255.255.255"),
		16,
		true,
	}

	_, err := kv.Process(context.Background(), lpm.Put(r))
	if err != nil {
		t.Error("Error inserting route: ", err, r)
	}

	_, err = kv.Process(context.Background(), d)
	if err != nil {
		t.Error("Error deleting route: ", err, d)
	}

	res, err := kv.Process(context.Background(), lpm.CalcLPM(net.ParseIP("255.255.255.255")))
	if err == nil {
		if res != nil {
			t.Error("Found a result when there should not have been one: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	hive.Stop()
	teardownTest()
}

/*
Test a delete, the result should be the shorter one with lower priority
*/
func TestExactDeleteMiss(t *testing.T) {
	setupTest()
	testLog.Println("TestExactDeleteMiss")

	go hive.Start()

	r1 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		16,
		"hello",
		10,
	}

	r2 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		8,
		"hello",
		1,
	}

	d := lpm.Del{
		net.ParseIP("255.255.255.0"),
		32,
		true,
	}

	_, err := kv.Process(context.Background(), lpm.Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), d)
	if err != nil {
		t.Error("Error deleting route: ", err, d)
	}

	res, err := kv.Process(context.Background(), lpm.CalcLPM(net.ParseIP("255.255.255.255")))
	if err == nil {
		if res == nil || !(compareRoute(r1, res.(lpm.Route))) {
			t.Error("Returned the wrong result: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	hive.Stop()
	teardownTest()
}

/*
Test a non exact delete, should return nothing
*/
func TestNonExactDelete1(t *testing.T) {
	setupTest()
	testLog.Println("TestNonExactDelete1")

	go hive.Start()

	r1 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		16,
		"hello",
		10,
	}

	r2 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		8,
		"hello",
		1,
	}

	r3 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		32,
		"hello",
		1,
	}

	r4 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		20,
		"hello",
		1,
	}

	r5 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		10,
		"hello",
		1,
	}

	d := lpm.Del{
		net.ParseIP("255.255.255.255"),
		8,
		false,
	}

	_, err := kv.Process(context.Background(), lpm.Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r3))
	if err != nil {
		t.Error("Error inserting route: ", err, r3)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r4))
	if err != nil {
		t.Error("Error inserting route: ", err, r4)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r5))
	if err != nil {
		t.Error("Error inserting route: ", err, r5)
	}

	_, err = kv.Process(context.Background(), d)
	if err != nil {
		t.Error("Error deleting route: ", err, d)
	}

	res, err := kv.Process(context.Background(), lpm.CalcLPM(net.ParseIP("255.255.255.255")))
	if err == nil {
		if res != nil {
			t.Error("Found a result when there should not have been one: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	hive.Stop()
	teardownTest()
}

/*
Test a non exact delete with varying prefixes
*/
func TestNonExactDelete2(t *testing.T) {
	setupTest()
	testLog.Println("TestNonExactDelete2")

	go hive.Start()

	r1 := lpm.Route{
		net.ParseIP("255.255.0.0"),
		16,
		"hello",
		10,
	}

	r2 := lpm.Route{
		net.ParseIP("255.255.255.123"),
		8,
		"hello",
		1,
	}

	r3 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		32,
		"hello",
		1,
	}

	r4 := lpm.Route{
		net.ParseIP("255.255.255.240"),
		20,
		"hello",
		1,
	}

	r5 := lpm.Route{
		net.ParseIP("255.255.20.0"),
		10,
		"hello",
		1,
	}

	d := lpm.Del{
		net.ParseIP("255.255.255.255"),
		8,
		false,
	}

	_, err := kv.Process(context.Background(), lpm.Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r3))
	if err != nil {
		t.Error("Error inserting route: ", err, r3)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r4))
	if err != nil {
		t.Error("Error inserting route: ", err, r4)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r5))
	if err != nil {
		t.Error("Error inserting route: ", err, r5)
	}

	_, err = kv.Process(context.Background(), d)
	if err != nil {
		t.Error("Error deleting route: ", err, d)
	}

	res, err := kv.Process(context.Background(), lpm.CalcLPM(net.ParseIP("255.255.255.255")))
	if err == nil {
		if res != nil {
			t.Error("Found a result when there should not have been one: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	hive.Stop()
	teardownTest()
}

/*
Test a non exact delete that leaves some prefixes behind
*/
func TestNonExactDelete3(t *testing.T) {
	setupTest()
	testLog.Println("TestNonExactDelete3")

	go hive.Start()

	r1 := lpm.Route{
		net.ParseIP("255.255.0.0"),
		16,
		"hello",
		10,
	}

	r2 := lpm.Route{
		net.ParseIP("255.255.255.123"),
		8,
		"hello",
		1,
	}

	r3 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		32,
		"hello",
		1,
	}

	r4 := lpm.Route{
		net.ParseIP("255.255.255.240"),
		20,
		"hello",
		1,
	}

	r5 := lpm.Route{
		net.ParseIP("255.255.20.0"),
		10,
		"hello",
		1,
	}

	r6 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		4,
		"hello",
		5,
	}

	r7 := lpm.Route{
		net.ParseIP("255.255.20.0"),
		3,
		"hello",
		2,
	}

	r8 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		6,
		"hello",
		1,
	}

	d := lpm.Del{
		net.ParseIP("255.255.255.255"),
		8,
		false,
	}

	_, err := kv.Process(context.Background(), lpm.Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r3))
	if err != nil {
		t.Error("Error inserting route: ", err, r3)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r4))
	if err != nil {
		t.Error("Error inserting route: ", err, r4)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r5))
	if err != nil {
		t.Error("Error inserting route: ", err, r5)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r6))
	if err != nil {
		t.Error("Error inserting route: ", err, r6)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r7))
	if err != nil {
		t.Error("Error inserting route: ", err, r7)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r8))
	if err != nil {
		t.Error("Error inserting route: ", err, r8)
	}

	_, err = kv.Process(context.Background(), d)
	if err != nil {
		t.Error("Error deleting route: ", err, d)
	}

	res, err := kv.Process(context.Background(), lpm.CalcLPM(net.ParseIP("255.255.255.255")))
	if err == nil {
		if res == nil || !(compareRoute(r6, res.(lpm.Route))) {
			t.Error("Returned the wrong result: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	hive.Stop()
	teardownTest()
}

/*
Test a non exact delete, should return nothing
*/
func TestNonExactDeleteMiss(t *testing.T) {
	setupTest()
	testLog.Println("TestNonExactDeleteMiss")

	go hive.Start()

	r1 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		16,
		"hello",
		10,
	}

	r2 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		8,
		"hello",
		1,
	}

	r3 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		32,
		"hello",
		3,
	}

	r4 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		20,
		"hello",
		6,
	}

	r5 := lpm.Route{
		net.ParseIP("255.255.255.255"),
		10,
		"hello",
		2,
	}

	d := lpm.Del{
		net.ParseIP("123.123.123.123"),
		8,
		false,
	}

	_, err := kv.Process(context.Background(), lpm.Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r3))
	if err != nil {
		t.Error("Error inserting route: ", err, r3)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r4))
	if err != nil {
		t.Error("Error inserting route: ", err, r4)
	}

	_, err = kv.Process(context.Background(), lpm.Put(r5))
	if err != nil {
		t.Error("Error inserting route: ", err, r5)
	}

	_, err = kv.Process(context.Background(), d)
	if err != nil {
		t.Error("Error deleting route: ", err, d)
	}

	res, err := kv.Process(context.Background(), lpm.CalcLPM(net.ParseIP("255.255.255.255")))
	if err == nil {
		if res == nil || !(compareRoute(r1, res.(lpm.Route))) {
			t.Error("Returned the wrong result: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	hive.Stop()
	teardownTest()
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
