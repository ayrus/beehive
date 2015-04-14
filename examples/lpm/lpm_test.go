package lpm

import (
	"fmt"
	bh "github.com/kandoo/beehive"
	"github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"

	"encoding/json"
	"io/ioutil"
	"log"
	"net"
	"time"
)

/*
 The tests here perform very basic *end-to-end* sanity and correctness checks to ensure that the
 HTTP endpoint of the LPM module, responds appropriately.
*/

const (
	endpoint = "http://localhost:7767"
	appPath  = "/apps/lpm"
)

var insertURL *string
var hive bh.Hive
var testLog *log.Logger
var kv *bh.Sync

func init() {
	u, _ := url.ParseRequestURI(endpoint)
	u.Path = appPath + "/insert"
	insertURL = new(string)
	*insertURL = fmt.Sprintf("%v", u)
	testLog = log.New(os.Stderr, "Testing LPM: ", 0)
}

func setupTest() {
	hive = bh.NewHive()
	options := &LPMOptions{
		ReplFactor: 3,
		Buckets:    5,
		Raftlog:    false,
		Lg:         false,
		Random:     false,
		Warmup:     true,
	}
	kv = Install(hive, *options)

	go hive.Start()
	time.Sleep(50 * time.Millisecond)
}

func teardownTest() {
	hive.Stop()
	os.RemoveAll(hive.Config().StatePath)
}

func sendRequest(payload string, method string, target string, t *testing.T) *http.Response {
	reader := strings.NewReader(payload)
	req, err := http.NewRequest(method, target, reader)
	if err != nil {
		t.Error(err)
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Error(err)
	}

	return res
}

func compareRoute(r1 Route, r2 Route) bool {
	return (r1.Dest.Equal(r2.Dest) && r1.Len == r2.Len && r1.Name == r2.Name && r1.Priority == r2.Priority)
}

/*
 Test that tries inserting a route without any params.
*/
func TestLPMEmptyPut(t *testing.T) {
	setupTest()
	testLog.Println("TestEmptyPut HTTP")

	res := sendRequest("", "PUT", *insertURL, t)
	if res.StatusCode != 400 {
		t.Errorf("Expected 400 Bad Response, got : %d", res.StatusCode)
	}
}

/*
 Test inserts a route.
*/
func TestLPMGenericPut(t *testing.T) {
	testLog.Println("TestGenericPut HTTP")

	payload := "{\"dest\":\"12.0.1.0\",\"len\":24, \"name\":\"Route1\", \"priority\":1}"
	res := sendRequest(payload, "PUT", *insertURL, t)
	if res.StatusCode != 200 {
		t.Errorf("Expected 200 OK, got : %d", res.StatusCode)
	}
}

/*
 Test that tries to perform a delete with no request params.
*/
func TestLPMEmptyDelete(t *testing.T) {
	testLog.Println("TestEmptyDelete HTTP")

	res := sendRequest("", "DELETE", *insertURL, t)
	if res.StatusCode != 400 {
		t.Errorf("Expected 400 Bad Response, got : %d", res.StatusCode)
	}
}

/*
 Test that deletes a route.
*/
func TestLPMGenericDelete(t *testing.T) {
	testLog.Println("TestGenericDelete HTTP")

	payload := "{\"dest\":\"12.0.2.0\",\"len\":24, \"name\":\"Route1\", \"priority\":1}"
	res := sendRequest(payload, "PUT", *insertURL, t)
	if res.StatusCode != 200 {
		t.Errorf("Cannot create route")
	}

	payload = "{\"dest\":\"12.0.2.0\", \"len\":24}"
	u, _ := url.ParseRequestURI(endpoint)
	u.Path = appPath + "/12.0.2.0"

	res = sendRequest(payload, "DELETE", fmt.Sprintf("%v", u), t)
	if res.StatusCode != 200 {
		t.Errorf("Expected 200 OK, got : %d", res.StatusCode)
	}
}

/*
 Test that performs a generic lookup on a route after it has been inserted.
*/
func TestLPMGenericGet(t *testing.T) {
	testLog.Println("TestGenericGet HTTP")

	payload := "{\"dest\":\"12.0.3.0\",\"len\":24, \"name\":\"Route1\", \"priority\":1}"
	res := sendRequest(payload, "PUT", *insertURL, t)
	if res.StatusCode != 200 {
		t.Errorf("Cannot create route")
	}

	u, _ := url.ParseRequestURI(endpoint)
	u.Path = appPath + "/12.0.3.0"

	res = sendRequest("", "GET", fmt.Sprintf("%v", u), t)
	if res.StatusCode != 200 {
		t.Errorf("Expected 200 OK, got : %d", res.StatusCode)
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		t.Errorf("Cannot read response body")
	}

	var actual Route
	err = json.Unmarshal(body, &actual)
	if err != nil {
		t.Errorf("Cannot unmarshal response body")
	}

	var expected Route
	err = json.Unmarshal([]byte(payload), &expected)
	if err != nil {
		t.Errorf("Cannot unmarshal payload")
	}

	if !(compareRoute(actual, expected)) {
		t.Errorf("Expected doesn't match actual")
	}
}

/*
 Test that inserts an arbitrary route and tries to lookup another one .
*/
func TestLPMGetMiss(t *testing.T) {
	testLog.Println("TestGetMiss HTTP")

	payload := "{\"dest\":\"12.0.4.0\",\"len\":24, \"name\":\"Route1\", \"priority\":1}"
	res := sendRequest(payload, "PUT", *insertURL, t)
	if res.StatusCode != 200 {
		t.Errorf("Cannot create route")
	}

	u, _ := url.ParseRequestURI(endpoint)
	u.Path = appPath + "/16.0.3.0"

	res = sendRequest("", "GET", fmt.Sprintf("%v", u), t)
	if res.StatusCode != 200 {
		t.Errorf("Expected 200 OK, got : %d", res.StatusCode)
	}

	if res.ContentLength != 0 {
		t.Errorf("Expected a miss, got content")
	}
}

/*
 Test that LPM returns the prefix with the highest priority.
*/
func TestLPMGetHighPriority(t *testing.T) {
	testLog.Println("TestGetHighPriority HTTP")

	r1 := Route{
		net.ParseIP("123.123.123.123"),
		32,
		"test",
		1,
	}

	r2 := Route{
		net.ParseIP("123.123.123.123"),
		24,
		"test",
		2,
	}

	r3 := Route{
		net.ParseIP("123.123.123.123"),
		16,
		"test",
		3,
	}

	r4 := Route{
		net.ParseIP("123.123.123.123"),
		8,
		"test",
		10,
	}
	routes := [4]Route{r1, r2, r3, r4}

	for _, rt := range routes {
		payload, err := json.Marshal(rt)
		if err != nil {
			t.Errorf("Cannot marshal route")
		}

		res := sendRequest(string(payload), "PUT", *insertURL, t)
		if res.StatusCode != 200 {
			t.Errorf("Cannot create route")
		}
	}

	u, _ := url.ParseRequestURI(endpoint)
	u.Path = appPath + "/123.123.123.123"

	res := sendRequest("", "GET", fmt.Sprintf("%v", u), t)
	if res.StatusCode != 200 {
		t.Errorf("Expected 200 OK, got : %d", res.StatusCode)
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		t.Errorf("Cannot read response body")
	}

	var actual Route
	err = json.Unmarshal(body, &actual)
	if err != nil {
		t.Errorf("Cannot unmarshal response body")
	}

	if !(compareRoute(actual, r4)) {
		t.Errorf("Expected doesn't match actual")
	}
}

/*
 Test where the prefix that matches is the longest one.
*/
func TestLPMGetPrefixMatchLong(t *testing.T) {
	testLog.Println("TestGetPrefixMatchLong HTTP")

	r1 := Route{
		net.ParseIP("255.255.240.0"),
		20,
		"test",
		5,
	}

	r2 := Route{
		net.ParseIP("255.255.0.0"),
		16,
		"test",
		3,
	}

	r3 := Route{
		net.ParseIP("255.0.0.0"),
		8,
		"test",
		2,
	}

	r4 := Route{
		net.ParseIP("240.0.0.0"),
		4,
		"test",
		1,
	}

	routes := [4]Route{r1, r2, r3, r4}

	for _, rt := range routes {
		payload, err := json.Marshal(rt)
		if err != nil {
			t.Errorf("Cannot marshal route")
		}

		res := sendRequest(string(payload), "PUT", *insertURL, t)
		if res.StatusCode != 200 {
			t.Errorf("Cannot create route")
		}
	}

	u, _ := url.ParseRequestURI(endpoint)
	u.Path = appPath + "/255.255.255.255"

	res := sendRequest("", "GET", fmt.Sprintf("%v", u), t)
	if res.StatusCode != 200 {
		t.Errorf("Expected 200 OK, got : %d", res.StatusCode)
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		t.Errorf("Cannot read response body")
	}

	var actual Route
	err = json.Unmarshal(body, &actual)
	if err != nil {
		t.Errorf("Cannot unmarshal response body")
	}

	if !(compareRoute(actual, r1)) {
		t.Errorf("Expected doesn't match actual")
	}
}

/*
 Test getting a route after it has been deleted.
*/
func TestLPMGetAfterDelete(t *testing.T) {
	testLog.Println("TestGetAfterDelete HTTP")

	payload := "{\"dest\":\"12.0.6.0\",\"len\":30, \"name\":\"Route1\", \"priority\":1}"
	res := sendRequest(payload, "PUT", *insertURL, t)
	if res.StatusCode != 200 {
		t.Errorf("Cannot create route")
	}

	payload = "{\"dest\":\"12.0.6.0\", \"len\":30}"
	u, _ := url.ParseRequestURI(endpoint)
	u.Path = appPath + "/12.0.6.0"

	res = sendRequest(payload, "DELETE", fmt.Sprintf("%v", u), t)
	if res.StatusCode != 200 {
		t.Errorf("Cannot delete route")
	}

	u, _ = url.ParseRequestURI(endpoint)
	u.Path = appPath + "/12.0.6.12"

	res = sendRequest("", "GET", fmt.Sprintf("%v", u), t)
	if res.StatusCode != 200 {
		t.Errorf("Expected 200 OK, got : %d", res.StatusCode)
	}

	if res.ContentLength != 0 {
		t.Errorf("Expected a miss, got content")
	}
	teardownTest()
}

/*
Test a simple LPM where there is only one entry in the database and it is a hit
*/
func TestSimpleLPM(t *testing.T) {
	setupTest()
	testLog.Println("TestSimpleLPM")

	r := Route{
		net.ParseIP("1.1.1.1"),
		16,
		"hello",
		2,
	}

	_, err := kv.Process(context.Background(), Put(r))
	if err != nil {
		t.Error("Error inserting route: ", r)
	}

	res, err := kv.Process(context.Background(), Get(net.ParseIP("1.1.1.1")))
	if err == nil {
		if res == nil || !(compareRoute(r, res.(Route))) {
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

	r := Route{
		net.ParseIP("255.255.255.255"),
		16,
		"hello",
		2,
	}

	_, err := kv.Process(context.Background(), Put(r))
	if err != nil {
		t.Error("Error inserting route: ", err, r)
	}

	res, err := kv.Process(context.Background(), Get(net.ParseIP("4.4.4.4")))
	if err == nil {
		if err == nil {
			if res != nil {
				t.Error("Found a result when there should not have been one: ", res)
			}
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	teardownTest()
}

/*
Test where the prefix does not match
*/
func TestMiss2(t *testing.T) {
	setupTest()
	testLog.Println("TestMiss2")

	r := Route{
		net.ParseIP("0.0.255.255"),
		24,
		"hello",
		2,
	}

	_, err := kv.Process(context.Background(), Put(r))
	if err != nil {
		t.Error("Error inserting route: ", err, r)
	}

	res, err := kv.Process(context.Background(), Get(net.ParseIP("255.0.0.0")))
	if err == nil {
		if res != nil {
			t.Error("Found a result when there should not have been one: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	teardownTest()
}

/*
Test the db is empty
*/
func TestMissEmpty(t *testing.T) {
	setupTest()
	testLog.Println("TestMissEmpty")

	res, err := kv.Process(context.Background(), Get(net.ParseIP("255.0.0.0")))

	if err == nil {
		if res != nil {
			t.Error("Found a result when there should not have been one: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	teardownTest()
}

/*
Test that LPM returns the prefix with the highest priority
*/
func TestHighPriority1(t *testing.T) {
	setupTest()
	testLog.Println("TestHighPriority1")

	r1 := Route{
		net.ParseIP("123.123.123.123"),
		32,
		"test",
		1,
	}

	r2 := Route{
		net.ParseIP("123.123.123.123"),
		24,
		"test",
		2,
	}

	r3 := Route{
		net.ParseIP("123.123.123.123"),
		16,
		"test",
		3,
	}

	r4 := Route{
		net.ParseIP("123.123.123.123"),
		8,
		"test",
		10,
	}

	_, err := kv.Process(context.Background(), Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), Put(r3))
	if err != nil {
		t.Error("Error inserting route: ", err, r3)
	}

	_, err = kv.Process(context.Background(), Put(r4))
	if err != nil {
		t.Error("Error inserting route: ", err, r4)
	}

	res, err := kv.Process(context.Background(), Get(net.ParseIP("123.123.123.123")))
	if err == nil {
		if res == nil || !(compareRoute(r4, res.(Route))) {
			t.Error("Returned the wrong result: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	teardownTest()
}

/*
Tests that when all the matching entries have same priority, return the one with the longest length
*/
func TestHighPriority2(t *testing.T) {
	setupTest()
	testLog.Println("TestHighPriority2")

	r1 := Route{
		net.ParseIP("123.123.123.123"),
		32,
		"test",
		1,
	}

	r2 := Route{
		net.ParseIP("123.123.123.123"),
		24,
		"test",
		1,
	}

	r3 := Route{
		net.ParseIP("123.123.123.123"),
		16,
		"test",
		1,
	}

	r4 := Route{
		net.ParseIP("123.123.123.123"),
		8,
		"test",
		1,
	}

	_, err := kv.Process(context.Background(), Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), Put(r3))
	if err != nil {
		t.Error("Error inserting route: ", err, r3)
	}

	_, err = kv.Process(context.Background(), Put(r4))
	if err != nil {
		t.Error("Error inserting route: ", err, r4)
	}

	res, err := kv.Process(context.Background(), Get(net.ParseIP("123.123.123.123")))
	if err == nil {
		if res == nil || !(compareRoute(r1, res.(Route))) {
			t.Error("Returned the wrong result: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	teardownTest()
}

/*
Tests that it will return the one with the highest priority when it is the longest
*/
func TestHighPriority3(t *testing.T) {
	setupTest()
	testLog.Println("TestHighPriority3")

	r1 := Route{
		net.ParseIP("123.123.123.123"),
		32,
		"test",
		5,
	}

	r2 := Route{
		net.ParseIP("123.123.123.123"),
		24,
		"test",
		3,
	}

	r3 := Route{
		net.ParseIP("123.123.123.123"),
		16,
		"test",
		2,
	}

	r4 := Route{
		net.ParseIP("123.123.123.123"),
		8,
		"test",
		1,
	}

	_, err := kv.Process(context.Background(), Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), Put(r3))
	if err != nil {
		t.Error("Error inserting route: ", err, r3)
	}

	_, err = kv.Process(context.Background(), Put(r4))
	if err != nil {
		t.Error("Error inserting route: ", err, r4)
	}

	res, err := kv.Process(context.Background(), Get(net.ParseIP("123.123.123.123")))
	if err == nil {
		if res == nil || !(compareRoute(r1, res.(Route))) {
			t.Error("Returned the wrong result: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	teardownTest()
}

/*
Test where the prefix that matches is the shortest one
*/
func TestPrefixMatchShort(t *testing.T) {
	setupTest()
	testLog.Println("TestPrefixMatchShort")

	r1 := Route{
		net.ParseIP("255.255.255.255"),
		32,
		"test",
		5,
	}

	r2 := Route{
		net.ParseIP("255.255.255.255"),
		24,
		"test",
		3,
	}

	r3 := Route{
		net.ParseIP("255.255.255.255"),
		16,
		"test",
		2,
	}

	r4 := Route{
		net.ParseIP("255.255.255.255"),
		8,
		"test",
		1,
	}

	_, err := kv.Process(context.Background(), Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), Put(r3))
	if err != nil {
		t.Error("Error inserting route: ", err, r3)
	}

	_, err = kv.Process(context.Background(), Put(r4))
	if err != nil {
		t.Error("Error inserting route: ", err, r4)
	}

	res, err := kv.Process(context.Background(), Get(net.ParseIP("255.0.0.0")))
	if err == nil {
		if res == nil || !(compareRoute(r4, res.(Route))) {
			t.Error("Returned the wrong result: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	teardownTest()
}

/*
Test where the prefix that matches is the longest one
*/
func TestPrefixMatchLong(t *testing.T) {
	setupTest()
	testLog.Println("TestPrefixMatchLong")

	r1 := Route{
		net.ParseIP("255.255.240.0"),
		20,
		"test",
		5,
	}

	r2 := Route{
		net.ParseIP("255.255.0.0"),
		16,
		"test",
		3,
	}

	r3 := Route{
		net.ParseIP("255.0.0.0"),
		8,
		"test",
		2,
	}

	r4 := Route{
		net.ParseIP("240.0.0.0"),
		4,
		"test",
		1,
	}

	_, err := kv.Process(context.Background(), Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), Put(r3))
	if err != nil {
		t.Error("Error inserting route: ", err, r3)
	}

	_, err = kv.Process(context.Background(), Put(r4))
	if err != nil {
		t.Error("Error inserting route: ", err, r4)
	}

	res, err := kv.Process(context.Background(), Get(net.ParseIP("255.255.255.255")))
	if err == nil {
		if res == nil || !(compareRoute(r1, res.(Route))) {
			t.Error("Returned the wrong result: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	teardownTest()
}

/*
Test a delete, result should be null
*/
func TestExactDelete1(t *testing.T) {
	setupTest()
	testLog.Println("TestExactDelete1")

	r := Route{
		net.ParseIP("255.255.255.255"),
		16,
		"hello",
		2,
	}

	d := Del{
		net.ParseIP("255.255.255.255"),
		16,
		true,
	}

	_, err := kv.Process(context.Background(), Put(r))
	if err != nil {
		t.Error("Error inserting route: ", err, r)
	}

	_, err = kv.Process(context.Background(), d)
	if err != nil {
		t.Error("Error deleting route: ", err, d)
	}

	res, err := kv.Process(context.Background(), Get(net.ParseIP("255.255.255.255")))
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

	r1 := Route{
		net.ParseIP("255.255.255.255"),
		16,
		"hello",
		10,
	}

	r2 := Route{
		net.ParseIP("255.255.255.255"),
		8,
		"hello",
		1,
	}

	d := Del{
		net.ParseIP("255.255.255.0"),
		32,
		true,
	}

	_, err := kv.Process(context.Background(), Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), d)
	if err != nil {
		t.Error("Error deleting route: ", err, d)
	}

	res, err := kv.Process(context.Background(), Get(net.ParseIP("255.255.255.255")))
	if err == nil {
		if res == nil || !(compareRoute(r1, res.(Route))) {
			t.Error("Returned the wrong result: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	teardownTest()
}

/*
Test a non exact delete, should return nothing
*/
func TestNonExactDelete1(t *testing.T) {
	setupTest()
	testLog.Println("TestNonExactDelete1")

	r1 := Route{
		net.ParseIP("255.255.255.255"),
		16,
		"hello",
		10,
	}

	r2 := Route{
		net.ParseIP("255.255.255.255"),
		8,
		"hello",
		1,
	}

	r3 := Route{
		net.ParseIP("255.255.255.255"),
		32,
		"hello",
		1,
	}

	r4 := Route{
		net.ParseIP("255.255.255.255"),
		20,
		"hello",
		1,
	}

	r5 := Route{
		net.ParseIP("255.255.255.255"),
		10,
		"hello",
		1,
	}

	d := Del{
		net.ParseIP("255.255.255.255"),
		8,
		false,
	}

	_, err := kv.Process(context.Background(), Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), Put(r3))
	if err != nil {
		t.Error("Error inserting route: ", err, r3)
	}

	_, err = kv.Process(context.Background(), Put(r4))
	if err != nil {
		t.Error("Error inserting route: ", err, r4)
	}

	_, err = kv.Process(context.Background(), Put(r5))
	if err != nil {
		t.Error("Error inserting route: ", err, r5)
	}

	_, err = kv.Process(context.Background(), d)
	if err != nil {
		t.Error("Error deleting route: ", err, d)
	}

	res, err := kv.Process(context.Background(), Get(net.ParseIP("255.255.255.255")))
	if err == nil {
		if res != nil {
			t.Error("Found a result when there should not have been one: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	teardownTest()
}

/*
Test a non exact delete with varying prefixes
*/
func TestNonExactDelete2(t *testing.T) {
	setupTest()
	testLog.Println("TestNonExactDelete2")

	r1 := Route{
		net.ParseIP("255.255.0.0"),
		16,
		"hello",
		10,
	}

	r2 := Route{
		net.ParseIP("255.255.255.123"),
		8,
		"hello",
		1,
	}

	r3 := Route{
		net.ParseIP("255.255.255.255"),
		32,
		"hello",
		1,
	}

	r4 := Route{
		net.ParseIP("255.255.255.240"),
		20,
		"hello",
		1,
	}

	r5 := Route{
		net.ParseIP("255.255.20.0"),
		10,
		"hello",
		1,
	}

	d := Del{
		net.ParseIP("255.255.255.255"),
		8,
		false,
	}

	_, err := kv.Process(context.Background(), Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), Put(r3))
	if err != nil {
		t.Error("Error inserting route: ", err, r3)
	}

	_, err = kv.Process(context.Background(), Put(r4))
	if err != nil {
		t.Error("Error inserting route: ", err, r4)
	}

	_, err = kv.Process(context.Background(), Put(r5))
	if err != nil {
		t.Error("Error inserting route: ", err, r5)
	}

	_, err = kv.Process(context.Background(), d)
	if err != nil {
		t.Error("Error deleting route: ", err, d)
	}

	res, err := kv.Process(context.Background(), Get(net.ParseIP("255.255.255.255")))
	if err == nil {
		if res != nil {
			t.Error("Found a result when there should not have been one: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	teardownTest()
}

/*
Test a non exact delete that leaves some prefixes behind
*/
func TestNonExactDelete3(t *testing.T) {
	setupTest()
	testLog.Println("TestNonExactDelete3")

	r1 := Route{
		net.ParseIP("255.255.0.0"),
		16,
		"hello",
		10,
	}

	r2 := Route{
		net.ParseIP("255.255.255.123"),
		8,
		"hello",
		1,
	}

	r3 := Route{
		net.ParseIP("255.255.255.255"),
		32,
		"hello",
		1,
	}

	r4 := Route{
		net.ParseIP("255.255.255.240"),
		20,
		"hello",
		1,
	}

	r5 := Route{
		net.ParseIP("255.255.20.0"),
		10,
		"hello",
		1,
	}

	r6 := Route{
		net.ParseIP("255.255.255.255"),
		4,
		"hello",
		5,
	}

	r7 := Route{
		net.ParseIP("255.255.20.0"),
		3,
		"hello",
		2,
	}

	r8 := Route{
		net.ParseIP("255.255.255.255"),
		6,
		"hello",
		1,
	}

	d := Del{
		net.ParseIP("255.255.255.255"),
		8,
		false,
	}

	_, err := kv.Process(context.Background(), Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), Put(r3))
	if err != nil {
		t.Error("Error inserting route: ", err, r3)
	}

	_, err = kv.Process(context.Background(), Put(r4))
	if err != nil {
		t.Error("Error inserting route: ", err, r4)
	}

	_, err = kv.Process(context.Background(), Put(r5))
	if err != nil {
		t.Error("Error inserting route: ", err, r5)
	}

	_, err = kv.Process(context.Background(), Put(r6))
	if err != nil {
		t.Error("Error inserting route: ", err, r6)
	}

	_, err = kv.Process(context.Background(), Put(r7))
	if err != nil {
		t.Error("Error inserting route: ", err, r7)
	}

	_, err = kv.Process(context.Background(), Put(r8))
	if err != nil {
		t.Error("Error inserting route: ", err, r8)
	}

	_, err = kv.Process(context.Background(), d)
	if err != nil {
		t.Error("Error deleting route: ", err, d)
	}

	res, err := kv.Process(context.Background(), Get(net.ParseIP("255.255.255.255")))
	if err == nil {
		if res == nil || !(compareRoute(r6, res.(Route))) {
			t.Error("Returned the wrong result: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	teardownTest()
}

/*
Test a non exact delete, should return nothing
*/
func TestNonExactDeleteMiss(t *testing.T) {
	setupTest()
	testLog.Println("TestNonExactDeleteMiss")

	r1 := Route{
		net.ParseIP("255.255.255.255"),
		16,
		"hello",
		10,
	}

	r2 := Route{
		net.ParseIP("255.255.255.255"),
		8,
		"hello",
		1,
	}

	r3 := Route{
		net.ParseIP("255.255.255.255"),
		32,
		"hello",
		3,
	}

	r4 := Route{
		net.ParseIP("255.255.255.255"),
		20,
		"hello",
		6,
	}

	r5 := Route{
		net.ParseIP("255.255.255.255"),
		10,
		"hello",
		2,
	}

	d := Del{
		net.ParseIP("123.123.123.123"),
		8,
		false,
	}

	_, err := kv.Process(context.Background(), Put(r1))
	if err != nil {
		t.Error("Error inserting route: ", err, r1)
	}

	_, err = kv.Process(context.Background(), Put(r2))
	if err != nil {
		t.Error("Error inserting route: ", err, r2)
	}

	_, err = kv.Process(context.Background(), Put(r3))
	if err != nil {
		t.Error("Error inserting route: ", err, r3)
	}

	_, err = kv.Process(context.Background(), Put(r4))
	if err != nil {
		t.Error("Error inserting route: ", err, r4)
	}

	_, err = kv.Process(context.Background(), Put(r5))
	if err != nil {
		t.Error("Error inserting route: ", err, r5)
	}

	_, err = kv.Process(context.Background(), d)
	if err != nil {
		t.Error("Error deleting route: ", err, d)
	}

	res, err := kv.Process(context.Background(), Get(net.ParseIP("255.255.255.255")))
	if err == nil {
		if res == nil || !(compareRoute(r1, res.(Route))) {
			t.Error("Returned the wrong result: ", res)
		}
	} else {
		t.Error("Error calculating LPM: ", err)
	}

	teardownTest()
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
