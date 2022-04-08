package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

type Job struct {
	Status        int    `json:"status,omitempty"`
	Error         string `json:"error,omitempty"`
	ID            string `json:"id,omitempty"`
	QueryResultID int    `json:"query_result_id,omitempty"`
	UpdatedAt     int    `json:"updated_at,omitempty"`
}

type QueryResult struct {
	RetrievedAt time.Time `json:"retrieved_at,omitempty"`
	QueryHash   string    `json:"query_hash,omitempty"`
	Query       string    `json:"query,omitempty"`
	Runtime     float64   `json:"runtime,omitempty"`
	Data        struct {
		Rows    []map[string]interface{} `json:"rows,omitempty"`
		Columns []map[string]string      `json:"columns,omitempty"`
	} `json:"data,omitempty"`
	ID           int `json:"id,omitempty"`
	DataSourceID int `json:"data_source_id,omitempty"`
}

type Response struct {
	Job         Job         `json:"job,omitempty"`
	QueryResult QueryResult `json:"query_result,omitempty"`
}

type App struct {
	RedashURl string
	Bind      string
	MaxWait   int
	Config    Config
	RDB       *redis.Client
}

func (a App) Start() {
	r := gin.New()

	// Global middleware
	// Logger middleware will write the logs to gin.DefaultWriter even if you set with GIN_MODE=release.
	// By default gin.DefaultWriter = os.Stdout
	r.Use(gin.Logger())

	// Recovery middleware recovers from any panics and writes a 500 if there was one.
	r.Use(gin.Recovery())

	r.POST("api/queries/:query/results", a.queryHandler)

	log.Fatal(r.Run(a.Bind))
}

type TableTimer struct {
	Name string `yaml:"name"`
	From int    `yaml:"from"`
	To   int    `yaml:"to"`
}

type AggFunc struct {
	Name string `yaml:"name"`
	Time int    `yaml:"time"`
}

type Config struct {
	Port             int
	Bind             string
	Layout           string
	RedashURL        string
	MaxWait          int
	MaxBlock         int          `yaml:"maxBlock"`
	TableTimers      []TableTimer `yaml:"tableTimers"`
	AggFuncs         []AggFunc    `yaml:"AggFuncs"`
	AllowSplitBucket string       `yaml:"AllowSplitBucket"`
}

func main() {
	redashURL := flag.String("redash", "https://redash.evgcdn.net/", "Redash url")
	bind := flag.String("bind", "0.0.0.0:8080", "Redash url")
	maxWait := flag.Int("wait", 100, "Max wait in seconds")
	redisHost := flag.String("redis", "localhost:2181", "redis url")
	redisDB := flag.Int("redisDB", 0, "redisDB")
	configFile := flag.String("config", "config.yaml", "Config file")
	flag.Parse()

	dat, err := ioutil.ReadFile(*configFile)
	if err != nil {
		panic(err)
	}
	var config Config
	err = yaml.Unmarshal([]byte(dat), &config)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	var a App
	a.Bind = fmt.Sprintf("%s:%d", config.Bind, config.Port)
	if config.Bind == "" {
		a.Bind = *bind
	}
	a.RedashURl = config.RedashURL
	if config.RedashURL == "" {
		a.RedashURl = *redashURL
	}
	a.MaxWait = config.MaxWait
	if config.MaxWait == 0 {
		a.MaxWait = *maxWait
	}
	a.Config = config

	rdb := redis.NewClient(&redis.Options{
		Addr:     *redisHost,
		Password: "",       // no password set
		DB:       *redisDB, // use default DB
	})
	a.RDB = rdb
	a.Start()

}

func selectTable(TableTimers []TableTimer, diff int) TableTimer {
	table := TableTimer{}
	for _, t := range TableTimers {
		if t.From < diff && (t.To > diff || t.To == -1) {
			table = t
			break
		}
	}
	return table
}

func selectAggFunc(AggFuncs []AggFunc, maxBlock, diff int) string {
	aggFunc := AggFuncs[len(AggFuncs)-1].Name
	block := 0
	for _, af := range AggFuncs {
		if diff/af.Time < maxBlock && diff/af.Time > block {
			aggFunc = af.Name
			block = int(diff / af.Time)
		}
	}
	return aggFunc
}

func (a *App) queryHandler(context *gin.Context) {
	key := context.Request.Header.Get("Authorization")
	query := context.Param("query")
	crashOnEmpty := context.Request.URL.Query().Get("crashOnEmpty")
	data, err := ioutil.ReadAll(context.Request.Body)
	if err != nil {
		panic(err)
	}
	output, err := a.doProxy(data, query, key)
	if err != nil {
		panic(err)
	}

	var finalResult Response
	err = json.Unmarshal(output, &finalResult)
	if err != nil {
		panic(err)
	}
	finalResult.QueryResult.Query = ""
	finalResult.QueryResult.QueryHash = ""
	if crashOnEmpty == "1" {
		if len(finalResult.QueryResult.Data.Rows) == 0 {
			panic(fmt.Errorf("Empty result \n"))
		}
	}

	defer func() {
		log.Println(string(data), toString(finalResult.QueryResult.Data.Rows))
	}()

	removedQueryData, err := json.Marshal(finalResult)
	if err != nil {
		panic(err)
	}
	context.String(200, string(removedQueryData))
}

type proxyResponse struct {
	data Response
	err  error
}

func (a *App) doProxy(jsonStr []byte, query, key string) ([]byte, error) {
	data := make(map[string]interface{})
	err := json.Unmarshal(jsonStr, &data)
	if err != nil {
		return nil, err
	}
	data["max_age"] = 1

	if params, ok := data["parameters"]; ok {
		d, ok := params.(map[string]interface{})["_agg"]
		d2, ok2 := params.(map[string]interface{})["_time"]
		buckets := params.(map[string]interface{})["bucket"]

		// auto time and agg function
		if (ok || ok2) && (d == "auto" || d2 == "auto") {
			for {
				fromTime, ok := params.(map[string]interface{})["fromTime"]
				if !ok {
					break
				}
				fromDate, err := time.Parse(a.Config.Layout, fromTime.(string))
				if err != nil {
					break
				}

				toTime, ok := params.(map[string]interface{})["toTime"]
				if !ok {
					toTime, ok = params.(map[string]interface{})["Time"]
					if !ok {
						break
					}
				}
				toDate, err := time.Parse(a.Config.Layout, toTime.(string))
				if err != nil {
					break
				}
				diff := int(toDate.Sub(fromDate).Seconds())

				table := selectTable(a.Config.TableTimers, diff)
				functionNames := selectAggFunc(a.Config.AggFuncs, a.Config.MaxBlock, diff)

				if ok && d == "auto" {
					params.(map[string]interface{})["_agg"] = table.Name
				}
				if ok2 && d2 == "auto" {
					params.(map[string]interface{})["_time"] = functionNames
				}
				break
			}
		}

		// split bucket
		part, ok := params.(map[string]interface{})["_part"]
		allowSplit := allowBucketSplit(a.Config.AllowSplitBucket, buckets.(string))
		if ok && part == "auto" && allowSplit {
			wg := &sync.WaitGroup{}
			wg.Add(10)

			result := make(chan proxyResponse, 1)

			for i := 0; i < 10; i++ {
				go func(threadNumber int, rawRequest []byte) {
					var response proxyResponse
					var d map[string]interface{}
					err := json.Unmarshal(rawRequest, &d)
					if err != nil {
						response.err = err
						result <- response
						return
					}

					p := d["parameters"]
					p.(map[string]interface{})["_part"] = fmt.Sprintf("%d", threadNumber)
					partRequestQuery, err := json.Marshal(d)
					if err != nil {
						response.err = err
						result <- response
						return
					}
					partResult, err := a.doProxy(partRequestQuery, query, key)
					if err != nil {
						response.err = err
						result <- response
						return
					}
					err = json.Unmarshal(partResult, &response.data)
					if err != nil {
						response.err = err
						result <- response
						return
					}

					log.Printf("Part %d  result %s \n", threadNumber, toString(response.data.QueryResult.Data.Rows))
					result <- response
					return
				}(i, jsonStr)
			}

			var allResults []proxyResponse
			var errs []string
			go func() {
				for t := range result {
					if len(t.data.QueryResult.Data.Rows) > 0 {
						allResults = append(allResults, t)
					}
					if t.err != nil {
						errs = append(errs, err.Error())
					}
					wg.Done()
				}
			}()

			wg.Wait()
			if len(errs) > 0 {
				return nil, fmt.Errorf("Something error %s\n", strings.Join(errs, ","))
			}
			return a.mergeResult(allResults)
		}

		// if not allow, reset _part to -1 and run
		if ok && part == "auto" && !allowSplit {
			params.(map[string]interface{})["_part"] = "-1"
		}
	}

	forceRequestQuery, err := json.Marshal(data)
	if err != nil {

		return nil, err
	}

	delete(data, "max_age")
	forceRequestQuery2, err := json.Marshal(data)
	if err != nil {

		return nil, err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/queries/%s/results", a.RedashURl, query), bytes.NewBuffer(forceRequestQuery))
	if err != nil {

		return nil, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("%s", key))
	req.Header.Set("Content-Type", "application/json")
	log.Println(fmt.Sprintf("%s/api/queries/%s/results", a.RedashURl, query), string(forceRequestQuery))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {

		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var response Response
	err = json.Unmarshal(respBody, &response)
	if err != nil {
		return nil, err
	}

	if response.Job.ID == "" {
		return nil, fmt.Errorf(string(respBody))
	}
	resultId := 0
	for i := 0; i < a.MaxWait; i++ {
		req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/jobs/%s", a.RedashURl, response.Job.ID), bytes.NewBuffer(forceRequestQuery2))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", fmt.Sprintf("%s", key))
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}

		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		_ = resp.Body.Close()
		var response Response
		err = json.Unmarshal(respBody, &response)
		if err != nil {
			return nil, err
		}
		if response.Job.Status == 3 || response.Job.Status == 4 {
			resultId = response.Job.QueryResultID
			break
		}
		time.Sleep(1 * time.Second)
	}
	if resultId == 0 {
		return nil, fmt.Errorf("Empty job\n")
	}

	req, err = http.NewRequest("POST", fmt.Sprintf("%s/api/queries/%s/results", a.RedashURl, query), bytes.NewBuffer(forceRequestQuery2))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("%s", key))
	req.Header.Set("Content-Type", "application/json")

	client = &http.Client{}
	resp, err = client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	respBody, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return respBody, nil
}

func allowBucketSplit(bucketConfig string, buckets string) bool {
	if bucketConfig == "" {
		return true
	}
	if buckets == "" {
		return false
	}
	for _, b := range strings.Split(buckets, ",") {
		if strings.Contains(bucketConfig, strings.ReplaceAll(b, "'", "")) {
			return true
		}
	}
	return false
}

func toString(rows []map[string]interface{}) string {
	data, err := json.Marshal(rows)
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (a *App) mergeResult(results []proxyResponse) ([]byte, error) {

	if len(results) == 0 {
		return nil, fmt.Errorf("mergeResult, Empty result\n")
	}

	result := results[0].data
	err := results[0].err

	for index, data := range results {
		if data.err != nil {
			err = data.err
			break
		}
		if index == 0 {
			continue
		}
		for rowIndex, row := range data.data.QueryResult.Data.Rows {
			if rowIndex >= len(result.QueryResult.Data.Rows) {
				return nil, fmt.Errorf("mergeResult, Result data not match\n")
			}
			for k, v := range row {
				if reflect.TypeOf(result.QueryResult.Data.Rows[rowIndex][k]).Name() != reflect.TypeOf(v).Name() {
					return nil, fmt.Errorf("mergeResult, Data type not match\n")
				}
				switch v.(type) {
				case string:
					result.QueryResult.Data.Rows[rowIndex][k] = ""
				case float64:
					result.QueryResult.Data.Rows[rowIndex][k] = result.QueryResult.Data.Rows[rowIndex][k].(float64) + v.(float64)
				default:
					continue
				}
			}
		}
	}
	if err != nil {
		return nil, err
	}
	result.QueryResult.Query = ""
	data, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return data, err
}
