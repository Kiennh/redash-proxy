package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gin-gonic/gin"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
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
	Port        int
	Bind        string
	Layout      string
	RedashURL   string
	MaxWait     int
	MaxBlock    int          `yaml:"maxBlock"`
	TableTimers []TableTimer `yaml:"tableTimers"`
	AggFuncs    []AggFunc    `yaml:"AggFuncs"`
	Part        int
	PartIgnore  []string
}

func main() {
	redashURL := flag.String("redash", "https://redash.evgcdn.net/", "Redash url")
	bind := flag.String("bind", "0.0.0.0:8080", "Redash url")
	maxWait := flag.Int("wait", 100, "Max wait in seconds")

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
	fmt.Println(">", config.MaxBlock)
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
	output, err := doProxy(data, a.RedashURl, query, key, a.Config.Layout, a.MaxWait, a.Config.MaxBlock, a.Config.TableTimers, a.Config.AggFuncs, crashOnEmpty)
	if err != nil {
		log.Println("Request body for error: ", string(data))
		panic(err)
	}
	context.String(200, string(output))
}

type proxyResponse struct {
	data Response
	err  error
}

func doProxy(jsonStr []byte, redashURL, query, key, layout string, maxWait, maxBlock int,
	tableTimers []TableTimer, aggFuncs []AggFunc, crashOnEmpty string) ([]byte, error) {
	data := make(map[string]interface{})
	err := json.Unmarshal(jsonStr, &data)
	if err != nil {
		return nil, err
	}
	fmt.Println(string(jsonStr))
	data["max_age"] = 1

	if params, ok := data["parameters"]; ok {
		d, ok := params.(map[string]interface{})["_agg"]
		d2, ok2 := params.(map[string]interface{})["_time"]
		if (ok || ok2) && (d == "auto" || d2 == "auto") {
			for {
				fromTime, ok := params.(map[string]interface{})["fromTime"]
				if !ok {
					break
				}
				fromDate, err := time.Parse(layout, fromTime.(string))
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
				toDate, err := time.Parse(layout, toTime.(string))
				if err != nil {
					break
				}
				diff := int(toDate.Sub(fromDate).Seconds())

				table := selectTable(tableTimers, diff)
				functionNames := selectAggFunc(aggFuncs, maxBlock, diff)

				fmt.Println(maxBlock, diff, functionNames)
				if ok && d == "auto" {
					params.(map[string]interface{})["_agg"] = table.Name
				}
				if ok2 && d2 == "auto" {
					params.(map[string]interface{})["_time"] = functionNames
				}
				break
			}
		}

		part, ok := params.(map[string]interface{})["_part"]
		if ok && part == "auto" {
			wg := &sync.WaitGroup{}
			wg.Add(10)

			result := make(chan proxyResponse, 1)

			for i := 0; i < 10; i++ {
				go func(threadNumber int, rawRequest []byte) {
					log.Println("Start", threadNumber)
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
					partResult, err := doProxy(partRequestQuery, redashURL, query, key, layout, maxWait, maxBlock, tableTimers, aggFuncs, crashOnEmpty)
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
					result <- response
					return
				}(i, jsonStr)
			}

			var allResults []proxyResponse
			go func() {
				for t := range result {
					log.Println("Received")
					allResults = append(allResults, t)
					wg.Done()
				}
			}()

			wg.Wait()

			return mergeResult(allResults)
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

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/queries/%s/results", redashURL, query), bytes.NewBuffer(forceRequestQuery))
	if err != nil {

		return nil, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("%s", key))
	req.Header.Set("Content-Type", "application/json")

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
	for i := 0; i < maxWait; i++ {
		req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/jobs/%s", redashURL, response.Job.ID), bytes.NewBuffer(forceRequestQuery2))
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

	req, err = http.NewRequest("POST", fmt.Sprintf("%s/api/queries/%s/results", redashURL, query), bytes.NewBuffer(forceRequestQuery2))
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
	var finalResult Response
	if crashOnEmpty == "1" {
		err := json.Unmarshal(respBody, &finalResult)
		if err != nil {
			return nil, err
		}
		if len(finalResult.QueryResult.Data.Rows) == 0 {
			return nil, fmt.Errorf("Empty result \n")
		}
	}
	return respBody, nil
}

func mergeResult(results []proxyResponse) ([]byte, error) {
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
