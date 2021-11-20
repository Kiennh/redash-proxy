package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gin-gonic/gin"
	"io/ioutil"
	"log"
	"net/http"
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
		Rows []struct {
			Query        []string `json:"query,omitempty"`
			SiteReadable string   `json:"Site readable,omitempty"`
			TotalSize    int64    `json:"total size,omitempty"`
		} `json:"rows,omitempty"`
		Columns []struct {
			FriendlyName string `json:"friendly_name,omitempty"`
			Type         string `json:"type,omitempty"`
			Name         string `json:"name,omitempty"`
		} `json:"columns,omitempty"`
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

func main() {
	redashURL := flag.String("redash", "https://redash.evgcdn.net/", "Redash url")
	bind := flag.String("bind", "0.0.0.0:8080", "Redash url")
	maxWait := flag.Int("wait", 100, "Max wait in seconds")
	flag.Parse()
	var a App
	a.RedashURl = *redashURL
	a.MaxWait = *maxWait
	a.Bind = *bind
	a.Start()
}

func (a *App) queryHandler(context *gin.Context) {
	key := context.Request.Header.Get("Authorization")
	query := context.Param("query")
	data, err := ioutil.ReadAll(context.Request.Body)
	if err != nil {
		panic(err)
	}
	output, err := doProxy(data, a.RedashURl, query, key, a.MaxWait)
	if err != nil {
		panic(err)
	}
	context.String(200, string(output))
}

func doProxy(jsonStr []byte, redashURL, query, key string, maxWait int) ([]byte, error) {
	data := make(map[string]interface{})
	err := json.Unmarshal(jsonStr, &data)
	if err != nil {
		return nil, err
	}

	data["max_age"] = 1

	forceRequestQuery, err := json.Marshal(data)
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
		req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/jobs/%s", redashURL, response.Job.ID), bytes.NewBuffer(jsonStr))
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

	req, err = http.NewRequest("POST", fmt.Sprintf("%s/api/queries/%s/results", redashURL, query), bytes.NewBuffer(jsonStr))
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
