package main

import (
	"database/sql"
	"flag"
	"fmt"
	"gopkg.in/cheggaaa/pb.v1"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"
)

var (
	url   = flag.String("url", "https://loripsum.net/api/10000/long/plaintext", "URL")
	count = flag.Int("count", 1000, "fill requests count")
)

const (
	INSERT_QUERY = `
		begin;
		insert into items (fts) values (to_tsvector('%s'));
		commit;
	`
)

func getter(wg *sync.WaitGroup, chm chan int) {
	var err error
	var db *sql.DB

	defer wg.Done()

	db, err = openTestConn()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()
	for range chm {
		resp, err := http.Get(*url)
		if err != nil {
			log.Println("Request error:", err)
			break
		}

		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		sql := fmt.Sprintf(INSERT_QUERY, strings.Replace(string(body), "\n", "", -1))
		makeQuery(db, sql)
	}
}

func getTexts() {
	var wg sync.WaitGroup
	chm := make(chan int, 1)

	for i := 0; i < *connections; i++ {
		wg.Add(1)
		go getter(&wg, chm)
	}

	bar := pb.StartNew(*count)
	for i := 0; i < *count; i++ {
		chm <- i
		bar.Increment()
	}

	close(chm)
	log.Printf("Inserted %d texts\n", *count)
	wg.Wait()
}
