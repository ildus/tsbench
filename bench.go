package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"os/user"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	pghost       = flag.String("host", "localhost", "Host")
	pgport       = flag.Int("port", 5432, "Port")
	pguser       = flag.String("user", "$USER", "User")
	pgdatabase   = flag.String("database", "postgres", "Database")
	connections  = flag.Int("connections", 0, "Connections count (by default number of cores)")
	bench_time   = flag.Int("bench_time", 60, "Bench time (seconds)")
	queries_file = flag.String("queries_file", "queries.txt", "File with queries")
	db           *sql.DB
	mu           sync.Mutex
	counter      uint64 = 0
)

const (
	QUERY = `
		select ts_rank_cd(fts, to_tsquery('russian', '%s')) as rank
		from items
		where fts @@ to_tsquery('russian', '%s')
		order by rank desc
		limit 1
	`
)

func openTestConn() (*sql.DB, error) {
	conninfo := fmt.Sprintf("postgres://%s@%s:%d/%s?sslmode=disable", *pguser,
		*pghost, *pgport, *pgdatabase)

	return sql.Open("postgres", conninfo)
}

func makeQuery(db *sql.DB, sql string) {
	var err error

	rows, err := db.Query(sql)
	defer rows.Close()
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		//do nothing
	}
}

func listener(wg *sync.WaitGroup, chm chan int, queries []string) {
	var err error
	var tsquery string

	defer wg.Done()

	db, err = openTestConn()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()
	for query_idx := range chm {
		tsquery = queries[query_idx]
		sql := fmt.Sprintf(QUERY, tsquery, tsquery)
		makeQuery(db, sql)
		atomic.AddUint64(&counter, 1)
	}
}

func processQueries() {
	var wg sync.WaitGroup
	chm := make(chan int, 1)
	bin, err := ioutil.ReadFile(*queries_file)

	if err != nil {
		log.Fatal(err)
	}
	queries := strings.Split(string(bin), "\n")

	for i := 0; i < *connections; i++ {
		wg.Add(1)
		go listener(&wg, chm, queries)
	}

	start_time := time.Now()
	for {
		for i := 0; i < len(queries); i++ {
			chm <- i

			if time.Since(start_time) > (time.Duration(*bench_time) * time.Second) {
				goto end
			}
		}
	}
end:
	close(chm)
	log.Print("Processed: ", atomic.LoadUint64(&counter))
	wg.Wait()
}

func main() {
	defer func() {
		if err := recover(); err != nil {
			log.Fatal("Program is exiting with exception: ", err)
		}
	}()

	go func() {
		sigchan := make(chan os.Signal, 10)
		signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)
		<-sigchan
		log.Println("Got interrupt signal. Exited")
		os.Exit(0)
	}()

	flag.Parse()
	if *pguser == "$USER" {
		userdata, err := user.Current()
		if err != nil {
			log.Fatal(err)
		}
		*pguser = userdata.Username
	}

	if *connections <= 0 {
		*connections = runtime.NumCPU()
	}

	log.Println("Number of connections: ", *connections)
	log.Println("Database: ", *pgdatabase)
	processQueries()
}
