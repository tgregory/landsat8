package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const informat = `02-01-2006`
const lstformat = `2006-01-02 15:04:05.000000`

type Time time.Time

func (t *Time) Set(v string) error {
	pt, e := time.Parse(informat, v)
	*t = Time(pt)
	return e
}

func (t *Time) String() string {
	if nil != t {
		return time.Time(*t).Format(informat)
	}
	return time.Time{}.Format(informat)
}

var zerod = Time{}

var from Time
var to Time
var sceneFile string
var descDir string
var nworkers int
var result string

func main() {
	flag.Var(&from, "f", "Lookup scenes starting from this day.")
	flag.Var(&to, "t", "Lookup scenes no older than this day.")
	flag.StringVar(&descDir, "d", "metadata", "Directory to save description files to.")
	flag.IntVar(&nworkers, "n", 4, "Number of simultaneous parallel downloads.")
	flag.StringVar(&sceneFile, "s", "scene_file", "Scene list file.")
	flag.StringVar(&result, "r", "result.txt", "Result of id query.")
	flag.Parse()

	if nworkers <= 0 {
		nworkers = 1
	}

	fr, err := os.Open(sceneFile)
	if nil != err {
		panic(err)
	}

	rw, err := os.Create(result)
	if nil != err {
		panic(err)
	}

	csvr := csv.NewReader(fr)

	// Read header
	fields, err := csvr.Read()

	if nil != err {
		panic(err)
	}

	queue := make(chan []string, nworkers)
	ids := make(chan string, nworkers)
	var wg sync.WaitGroup
	var wg2 sync.WaitGroup

	for i := 0; i < nworkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var ctr int
			for rec := range queue {
				id := rec[0]
				date, err := time.Parse(lstformat, rec[1])
				if nil != err {
					log.Printf("Failed to parse date: %v\n %v\n", err, rec)
					continue
				}
				if from != zerod && date.Before(time.Time(from)) {
					continue
				}
				if to != zerod && date.After(time.Time(to)) {
					continue
				}
				filename := id + `_MTL.txt`
				fp := filepath.Join(descDir, filename)
				url := strings.TrimSuffix(rec[10], `index.html`) + filename
				_, err = os.Stat(fp)
				if !os.IsNotExist(err) {
					log.Printf("File exists: %s\n", filename)
				} else {
					ctr++
					if ctr%20 == 0 {
						ctr = 0
						time.Sleep(3 * time.Second)
					}
					func() {
						res, err := http.Get(url)
						defer res.Body.Close()
						if nil != err {
							log.Printf("Download failed: %v\n", err)
							return
						}
						out, err := os.Create(fp)
						defer out.Close()
						if nil != err {
							log.Printf("Local file creation failed: %s\n", filename)
							return
						}
						_, err = io.Copy(out, res.Body)
						if nil != err {
							log.Printf("Failed to save remote data: %v\n", err)
						}
					}()
				}

				func() {
					mtl, err := os.Open(fp)
					defer mtl.Close()
					if nil != err {
						log.Printf(`Failed to open MTL "%s": %v\n`, fp, err)
						return
					}

					mtls := bufio.NewScanner(mtl)

					for mtls.Scan() {
						l := strings.Trim(mtls.Text(), " ")
						if strings.HasPrefix(l, `SUN_ELEVATION = `) {
							l = strings.TrimPrefix(l, `SUN_ELEVATION = `)
							val, _ := strconv.ParseFloat(l, 64)
							if val < 0 {
								ids <- id
							}
							return
						}
					}
				}()
			}
		}()
	}

	wg2.Add(1)
	go func() {
		defer wg2.Done()

		for id := range ids {
			_, err := rw.WriteString(id + "\n")
			if nil != err {
				log.Printf(`Failed to write id "%s": %v\n`, id, err)
			}
		}
	}()

	for fields, err = csvr.Read(); io.EOF != err; fields, err = csvr.Read() {
		if nil != err {
			log.Printf("Error reading csv line: %v\n", err)
			continue
		}
		queue <- fields
	}
	close(queue)
	wg.Wait()
	close(ids)
	wg2.Wait()
	rw.Close()
}
