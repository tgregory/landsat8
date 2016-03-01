package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
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

var (
	UnexpectedLevel = errors.New("Unexpected processing level")
)

const metaDir = "metadata"
const bandDir = "bands"

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

type ints []int

func (b *ints) String() string {
	return fmt.Sprintf("%d", *b)
}

func (b *ints) Set(value string) error {
	val, err := strconv.Atoi(value)
	if nil != err {
		return err
	}
	*b = append(*b, val)
	return nil
}

type plevel int

const (
	L1T  = plevel(0)
	L1GT = plevel(1)
)

type Scene interface {
	Id() string
	Acquisition() time.Time
	CloudCover() float64
	ProcessingLevel() plevel
	IsDay() (bool, error)
	GetBQA() (io.ReadCloser, error)
	GetBand(band int) (io.ReadCloser, error)
	GetMeta() (io.ReadCloser, error)
}

type Repository interface {
	GetScene(rec []string) (Scene, error)
}

type meta struct {
	day bool
}

type httpScene struct {
	repo            *HttpRepository
	id              string
	acquisitionDate time.Time
	cloudCover      float64
	processingLevel plevel
	baseUrl         string
	meta            *meta
}

func (s *httpScene) Id() string {
	return s.id
}

func (s *httpScene) Acquisition() time.Time {
	return s.acquisitionDate
}

func (s *httpScene) CloudCover() float64 {
	return s.cloudCover
}

func (s *httpScene) ProcessingLevel() plevel {
	return s.processingLevel
}

func (s *httpScene) IsDay() (bool, error) {
	if nil == s.meta {
		s.meta = &meta{}
		r, err := s.GetMeta()
		if nil != err {
			log.Printf(`Failed to open MTL: %v\n`, err)
			return false, err
		}
		defer r.Close()
		mtls := bufio.NewScanner(r)
		for mtls.Scan() {
			l := strings.Trim(mtls.Text(), " ")
			if strings.HasPrefix(l, `SUN_ELEVATION = `) {
				l = strings.TrimPrefix(l, `SUN_ELEVATION = `)
				val, _ := strconv.ParseFloat(l, 64)
				if val >= 0 {
					s.meta.day = true
				}
				break
			}
		}
	}
	return s.meta.day, nil
}

func (s *httpScene) GetBQA() (io.ReadCloser, error) {
	return s.repo.getBQA(s.id, s.baseUrl)
}

func (s *httpScene) GetBand(band int) (io.ReadCloser, error) {
	return s.repo.getBand(s.id, band, s.baseUrl)
}

func (s *httpScene) GetMeta() (io.ReadCloser, error) {
	return s.repo.getMeta(s.id, s.baseUrl)
}

type HttpRepository struct {
	Client      http.Client
	CachePath   string
	NoCacheMeta bool
	CacheBands  bool
}

func (r *HttpRepository) GetScene(rec []string) (Scene, error) {
	accDate, err := time.Parse(lstformat, rec[1])
	if nil != err {
		return nil, err
	}
	cc, err := strconv.ParseFloat(rec[2], 64)
	if nil != err {
		return nil, err
	}
	var pl plevel
	if "L1T" == rec[3] {
		pl = L1T
	} else if "L1GT" == rec[3] {
		pl = L1GT
	} else {
		return nil, UnexpectedLevel
	}
	return &httpScene{
		r,
		rec[0],
		accDate,
		cc,
		pl,
		strings.TrimSuffix(rec[10], `index.html`),
		nil,
	}, nil
}

func (r *HttpRepository) get(url string, cached string, cache bool) (io.ReadCloser, error) {
	var err error
	_, err = os.Stat(cached)
	if os.IsNotExist(err) {
		res, err := http.Get(url)
		if !cache {
			return res.Body, err
		}
		if nil != err {
			log.Printf("Request failed: %v\n", err)
			return nil, err
		}
		defer res.Body.Close()
		if nil != err {
			log.Printf("Download failed: %v\n", err)
			return nil, err
		}
		out, err := os.Create(cached)
		defer out.Close()
		if nil != err {
			log.Printf("Local file creation failed: %s\n", cached)
			return nil, err
		}
		_, err = io.Copy(out, res.Body)
		if nil != err {
			log.Printf("Failed to save remote data: %v\n", err)
		}
	} else {
		log.Printf("Cached copy found: %s\b", cached)
	}
	return os.Open(cached)
}

func (r *HttpRepository) getMeta(id string, baseUrl string) (io.ReadCloser, error) {
	fname := id + "_MTL.txt"
	return r.get(baseUrl+fname, filepath.Join(r.CachePath, metaDir, id+fname), !r.NoCacheMeta)
}

func (r *HttpRepository) getBand(id string, band int, baseUrl string) (io.ReadCloser, error) {
	fname := fmt.Sprintf("%s_B%d.TIF", id, band)
	return r.get(baseUrl+fname, filepath.Join(r.CachePath, bandDir, fname), r.CacheBands)
}

func (r *HttpRepository) getBQA(id string, baseUrl string) (io.ReadCloser, error) {
	fname := id + "_BQA.TIF"
	return r.get(baseUrl+fname, filepath.Join(r.CachePath, bandDir, fname), r.CacheBands)
}

var zerod = Time{}

var from Time
var to Time
var sceneFile string
var descDir string
var nworkers int
var result string
var timeout time.Duration
var retries int
var bands ints
var bqa bool
var dpath string

func main() {
	flag.Var(&from, "f", "Lookup scenes starting from this day.")
	flag.Var(&to, "t", "Lookup scenes no older than this day.")
	flag.StringVar(&descDir, "d", "metadata", "Directory to save description files to.")
	flag.IntVar(&nworkers, "n", 4, "Number of simultaneous parallel downloads.")
	flag.StringVar(&sceneFile, "s", "scene_list", "Scene list file.")
	flag.StringVar(&result, "r", "result.txt", "Result of id query.")
	flag.Var(&bands, "b", "List of bands.")
	flag.BoolVar(&bqa, "bqa", false, "Download BQA.")
	flag.StringVar(&dpath, "p", "download", "Path where data will be stored.")
	flag.DurationVar(&timeout, "timeout", 20*time.Minute, "HTTP request timeout.")
	flag.IntVar(&retries, "retries", 3, "Number of save retries.")
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

	repo := HttpRepository{
		Client: http.Client{
			Timeout: timeout,
		},
	}

	queue := make(chan []string, nworkers)
	ids := make(chan string, nworkers)
	var wg sync.WaitGroup
	var wg2 sync.WaitGroup

	for i := 0; i < nworkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rec := range queue {
				func() {
					var rtrs int
					var err error
					var day bool
					scene, err := repo.GetScene(rec)
					if nil != err {
						log.Printf("Failed to parse scene record: %s\n", err)
						return
					}
					if scene.Acquisition().Before(time.Time(from)) || scene.Acquisition().After(time.Time(to)) {
						return
					}
					for rtrs = retries; rtrs > 0; rtrs-- {
						day, err = scene.IsDay()
						if nil == err {
							break
						}
					}
					if 0 == rtrs {
						log.Printf("Failed to determine if scene is nighttime: %v\n", err)
						return
					}

					if !day {
						dir := filepath.Join(dpath, scene.Id())
						err = os.MkdirAll(dir, os.ModeDir|os.ModePerm)
						if nil != err {
							log.Printf("Failed to create storage directory: %v\n", err)
							return
						}
						fname := scene.Id() + "_MTL.txt"
						fpath := filepath.Join(dir, fname)
						for rtrs = retries; rtrs > 0; rtrs-- {
							out, err := os.Create(fpath)
							defer out.Close()
							if nil != err {
								log.Printf("Failed to create meta output file: %v\n", err)
								return
							}
							meta, err := scene.GetMeta()
							if nil != err {
								continue
							}
							_, err = io.Copy(out, meta)
							if nil != err {
								continue
							}
							break
						}
						if 0 == rtrs {
							log.Printf("Failed to download meta: %v\n", err)
							return
						}

						if bqa {
							fname := scene.Id() + "_BQA.TIF"
							fpath := filepath.Join(dir, fname)
							for rtrs = retries; rtrs > 0; rtrs-- {
								out, err := os.Create(fpath)
								defer out.Close()
								if nil != err {
									log.Printf("Failed to create BQA output file: %v\n", err)
									return
								}
								qual, err := scene.GetBQA()
								if nil != err {
									continue
								}
								_, err = io.Copy(out, qual)
								if nil != err {
									continue
								}
								break
							}
							if 0 == rtrs {
								log.Printf("Failed to download bqa: %v\n", err)
								return
							}
						}

						for _, band := range bands {
							fname := fmt.Sprintf("%s_B%d.TIF", scene.Id(), band)
							fpath := filepath.Join(dir, fname)
							for rtrs = retries; rtrs > 0; rtrs-- {
								out, err := os.Create(fpath)
								defer out.Close()
								if nil != err {
									log.Printf("Failed to create band output file: %v\n", err)
									return
								}
								ban, err := scene.GetBand(band)
								if nil != err {
									continue
								}
								_, err = io.Copy(out, ban)
								if nil != err {
									continue
								}
								break
							}
							if 0 == rtrs {
								log.Printf("Failed to download band: %v\n", err)
								return
							}
						}

						log.Printf("Done with %s\n", scene.Id())
						ids <- scene.Id()
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
