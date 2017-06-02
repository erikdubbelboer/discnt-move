package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/redis.v5"
)

func getNodeID(client *redis.Client) (string, error) {
	full, err := client.ClusterNodes().Result()
	if err != nil {
		return "", err
	}

	lines := strings.Split(full, "\n")

	for _, line := range lines {
		columns := strings.Fields(line)
		if columns[2] == "myself" {
			return columns[0], nil
		}
	}

	return "", fmt.Errorf("could not find myself")
}

func counterDebug(client *redis.Client, key string, id string) (float64, bool, error) {
	cmd := redis.NewCmd("DEBUG", "COUNTER", key)
	if err := client.Process(cmd); err != nil {
		return 0, false, err
	}
	val, err := cmd.Result()
	if err != nil {
		return 0, false, err
	}
	debug := val.([]interface{})
	shards := debug[0].([]interface{})
	for _, shardI := range shards {
		shard := shardI.([]interface{})
		shardID := shard[0].(string)
		if shardID == id {
			v, err := strconv.ParseFloat(shard[1].(string), 64)
			return v, true, err
		}
	}

	return 0, false, nil
}

func main() {
	fromAddr := flag.String("from", "", "Move counters from this discnt instance")
	toAddr := flag.String("to", "", "Move counters to this discnt instance")
	connections := flag.Int("n", 32, "How many connections to use")
	flag.Parse()

	from := redis.NewClient(&redis.Options{
		Addr:       *fromAddr,
		MaxRetries: 2,
		PoolSize:   *connections,
	})

	to := redis.NewClient(&redis.Options{
		Addr:       *toAddr,
		MaxRetries: 2,
		PoolSize:   *connections,
	})

	fromID, err := getNodeID(from)
	if err != nil {
		panic(err)
	}

	for {
		allKeys, err := from.Keys("*").Result()
		if err != nil {
			panic(err)
		}

		log.Printf("[DEBUG] %d keys", len(allKeys))

		keys := make(map[string]struct{})
		var mu sync.Mutex
		var done sync.WaitGroup
		limit := make(chan struct{}, *connections)

		for _, key := range allKeys {
			limit <- struct{}{}
			done.Add(1)
			go func(key string) {
				_, exists, err := counterDebug(from, key, fromID)
				if err != nil {
					log.Printf("[ERR] %v", err)
				}

				if exists {
					mu.Lock()
					keys[key] = struct{}{}
					mu.Unlock()
				}
				<-limit
				done.Done()
			}(key)
		}
		done.Wait()

		if len(keys) == 0 {
			break
		}

		log.Printf("[INFO] %d counters to move", len(keys))

		for x := 0; x < 60; x++ {
			start := time.Now()

			del := make([]string, 0)
			stop := false

			for key, _ := range keys {
				limit <- struct{}{}
				mu.Lock()
				if stop {
					mu.Unlock()
					break
				}
				mu.Unlock()
				done.Add(1)
				go func(key string) {
					defer func() {
						<-limit
						done.Done()
					}()

					v, exists, err := counterDebug(from, key, fromID)
					if err != nil {
						log.Printf("[ERR] %v", err)
						return
					}

					if !exists {
						mu.Lock()
						del = append(del, key)
						mu.Unlock()
					} else {
						if v < 0.000001 && v > -0.000001 {
							log.Printf("[DEBUG] %s %.17g reset", key, v)
							cmd := redis.NewCmd("DEBUG", "RESETSHARD", key, fromID)
							if err := from.Process(cmd); err != nil {
								log.Printf("[ERR] %v", err)
								mu.Lock()
								stop = true
								mu.Unlock()
								return
							}
							_, err := cmd.Result()
							if err != nil {
								log.Printf("[ERR] %v", err)
								mu.Lock()
								stop = true
								mu.Unlock()
								return
							}
							mu.Lock()
							del = append(del, key)
							mu.Unlock()
							return
						}

						log.Printf("[DEBUG] %s %.17g", key, v)

						if err := to.IncrByFloat(key, v).Err(); err != nil {
							log.Printf("[ERR] failed to increment %s by %f on to: %v", key, v, err)
							mu.Lock()
							stop = true
							mu.Unlock()
							return
						}

						time.Sleep(time.Second * 10)

						if err := from.IncrByFloat(key, -v).Err(); err != nil {
							log.Printf("[ERR] failed to increment %s by %f on from: %v", key, -v, err)
							if err := to.IncrByFloat(key, -v).Err(); err != nil {
								log.Printf("[ERR] failed to increment %s by %f on to, DO THIS MANUALLY: %v", key, -v, err)
							}
							mu.Lock()
							stop = true
							mu.Unlock()
							return
						}
					}
				}(key)
			}
			done.Wait()

			log.Printf("[DEBUG] %v", time.Since(start))

			if stop {
				return
			}

			for _, key := range del {
				delete(keys, key)
			}

			if len(keys) == 0 {
				break
			}

			if d := time.Since(start); d < time.Second {
				time.Sleep(time.Second - d)
			}
		}
	}
}
