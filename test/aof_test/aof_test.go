package aof_test

import (
	"RedisShake/internal/config"
	"RedisShake/internal/function"
	"RedisShake/internal/log"
	"RedisShake/internal/reader"
	"RedisShake/internal/status"
	"RedisShake/internal/utils"
	"RedisShake/internal/writer"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/mcuadros/go-defaults"

	"github.com/go-redis/redis"
)

const (
	AOFRestorePath    = "/test_aof_restore.toml"
	AppendOnlyAoFPath = "/appendonlydir/appendonly.aof.manifest"
)

func AOFMain(configPath, aofFilePath string) {
	os.Args = []string{aofFilePath, configPath}
	v := config.LoadConfig()

	log.Init(config.Opt.Advanced.LogLevel, config.Opt.Advanced.LogFile, config.Opt.Advanced.Dir)
	utils.ChdirAndAcquireFileLock()
	utils.SetNcpu()
	utils.SetPprofPort()
	function.Init()

	// create reader
	var theReader reader.Reader
	// set filepath
	opts := &reader.AOFReaderOptions{
		Filepath:     aofFilePath,
		AOFTimestamp: 0,
	}
	theReader = reader.NewAOFReader(opts)
	log.Infof("create AOFReader: %v", opts.Filepath)

	// create writer
	var theWriter writer.Writer
	if v.IsSet("redis_writer") {
		opts := new(writer.RedisWriterOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("redis_writer", opts)
		if err != nil {
			log.Panicf("failed to read the RedisStandaloneWriter config entry. err: %v", err)
		}
		if opts.Cluster {
			theWriter = writer.NewRedisClusterWriter(opts)
			log.Infof("create RedisClusterWriter: %v", opts.Address)
		} else {
			theWriter = writer.NewRedisStandaloneWriter(opts)
			log.Infof("create RedisStandaloneWriter: %v", opts.Address)
		}
	} else {
		log.Panicf("no writer config entry found")
	}

	// create status
	status.Init(theReader, theWriter)

	log.Infof("start syncing...")

	ch := theReader.StartRead()
	for e := range ch {
		// calc arguments
		e.Parse()
		status.AddReadCount(e.CmdName)

		// filter
		log.Debugf("function before: %v", e)
		entries := function.RunFunction(e)
		log.Debugf("function after: %v", entries)

		for _, entry := range entries {
			entry.Parse()
			theWriter.Write(entry)
			status.AddWriteCount(entry.CmdName)
		}
	}

	theWriter.Close()       // Wait for all writing operations to complete
	utils.ReleaseFileLock() // Release file lock
	log.Infof("all done")
}

// if you use this test you need start redis in port 6379s
func TestMainFunction(t *testing.T) {

	wdPath, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	configPath := wdPath + AOFRestorePath
	aofFilePath := wdPath + AppendOnlyAoFPath
	fmt.Printf("configPath:%v, aofFilepath:%v\n", configPath, aofFilePath)
	AOFMain(configPath, aofFilePath)
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	pong, err := client.Ping().Result()
	if err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}
	fmt.Println("Connected to Redis:", pong)

	for i := 1; i <= 10000; i++ {
		key := strconv.Itoa(i)
		value, err := client.Get(key).Result()
		if err != nil {
			fmt.Printf("Failed to read key %v from Redis: %v\n", key, err)
			return
		}
		if value != key {
			fmt.Printf("Mismatch: expected %v but got %v for key %v\n", key, value, key)
		}
	}

	for i := 1; i <= 10000; i++ {
		key := strconv.Itoa(i)
		value, err := client.HGet("mymap", key).Result()
		if err != nil {
			fmt.Printf("Failed to read field %v from hash: %v\n", key, err)
			return
		}
		if value != key {
			fmt.Printf("Mismatch: expected %v but got %v for field %v in hash\n", key, value, key)
		}
	}

	for i := 0; i < 10000; i++ {
		value, err := client.LIndex("mylist", int64(i)).Result()
		if err != nil {
			fmt.Printf("Failed to read index %v from list: %v\n", i, err)
			return
		}
		expected := strconv.Itoa(i + 1)
		if value != expected {
			fmt.Printf("Mismatch: expected %v but got %v at index %v in list\n", expected, value, i)
		}
	}

	for i := 1; i <= 10000; i++ {
		expected := strconv.Itoa(i)
		score, err := client.ZScore("myzset", expected).Result()
		if err != nil {
			fmt.Printf("Failed to read member %v from sorted set: %v\n", expected, err)
			return
		}
		if score != float64(i) {
			fmt.Printf("Mismatch: expected rank %v but got %v for member %v in sorted set\n", i-1, score, expected)
		}
	}

	messages, err := client.XRead(&redis.XReadArgs{
		Streams: []string{"mystream", "0"},
		Block:   0,
		Count:   10000,
	}).Result()

	if err != nil {
		fmt.Println("Failed to read from stream:", err)
		return
	}

	for i, message := range messages[0].Messages {
		expected := strconv.Itoa(i + 1)
		value, _ := message.Values["value"].(string)
		if value != expected {
			fmt.Printf("Mismatch: expected %v but got %v in stream\n", expected, value)
		}
	}

}
