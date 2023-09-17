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
	"github.com/mcuadros/go-defaults"
	"os"
	"testing"

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
	// 验证恢复是否完全一致
	expected := map[string]string{
		//"kl":    "kl",
		"key0":  "2022-03-29 17:25:54.592593",
		"key1":  "2022-03-29 17:25:54.876326",
		"key2":  "2022-03-29 17:25:52.871918",
		"key3":  "2022-03-29 17:25:53.034060",
		"key4":  "2022-03-29 17:25:53.196913",
		"key5":  "2022-03-29 17:25:53.356234",
		"key6":  "2022-03-29 17:25:53.513544",
		"key7":  "2022-03-29 17:25:53.671556",
		"key8":  "2022-03-29 17:25:53.861237",
		"key9":  "2022-03-29 17:25:54.020518",
		"key10": "2022-03-29 17:25:54.177881",
		"key11": "2022-03-29 17:25:54.337640",
	}

	for key, value := range expected {
		fmt.Printf("key:%v", key)
		result, err := client.Get(key).Result()
		if err != nil {
			t.Fatalf("Failed to read key %s from Redis: %v", key, err)
		}

		if result != value {
			t.Errorf("Value for key %s is incorrect. Expected: %s, Got: %s", key, value, result)
		}
	}
	result, err := client.DbSize().Result()
	fmt.Printf("result number:%v\n", result)
	if err != nil {
		t.Fatalf("Failed DBSize %v", err)
	}
	if result != int64(len(expected)) {
		t.Fatalf("the number not equal")
	}

}
