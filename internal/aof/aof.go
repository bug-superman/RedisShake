package aof

import (
	"bufio"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"RedisShake/internal/entry"
	"RedisShake/internal/log"
)

const (
	COK          = 1
	CERR         = -1
	AOFNotExist  = 1
	AOFOpenErr   = 3
	AOFOK        = 0
	AOFEmpty     = 2
	AOFFailed    = 4
	AOFTruncated = 5
	SizeMax      = 128
)

var DangerousCommands = map[string]bool{
	"flushdb":        true,
	"acl":            true,
	"slowlog":        true,
	"debug":          true,
	"role":           true,
	"keys":           true,
	"pfselftest":     true,
	"client":         true,
	"bgrewriteaof":   true,
	"replicaof":      true,
	"monitor":        true,
	"restore-asking": true,
	"latency":        true,
	"replconf":       true,
	"pfdebug":        true,
	"bgsave":         true,
	"sync":           true,
	"config":         true,
	"flushall":       true,
	"cluster":        true,
	"info":           true,
	"lastsave":       true,
	"slaveof":        true,
	"swapdb":         true,
	"module":         true,
	"restore":        true,
	"migrate":        true,
	"save":           true,
	"shutdown":       true,
	"psync":          true,
	"sort":           true,
}

type Loader struct {
	filPath string
	ch      chan *entry.Entry
}

func NewLoader(filPath string, ch chan *entry.Entry) *Loader {
	ld := new(Loader)
	ld.ch = ch
	ld.filPath = filPath
	return ld
}

func MakePath(Paths string, FileName string) string {
	return path.Join(Paths, FileName)
}

func ReadCompleteLine(reader *bufio.Reader) ([]byte, error) {
	line, isPrefix, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}

	for isPrefix {
		var additional []byte
		additional, isPrefix, err = reader.ReadLine()
		if err != nil {
			return nil, err
		}
		line = append(line, additional...)
	}

	return line, err
}

func LoadSingleAppendOnlyFile(AOFDirName string, FileName string, ch chan *entry.Entry, LastFile bool, AOFTimeStamp int64, FilterDangerousCommands string) int {
	ret := AOFOK
	AOFFilepath := MakePath(AOFDirName, FileName)
	fp, err := os.Open(AOFFilepath)
	if err != nil {
		if os.IsNotExist(err) {
			if _, err := os.Stat(AOFFilepath); err == nil || !os.IsNotExist(err) {
				log.Infof("Fatal error: can't open the append log File %v for reading: %v", FileName, err.Error())
				return AOFOpenErr
			} else {
				log.Infof("The append log File %v doesn't exist: %v", FileName, err.Error())
				return AOFNotExist
			}

		}
		defer fp.Close()

		stat, _ := fp.Stat()
		if stat.Size() == 0 {
			return AOFEmpty
		}
	}
	reader := bufio.NewReader(fp)
	for {

		line, err := ReadCompleteLine(reader)
		{
			if err != nil {
				if err == io.EOF {
					break
				} else {
					log.Infof("Unrecoverable error reading the append only File %v: %v", FileName, err)
					ret = AOFFailed
					return ret
				}
			} else {
				_, errs := fp.Seek(0, io.SeekCurrent)
				if errs != nil {
					log.Infof("Unrecoverable error reading the append only File %v: %v", FileName, errs)
					ret = AOFFailed
					return ret
				}
			}

			if line[0] == '#' {
				if AOFTimeStamp != 0 && strings.HasPrefix(string(line), "#TS:") {
					var ts int64
					ts, err = strconv.ParseInt(strings.TrimPrefix(string(line[1:]), "#TS:"), 10, 64)
					if err != nil {
						log.Panicf("Invalid timestamp annotation")
					}

					if ts > AOFTimeStamp && LastFile {
						ret = AOFTruncated
						log.Infof("AOFTruncated%s", line)
						return ret
					}
				}
				continue
			}
			if line[0] != '*' {
				log.Panicf("Bad File format reading the append only File %v:make a backup of your AOF File, then use ./redis-check-AOF --fix <FileName.manifest>", FileName)
			}
			argc, _ := strconv.ParseInt(string(line[1:]), 10, 64)
			if argc < 1 {
				log.Panicf("Bad File format reading the append only File %v:make a backup of your AOF File, then use ./redis-check-AOF --fix <FileName.manifest>", FileName)
			}
			if argc > int64(SizeMax) {
				log.Panicf("Bad File format reading the append only File %v:make a backup of your AOF File, then use ./redis-check-AOF --fix <FileName.manifest>", FileName)
			}
			e := entry.NewEntry()
			argv := []string{}

			for j := 0; j < int(argc); j++ {
				//line, err := reader.ReadString('\n')
				line, err := ReadCompleteLine(reader)
				if err != nil || line[0] != '$' {
					log.Infof("Bad File format reading the append only File %v:make a backup of your AOF File, then use ./redis-check-AOF --fix <FileName.manifest>", FileName)
					ret = AOFFailed
					return ret
				}
				len, _ := strconv.ParseInt(string(line[1:]), 10, 64)
				argstring := make([]byte, len+2)
				argstring, err = ReadCompleteLine(reader)
				if err != nil {
					log.Infof("Unrecoverable error reading the append only File %v: %v", FileName, err)
					ret = AOFFailed
					return ret
				}
				argstring = argstring[:len]
				argv = append(argv, string(argstring))
			}
			if DangerousCommands[argv[0]] && FilterDangerousCommands == "yes" {
				log.Infof("Skip dangerous commands:%s", argv[0])
				continue
			}
			for _, value := range argv {
				e.Argv = append(e.Argv, value)
			}
			ch <- e

		}

	}
	return ret
}
