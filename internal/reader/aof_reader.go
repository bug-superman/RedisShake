package reader

import (
	"os"
	"path"
	"path/filepath"

	"github.com/alibaba/RedisShake/internal/aof"
	"github.com/alibaba/RedisShake/internal/entry"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/statistics"
)

type aofReader struct {
	path string
	ch   chan *entry.Entry
}

func NewAOFReader(path string) Reader {
	log.Infof("NewAOFReader: path=[%s]", path)
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		log.Panicf("NewAOFReader: filepath.Abs error: %s", err.Error())
	}
	log.Infof("NewAOFReader: absolute path=[%s]", absolutePath)
	r := &aofReader{
		path: absolutePath,
		ch:   make(chan *entry.Entry),
	}
	return r
}

func (r *aofReader) StartRead() chan *entry.Entry {
	r.ch = make(chan *entry.Entry, 1024)

	go func() {
		aof.AOFFileInfo = *(aof.NewAOFFileInfo())
		aof.AOFLoadManifestFromDisk()
		am := aof.AOFFileInfo.GetAOFManifest()

		if am == nil {
			paths := path.Join(aof.AOFFileInfo.GetAOFDirName(), aof.AOFFileInfo.GetAOFFileName())
			aof.CheckAOFMain(paths)
			log.Infof("start send AOF。path=[%s]", r.path)
			fi, err := os.Stat(r.path)
			if err != nil {
				log.Panicf("NewAOFReader: os.Stat error：%s", err.Error())
			}
			statistics.Metrics.AofFileSize = uint64(fi.Size())
			statistics.Metrics.AofReceivedSize = uint64(fi.Size())
			aofLoader := aof.NewLoader(r.path, r.ch)

			_ = aofLoader.LoadSingleAppendOnlyFile(paths, r.ch)
			log.Infof("Send AOF finished. path=[%s]", r.path)
			close(r.ch)
		} else {
			paths := path.Join(aof.AOFFileInfo.GetAOFDirName(), aof.GetAOFManifestFileName())
			aof.CheckAOFMain(paths)
			log.Infof("start send AOF。path=[%s]", r.path)
			fi, err := os.Stat(r.path)
			if err != nil {
				log.Panicf("NewAOFReader: os.Stat error：%s", err.Error())
			}
			statistics.Metrics.AofFileSize = uint64(fi.Size())
			statistics.Metrics.AofReceivedSize = uint64(fi.Size())
			aofLoader := aof.NewLoader(r.path, r.ch)
			_ = aofLoader.LoadAppendOnlyFile(aof.AOFFileInfo.GetAOFManifest(), r.ch)
			log.Infof("Send AOF finished. path=[%s]", r.path)
			close(r.ch)
		}
	}()

	return r.ch
}
