package reader

import (
	"RedisShake/internal/aof"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"bytes"

	"os"
	"path"

	"bufio"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	AOFManifestFileTypeBase = "b" /* Base File */
	AOFManifestTypeHist     = "h" /* History File */
	AOFManifestTypeIncr     = "i" /* INCR File */
	RDBFormatSuffix         = ".RDB"
	AOFFormatSuffix         = ".AOF"
	BaseFileSuffix          = ".Base"
	IncrFileSuffix          = ".incr"
	TempFileNamePrefix      = "temp-"
	COK                     = 1
	CERR                    = -1
	EINTR                   = 4
	ManifestNameSuffix      = ".manifest"
	AOFNotExist             = 1
	AOFOpenErr              = 3
	AOFOK                   = 0
	AOFEmpty                = 2
	AOFFailed               = 4
	AOFTruncated            = 5
	SizeMax                 = 128
	RDBFlagsAOFPreamble     = 1 << 0
	AOFManifestKeyFileName  = "File"
	AOFManifestKeyFileSeq   = "seq"
	AOFManifestKeyFileType  = "type"
)

func Ustime() int64 {
	tv := time.Now()
	ust := int64(tv.UnixNano()) / 1000
	return ust

}

func StringNeedsRepr(s string) int {
	sLen := len(s)
	point := 0
	for sLen > 0 {
		if s[point] == '\\' || s[point] == '"' || s[point] == '\n' || s[point] == '\r' ||
			s[point] == '\t' || s[point] == '\a' || s[point] == '\b' || !unicode.IsPrint(rune(s[point])) || unicode.IsSpace(rune(s[point])) {
			return 1
		}
		sLen--
		point++
	}

	return 0
}

type INFO struct {
	AOFDirName         string
	AOFUseRDBPreamble  int // not support parsing rdb preamble
	AOFManifest        *AOFManifest
	AOFFileName        string
	AOFCurrentSize     int64
	AOFRewriteBaseSize int64
	updateLoadingFile  string
	ch                 chan *entry.Entry
}

func (manifestInfo *INFO) GetAOFDirName() string {
	return manifestInfo.AOFDirName
}

func NewAOFFileInfo(aofFilePath string, ch chan *entry.Entry) *INFO {
	return &INFO{
		AOFDirName:         filepath.Dir(aofFilePath),
		AOFUseRDBPreamble:  0,
		AOFManifest:        nil,
		AOFFileName:        filepath.Base(aofFilePath),
		AOFCurrentSize:     0,
		AOFRewriteBaseSize: 0,
		ch:                 ch,
	}
}

type listIter struct {
	next      *listNode
	Direction int
}

type lists struct {
	head, tail *listNode
	len        uint64
}

type listNode struct {
	prev  *listNode
	next  *listNode
	value interface{}
}

func ListCreate() *lists {
	lists := &lists{}
	lists.head = nil
	lists.tail = nil
	lists.len = 0
	return lists
}
func ListNext(iter *listIter) *listNode {
	Current := iter.next

	if Current != nil {
		if iter.Direction == 0 {
			iter.next = Current.next
		} else {
			iter.next = Current.prev
		}
	}
	return Current
}

func (list *lists) ListsRewind(li *listIter) {
	li.next = list.head
	li.Direction = 0
}

func ListLinkNodeTail(lists *lists, node *listNode) {
	if lists.len == 0 {
		lists.head = node
		lists.tail = node
		node.prev = nil
		node.next = nil
	} else {
		node.prev = lists.tail
		node.next = nil
		lists.tail.next = node
		lists.tail = node
	}
	lists.len++
}

func ListAddNodeTail(lists *lists, value interface{}) *lists {
	node := &listNode{
		value: value,
		prev:  nil,
		next:  nil,
	}
	ListLinkNodeTail(lists, node)
	return lists
}

func ListsRewindTail(list *lists, li *listIter) {
	li.next = list.tail
	li.Direction = 1
}

func ListDup(orig *lists) *lists {
	var copyDup *lists
	var iter listIter
	var node *listNode
	copyDup = ListCreate()
	if copyDup == nil {
		return nil
	}
	copyDup.ListsRewind(&iter)
	node = ListNext(&iter)
	var value interface{}
	for node != nil {
		value = node.value
	}

	if ListAddNodeTail(copyDup, value) == nil {
		return nil
	}
	return copyDup
}

func ListIndex(list *lists, index int64) *listNode {
	var n *listNode

	if index < 0 {
		index = (-index) - 1
		n = list.tail
		for ; index > 0 && n != nil; index-- {
			n = n.prev
		}
	} else {
		n = list.head
		for ; index > 0 && n != nil; index-- {
			n = n.next
		}
	}
	return n
}

func ListLinkNodeHead(list *lists, node *listNode) {
	if list.len == 0 {
		list.head = node
		list.tail = node
		node.prev = nil
		node.next = nil
	} else {
		node.prev = nil
		node.next = list.head
		list.head.prev = node
		list.head = node
	}
	list.len++
}

func ListAddNodeHead(list *lists, value interface{}) *lists {
	node := &listNode{
		value: value,
	}
	ListLinkNodeHead(list, node)

	return list
}

func ListUnlinkNode(list *lists, node *listNode) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		list.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		list.tail = node.prev
	}
	node.next = nil
	node.prev = nil

	list.len--
}
func ListDelNode(list *lists, node *listNode) {
	ListUnlinkNode(list, node)
}

func (a *AOFInfo) GetAOFInfoName() string {
	return a.FileName
}

func DirExists(dName string) int {
	_, err := os.Stat(dName)
	if err != nil {
		return 0
	}

	return 1
}

func FileExist(FileName string) int {
	_, err := os.Stat(FileName)
	if err != nil {
		return 0
	}

	return 1
}

func IsHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
		(c >= 'A' && c <= 'F')
}

func HexDigitToInt(c byte) int {
	switch c {
	case '0':
		return 0
	case '1':
		return 1
	case '2':
		return 2
	case '3':
		return 3
	case '4':
		return 4
	case '5':
		return 5
	case '6':
		return 6
	case '7':
		return 7
	case '8':
		return 8
	case '9':
		return 9
	case 'a', 'A':
		return 10
	case 'b', 'B':
		return 11
	case 'c', 'C':
		return 12
	case 'd', 'D':
		return 13
	case 'e', 'E':
		return 14
	case 'f', 'F':
		return 15
	default:
		return 0
	}
}

func SplitArgs(line string) ([]string, int) {
	var p string = line
	var Current string
	var vector []string
	argc := 0
	i := 0
	lens := len(p)
	for { //SKIP BLANKS
		for i < lens && unicode.IsSpace(rune(p[i])) {
			i++
		}
		if i < lens {
			inq := false  // Set to true if we are in "quotes"
			insq := false // Set to true if we are in 'single quotes'
			done := false

			for !done {
				if inq {

					if p[i] == '\\' && (p[i+1]) == 'x' && IsHexDigit(p[i+2]) && IsHexDigit(p[i+3]) {
						_, err1 := strconv.ParseInt(string(p[i+2]), 16, 64)
						_, err2 := strconv.ParseInt(string(p[i+3]), 16, 64)
						if err1 == nil && err2 == nil {
							hexadecimal := (HexDigitToInt((p[i+2])) * 16) + HexDigitToInt(p[i+3])
							Current = Current + fmt.Sprint(hexadecimal)
							i += 3
						}

					} else if p[i] == '\\' && i+1 < lens {
						var c byte
						i++
						switch p[i] {
						case 'n':
							c = '\n'
						case 'r':
							c = 'r'
						case 'a':
							c = '\a'
						default:
							c = p[i]
						}
						Current += string(c)
					} else if p[i] == '"' {
						if i+1 < lens && !unicode.IsSpace(rune(p[i+1])) {
							return nil, 0
						}
						done = true
					} else if i >= lens {
						return nil, 0
					} else {
						Current += string(p[i])
					}
				} else if insq {
					if p[i] == '\\' && p[i+1] == '\'' {
						i++
						Current += "'"
					} else if p[i] == '\'' {
						if i+1 < lens && !unicode.IsSpace(rune(p[i+1])) {
							return nil, 0
						}
						done = true
					} else if i >= lens {
						return nil, 0
					} else {
						Current += string(p[i])
					}

				} else {
					switch p[i] {
					case ' ', '\n', '\r', '\t', '\000':
						done = true
					case '"':
						inq = true
					case '\'':
						insq = true
					default:
						Current += string(p[i])
					}
				}
				if i < lens {
					i++
				}
				if i == lens {
					done = true
				}
			}

			vector = append(vector, Current)
			argc++
			Current = ""

		} else {
			return vector, argc
		}

	}
}

func StringCatLen(s string, t []byte, lent int) string {
	curLen := len(s)

	if curLen == 0 {
		return ""
	}

	buf := make([]byte, curLen+lent)

	copy(buf[:curLen], s)
	copy(buf[curLen:], t)
	return string(buf)
}

func StringCatPrintf(s string, fmtStr string, args ...interface{}) string {
	result := fmt.Sprintf(fmtStr, args...)
	if s == "" {
		return result
	} else {
		return s + result
	}
}

func StringCatRepr(s string, p string, length int) string {
	s = s + ("\"")
	for i := 0; i < length; i++ {
		switch p[i] {
		case '\\', '"':
			s = StringCatPrintf(s, "\\%c", p[i])
		case '\n':
			s = s + "\\n"
		case '\r':
			s = s + "\\r"
		case '\t':
			s = s + "\\t"
		case '\a':
			s = s + "\\a"
		case '\b':
			s = s + "\\b"
		default:
			if strconv.IsPrint(rune(p[i])) {
				s = s + string(p[i])
			} else {
				s = s + "\\x%02x"
			}
		}
	}
	return s + "\""
}

func (manifestInfo *INFO) UpdateLoadingFileName(FileName string) {
	manifestInfo.updateLoadingFile = FileName
}

// AOFInfo AOF manifest definition
type AOFInfo struct {
	FileName    string
	FileSeq     int64
	AOFFileType string
}

func AOFInfoCreate() *AOFInfo {
	return new(AOFInfo)
}

var AofInfo AOFInfo = *AOFInfoCreate()

func AOFInfoDup(orig *AOFInfo) *AOFInfo {
	if orig == nil {
		log.Panicf("Assertion failed: orig != nil")
	}
	ai := AOFInfoCreate()
	ai.FileName = orig.FileName
	ai.FileSeq = orig.FileSeq
	ai.AOFFileType = orig.AOFFileType
	return ai
}

func AOFInfoFormat(buf string, ai *AOFInfo) string {
	var AOFManifestcreate string
	if StringNeedsRepr(ai.FileName) == 1 {
		AOFManifestcreate = StringCatRepr("", ai.FileName, len(ai.FileName))
	}
	var ret string
	if AOFManifestcreate != "" {
		ret = StringCatPrintf(buf, "%s %s %s %d %s %s\n", AOFManifestKeyFileName, AOFManifestcreate, AOFManifestKeyFileSeq, ai.FileSeq, AOFManifestKeyFileType, ai.AOFFileType)
	} else {
		ret = StringCatPrintf(buf, "%s %s %s %d %s %s\n", AOFManifestKeyFileName, ai.FileName, AOFManifestKeyFileSeq, ai.FileSeq, AOFManifestKeyFileType, ai.AOFFileType)
	}
	return ret
}

func PathIsBaseName(Path string) bool {
	return strings.IndexByte(Path, '/') == -1 && strings.IndexByte(Path, '\\') == -1
}

func AOFLoadManifestFromFile(amFilepath string) *AOFManifest {
	var maxseq int64
	am := AOFManifestcreate()
	fp, err := os.Open(amFilepath)
	if err != nil {
		log.Panicf("Fatal error:can't open the AOF manifest %v for reading: %v", amFilepath, err)
	}
	defer fp.Close()
	var argv []string
	var ai *AOFInfo
	var line string
	linenum := 0
	reader := bufio.NewReader(fp)
	for {
		buf, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				if linenum == 0 {
					log.Infof("Found an empty AOF manifest")
					am = nil
					return am
				} else {
					break
				}

			} else {
				log.Infof("Read AOF manifest failed")
				am = nil
				return am

			}
		}

		linenum++
		if buf[0] == '#' {
			continue
		}
		if !strings.Contains(buf, "\n") {
			log.Infof("The AOF manifest File contains too long line")
			return nil
		}
		line = strings.Trim(buf, " \t\r\n")
		if len(line) == 0 {
			log.Infof("Invalid AOF manifest File format")
			return nil
		}
		argc := 0
		argv, argc = SplitArgs(line)

		if argc < 6 || argc%2 != 0 {
			log.Infof("Invalid AOF manifest File format")
			am = nil
			return am
		}
		ai = AOFInfoCreate()
		for i := 0; i < argc; i += 2 {
			if strings.EqualFold(argv[i], AOFManifestKeyFileName) {
				ai.FileName = argv[i+1]
				if !PathIsBaseName(ai.FileName) {
					log.Panicf("File can't be a path, just a Filename")
				}
			} else if strings.EqualFold(argv[i], AOFManifestKeyFileSeq) {
				ai.FileSeq, _ = strconv.ParseInt(argv[i+1], 10, 64)
			} else if strings.EqualFold(argv[i], AOFManifestKeyFileType) {
				ai.AOFFileType = string(argv[i+1][0])
			}
		}
		if ai.FileName == "" || ai.FileSeq == 0 || ai.AOFFileType == "" {
			log.Panicf("Invalid AOF manifest File format")
		}
		if ai.AOFFileType == AOFManifestFileTypeBase {
			if am.BaseAOFInfo != nil {
				log.Panicf("Found duplicate Base File information")
			}
			am.BaseAOFInfo = ai
			am.CurrBaseFileSeq = ai.FileSeq
		} else if ai.AOFFileType == AOFManifestTypeHist {
			am.HistoryList = ListAddNodeTail(am.HistoryList, ai)
		} else if ai.AOFFileType == AOFManifestTypeIncr {
			if ai.FileSeq <= maxseq {
				log.Panicf("Found a non-monotonic sequence number")
			}
			am.incrAOFList = ListAddNodeTail(am.HistoryList, ai)
			am.CurrIncrFileSeq = ai.FileSeq
			maxseq = ai.FileSeq
		} else {
			log.Panicf("Unknown AOF File type")
		}
		line = " "
		ai = nil
	}
	return am
}

type AOFManifest struct {
	BaseAOFInfo     *AOFInfo
	incrAOFList     *lists
	HistoryList     *lists
	CurrBaseFileSeq int64
	CurrIncrFileSeq int64
	Dirty           int64
}

func AOFManifestcreate() *AOFManifest {
	am := &AOFManifest{
		incrAOFList: ListCreate(),
		HistoryList: ListCreate(),
	}
	return am
}

func AOFManifestDup(orig *AOFManifest) *AOFManifest {
	if orig == nil {
		panic("orig is nil")
	}

	am := &AOFManifest{
		CurrBaseFileSeq: orig.CurrBaseFileSeq,
		CurrIncrFileSeq: orig.CurrIncrFileSeq,
		Dirty:           orig.Dirty,
	}

	if orig.BaseAOFInfo != nil {
		am.BaseAOFInfo = AOFInfoDup(orig.BaseAOFInfo)
	}

	am.incrAOFList = ListDup(orig.incrAOFList)
	am.HistoryList = ListDup(orig.HistoryList)

	if am.incrAOFList == nil || am.HistoryList == nil {
		log.Panicf("IncrAOFlist or HistoryAOFlist is nil")
	}
	return am
}

func GetAOFManifestAsString(am *AOFManifest) string {
	if am == nil {
		panic("am is nil")
	}
	var buf string
	var ln *listNode
	var li listIter

	if am.BaseAOFInfo != nil {
		buf = AOFInfoFormat(buf, am.BaseAOFInfo)
	}
	am.HistoryList.ListsRewind(&li)
	ln = ListNext(&li)
	for ln != nil {
		ai, ok := ln.value.(*AOFInfo)
		if ok {
			buf = AOFInfoFormat(buf, ai)
		}
		ln = ListNext(&li)
	}

	am.incrAOFList.ListsRewind(&li)
	ln = ListNext(&li)
	for ln != nil {
		ai, ok := ln.value.(*AOFInfo)
		if ok {
			buf = AOFInfoFormat(buf, ai)
		}
		ln = ListNext(&li)
	}

	return buf

}

func (manifestInfo *INFO) GetNewBaseFileNameAndMarkPreAsHistory(am *AOFManifest) string {
	if am == nil {
		log.Panicf("AOFManifest is nil")
	}
	if am.BaseAOFInfo != nil {
		if am.BaseAOFInfo.AOFFileType != AOFManifestFileTypeBase {
			log.Panicf("Base_AOF_info has invalid File_type")
		}
		am.BaseAOFInfo.AOFFileType = AOFManifestTypeHist
	}
	var formatSuffix string
	if manifestInfo.AOFUseRDBPreamble == 1 {
		formatSuffix = RDBFormatSuffix
	} else {
		formatSuffix = AOFFormatSuffix
	}
	ai := AOFInfoCreate()
	ai.FileName = StringCatPrintf("%s.%d%s%d", AofInfo.GetAOFInfoName(), am.CurrBaseFileSeq+1, BaseFileSuffix, formatSuffix)
	ai.FileSeq = am.CurrBaseFileSeq + 1
	ai.AOFFileType = AOFManifestFileTypeBase
	am.BaseAOFInfo = ai
	am.Dirty = 1
	return am.BaseAOFInfo.FileName
}

func (manifestInfo *INFO) AOFLoadManifestFromDisk() {
	manifestInfo.AOFManifest = AOFManifestcreate()
	if DirExists(manifestInfo.AOFDirName) == 0 {
		log.Infof("The AOF Directory %v doesn't exist\n", manifestInfo.AOFDirName)
		return
	}

	amName := manifestInfo.GetAOFManifestFileName()
	amFilepath := path.Join(manifestInfo.AOFDirName, amName)
	if FileExist(amFilepath) == 0 {
		log.Infof("The AOF Directory %v doesn't exist\n", manifestInfo.AOFDirName)
		return
	}

	am := AOFLoadManifestFromFile(amFilepath)
	//if am != nil {
	manifestInfo.AOFManifest = am
	//}
}

func (manifestInfo *INFO) GetNewIncrAOFName(am *AOFManifest) string {
	ai := AOFInfoCreate()
	ai.AOFFileType = AOFManifestTypeIncr
	ai.FileName = StringCatPrintf("", "%s.%d%s%s", manifestInfo.AOFFileName, am.CurrIncrFileSeq+1, IncrFileSuffix, AOFFormatSuffix)
	ai.FileSeq = am.CurrIncrFileSeq + 1
	ListAddNodeTail(am.incrAOFList, ai)
	am.Dirty = 1
	return ai.FileName
}

func (manifestInfo *INFO) GetTempIncrAOFName() string {
	return StringCatPrintf("", "%s%s%s", TempFileNamePrefix, manifestInfo.AOFFileName, IncrFileSuffix)
}

func (manifestInfo *INFO) GetLastIncrAOFName(am *AOFManifest) string {
	if am == nil {
		log.Panicf(("AOFManifest is nil"))
	}

	if am.incrAOFList.len == 0 {
		return manifestInfo.GetNewIncrAOFName(am)
	}

	lastnode := ListIndex(am.incrAOFList, -1)

	ai, ok := lastnode.value.(AOFInfo)
	if !ok {
		log.Panicf("Failed to convert last node.value to AOFInfo")
	}
	return ai.FileName
}

func (manifestInfo *INFO) GetAOFManifestFileName() string {
	return manifestInfo.AOFFileName
}

func (manifestInfo *INFO) GetTempAOFManifestFileName() string {
	return StringCatPrintf("", "%s%s", TempFileNamePrefix, manifestInfo.AOFFileName)
}

func (manifestInfo *INFO) AOFFileExist(FileName string) int {
	Filepath := path.Join(manifestInfo.AOFDirName, FileName)
	ret := FileExist(Filepath)
	return ret
}

func (manifestInfo *INFO) GetAppendOnlyFileSize(FileName string, status *int) int64 {
	var size int64

	AOFFilePath := path.Join(manifestInfo.AOFDirName, FileName)

	stat, err := os.Stat(AOFFilePath)
	if err != nil {
		if status != nil {
			if os.IsNotExist(err) {
				*status = AOFNotExist
			} else {
				*status = AOFOpenErr
			}
		}
		log.Panicf("Unable to obtain the AOF File %v length. stat: %v", FileName, err.Error())
		size = 0
	} else {
		if status != nil {
			*status = AOFOK
		}
		size = stat.Size()
	}
	return size
}

func (manifestInfo *INFO) GetBaseAndIncrAppendOnlyFilesSize(am *AOFManifest, status *int) int64 {
	var size int64
	var ln *listNode = new(listNode)
	var li *listIter = new(listIter)
	if am.BaseAOFInfo != nil {
		if am.BaseAOFInfo.AOFFileType != AOFManifestFileTypeBase {
			log.Panicf("File type must be Base.")
		}
		size += manifestInfo.GetAppendOnlyFileSize(am.BaseAOFInfo.FileName, status)
		if *status != AOFOK {
			return 0
		}
	}

	am.incrAOFList.ListsRewind(li)
	ln = ListNext(li)
	for ln != nil {
		ai := ln.value.(*AOFInfo)
		if ai.AOFFileType != AOFManifestTypeIncr {
			log.Panicf("File type must be Incr")
		}
		size += manifestInfo.GetAppendOnlyFileSize(ai.FileName, status)
		if *status != AOFOK {
			return 0
		}
		ln = ListNext(li)
	}
	return size
}

func GetBaseAndIncrAppendOnlyFilesNum(am *AOFManifest) int {
	num := 0
	if am.BaseAOFInfo != nil {
		num++
	}
	if am.incrAOFList != nil {
		num += int(am.incrAOFList.len)
	}
	return num
}

func (manifestInfo *INFO) LoadAppendOnlyFile(am *AOFManifest, ch chan *entry.Entry, AOFTimeStamp int64, FilterDangerousCommands string) int {
	if am == nil {
		log.Panicf("AOFManifest is null")
	}
	status := AOFOK
	ret := AOFOK
	var start int64
	var totalSize int64 = 0
	var BaseSize int64 = 0
	var AOFName string
	var totalNum, AOFNum, lastFile int

	if manifestInfo.AOFFileExist(manifestInfo.AOFFileName) == 1 {
		if DirExists(manifestInfo.AOFDirName) == 0 ||
			(am.BaseAOFInfo == nil && am.incrAOFList.len == 0) ||
			(am.BaseAOFInfo != nil && am.incrAOFList.len == 0 &&
				strings.Compare(am.BaseAOFInfo.FileName, manifestInfo.AOFFileName) == 0 && manifestInfo.AOFFileExist(manifestInfo.AOFFileName) == 0) {
			log.Panicf("This is an old version of the AOF File")
		}
	}

	if am.BaseAOFInfo == nil && am.incrAOFList == nil {
		return AOFNotExist
	}

	totalNum = GetBaseAndIncrAppendOnlyFilesNum(am)
	if totalNum <= 0 {
		log.Panicf("Assertion failed: IncrAppendOnlyFilestotalNum > 0")
	}

	totalSize = manifestInfo.GetBaseAndIncrAppendOnlyFilesSize(am, &status)
	if status != AOFOK {
		if status == AOFNotExist {
			status = AOFFailed
		}
		return status
	} else if totalSize == 0 {
		return AOFEmpty
	}

	log.Infof("The AOF File starts loading.\n")
	if am.BaseAOFInfo != nil {
		if am.BaseAOFInfo.AOFFileType == AOFManifestFileTypeBase {
			AOFName = am.BaseAOFInfo.FileName
			manifestInfo.UpdateLoadingFileName(AOFName)
			BaseSize = manifestInfo.GetAppendOnlyFileSize(AOFName, nil)
			lastFile = totalNum
			start = Ustime()
			if lastFile != 1 {
				ret = manifestInfo.ParsingSingleAppendOnlyFile(AOFName, false, 0)
			} else {
				ret = manifestInfo.ParsingSingleAppendOnlyFile(AOFName, true, AOFTimeStamp)
			}
			if ret == AOFOK || (ret == AOFTruncated && lastFile == 1) {
				log.Infof("DB loaded from Base File %v: %.3f seconds", AOFName, float64(Ustime()-start)/1000000)
			}

			if ret == AOFEmpty {
				ret = AOFOK
			}

			if ret == AOFTruncated && lastFile == 0 {
				ret = AOFFailed
				log.Infof("Fatal error: the truncated File is not the last File")
			}

			if ret == AOFOpenErr || ret == AOFFailed {
				if ret == AOFOK || ret == AOFTruncated {
					log.Infof("The AOF File was successfully loaded\n")
				} else {
					if ret == AOFOpenErr {
						log.Panicf("There was an error opening the AOF File.\n")
					} else {
						log.Panicf("Failed to open AOF File.\n")
					}
				}
				return ret
			}
		}
		totalNum--
	}

	if am.incrAOFList.len > 0 {
		var ln *listNode = new(listNode)
		var li listIter

		am.incrAOFList.ListsRewind(&li)
		ln = ListNext(&li)
		for ln != nil {
			ai := ln.value.(*AOFInfo)
			if ai.AOFFileType != AOFManifestTypeIncr {
				log.Panicf("The manifestType must be Incr")
			}
			AOFName = ai.FileName
			manifestInfo.UpdateLoadingFileName(AOFName)
			lastFile = totalNum
			AOFNum++
			start = Ustime()
			if lastFile == 1 {
				ret = manifestInfo.ParsingSingleAppendOnlyFile(AOFName, true, AOFTimeStamp)
			} else {
				ret = manifestInfo.ParsingSingleAppendOnlyFile(AOFName, false, 0)
			}
			if ret == AOFOK || (ret == AOFTruncated && lastFile == 1) {
				log.Infof("DB loaded from incr File %v: %.3f seconds", AOFName, float64(Ustime()-start)/1000000)
			}

			if ret == AOFEmpty {
				ret = AOFOK
			}

			if ret == AOFTruncated && lastFile == 0 {
				ret = AOFFailed
				log.Infof("Fatal error: the truncated File is not the last File\n")
			}

			if ret == AOFOpenErr || ret == AOFFailed {
				if ret == AOFOpenErr {
					log.Infof("There was an error opening the AOF File.\n")
				} else {
					log.Infof("Failed to open AOF File.\n")
				}
				return ret
			}
			ln = ListNext(&li)
		}
		totalNum--
	}

	manifestInfo.AOFCurrentSize = totalSize
	manifestInfo.AOFRewriteBaseSize = BaseSize
	return ret

}

func (manifestInfo *INFO) ParsingSingleAppendOnlyFile(FileName string, LastFile bool, AOFTimeStamp int64) int {
	ret := AOFOK
	AOFFilepath := path.Join(manifestInfo.AOFDirName, FileName)
	println(AOFFilepath)
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

		stat, _ := fp.Stat()
		if stat.Size() == 0 {
			return AOFEmpty
		}
	}
	defer fp.Close()
	sig := make([]byte, 5)
	if n, err := fp.Read(sig); err != nil || n != 5 || !bytes.Equal(sig, []byte("REDIS")) {
		if _, err := fp.Seek(0, 0); err != nil {
			log.Infof("Unrecoverable error reading the append only File %v: %v", FileName, err)
			ret = AOFFailed
			return ret
		}
	} else { //Skipped RDB checksum and has not been processed yet.
		log.Infof("Reading RDB Base File on AOF loading...")
		rdbOpt := RdbReaderOptions{Filepath: AOFFilepath}
		ldRDB := NewRDBReader(&rdbOpt)
		ldRDB.StartRead()
		return AOFOK
	}
	// load single aof file
	aofSingleReader := aof.NewLoader(MakePath(manifestInfo.AOFDirName, FileName), manifestInfo.ch)
	ret = aofSingleReader.LoadSingleAppendOnlyFile(LastFile, AOFTimeStamp)
	return ret

}

func MakePath(Paths string, FileName string) string {
	return path.Join(Paths, FileName)
}
