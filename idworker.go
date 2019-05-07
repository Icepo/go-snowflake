package go_snowflake

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

const (
	BitLenTime     = 42 // bit length of time
	BitLenSequence = 13 // bit length of sequence number
	BitLenWorkerID = 8  // bit length of machine id

	timeStampUnit = 1e7 // nsec, i.e. 10 msec
)

var (
	defaultStartTime     = time.Date(2019, 5, 6, 0, 0, 0, 0, time.UTC)
	ErrStartTimeAfterNow = errors.New("startTime after now")
	ErrOverTime          = errors.New("over the time limit")
)

type IdWorker struct {
	mutex       *sync.Mutex
	sequence    uint16 //序列号 0-8129
	startTime   int64  //基准时间戳
	workerId    uint8
	elapsedTime int64 //流逝的时间
}

type Settings struct {
	StartTime time.Time //基准时间
	WorkerId  uint8     //机器号0-255
}

func NewIdWorker(st Settings) (*IdWorker, error) {
	iw := new(IdWorker)
	iw.mutex = new(sync.Mutex)
	iw.sequence = uint16(1<<BitLenSequence - 1)
	if st.StartTime.After(time.Now()) {
		return nil, ErrStartTimeAfterNow
	}
	if st.StartTime.IsZero() {
		iw.startTime = toTimeStamp(defaultStartTime)
	} else {
		iw.startTime = toTimeStamp(st.StartTime)
	}
	iw.workerId = st.WorkerId
	return iw, nil
}

func (i *IdWorker) NextStr() (string, error) {
	id, err := i.NextInt()
	if err != nil {
		return "", err
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(id))
	return base64.StdEncoding.EncodeToString(buf), err
}

func (i *IdWorker) NextInt() (int64, error) {
	const maskSequence = uint16(1<<BitLenSequence - 1)
	i.mutex.Lock()
	defer i.mutex.Unlock()
	current := toTimeStamp(time.Now()) - i.startTime
	if i.elapsedTime < current {
		i.elapsedTime = current
		i.sequence = 0
	} else { // sf.elapsedTime >= current
		i.sequence = (i.sequence + 1) & maskSequence
		if i.sequence == 0 {
			i.elapsedTime++
			time.Sleep(sleepTime(i.elapsedTime - current))
		}
	}

	if i.elapsedTime >= 1<<BitLenTime {
		return 0, ErrOverTime
	}

	return int64(i.elapsedTime)<<(BitLenSequence+BitLenWorkerID) |
		int64(i.sequence)<<BitLenWorkerID |
		int64(i.workerId), nil
}

func sleepTime(overtime int64) time.Duration {
	return time.Duration(overtime)*10*time.Millisecond -
		time.Duration(time.Now().UTC().UnixNano()%timeStampUnit)*time.Nanosecond
}

func toTimeStamp(t time.Time) int64 {
	return t.UTC().UnixNano() / timeStampUnit
}

// Decompose returns a set of Sonyflake ID parts.
func Decompose(id int64) map[string]int64 {
	const maskSequence = int64((1<<BitLenSequence - 1) << BitLenWorkerID)
	const maskWorkerID = int64(1<<BitLenWorkerID - 1)

	elapsedTime := id >> (BitLenSequence + BitLenWorkerID)
	sequence := id & maskSequence >> BitLenWorkerID
	workerId := id & maskWorkerID
	return map[string]int64{
		"id":          id,
		"elapsedTime": elapsedTime,
		"sequence":    sequence,
		"workerId":    workerId,
	}
}
