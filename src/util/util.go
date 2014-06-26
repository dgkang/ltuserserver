package util

import (
	"bytes"
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/fzzy/radix/redis"
	"github.com/wendal/goyaml2"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path"
	"runtime"
	"time"
)

const (
	ERROR = 0
	INFO  = 1
	WARN  = 2
	DEBUG = 3
)

func CrashStack(L *Logging) {
	if err := recover(); err != nil {
		L.Printf(ERROR, "Exception: %v", err)
		for i := 1; ; i++ {
			_, file, line, ok := runtime.Caller(i)
			if file != "" {
				L.Printf(ERROR, "Stack: %s,%d", file, line)
			}
			if !ok {
				break
			}
		}
	}
}

type SignalHandler func(s os.Signal, d interface{})

type Handler struct {
	h SignalHandler
	d interface{}
}

type SignalSet struct {
	m map[os.Signal]*Handler
	l *Logging
}

func (ss *SignalSet) Register(s os.Signal, h SignalHandler, d interface{}) {
	if _, ok := ss.m[s]; !ok {
		ss.m[s] = &Handler{h, d}
	}
}

func (ss *SignalSet) Handle(s os.Signal) (err error) {
	if _, found := ss.m[s]; found {
		ss.m[s].h(s, ss.m[s].d)
		return nil
	} else {
		return fmt.Errorf("%v signal no register handler", s)
	}
}

func (ss *SignalSet) Listen() {
	for {
		c := make(chan os.Signal)

		var sigs []os.Signal
		for sig, _ := range ss.m {
			sigs = append(sigs, sig)
		}
		signal.Notify(c, sigs...)
		sig := <-c

		if e := ss.Handle(sig); e != nil {
			ss.l.Printf(ERROR, "%s", e.Error())
		}
	}
}

func SignalNew(l *Logging) *SignalSet {
	ss := &SignalSet{make(map[os.Signal]*Handler), l}
	return ss
}

func RandProbability(P []int, R int) int {
	if b := func() bool {
		T := 0
		for _, v := range P {
			T += v
		}
		if T != R {
			return false
		}
		return true
	}(); !b {
		return -1
	}

	r := rand.Int() % R
	for k, v := range P {
		if r <= v {
			return k
		} else {
			r -= v
		}
	}
	return -1
}

func LoadConf(file string, conf interface{}) error {

	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	b, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}

	err = json.Unmarshal(b, &conf)
	if err != nil {
		return err
	}
	return nil
}

type YAMLConfigContainer struct {
	data map[string]interface{}
}

func NewYaml(path string) (*YAMLConfigContainer, error) {
	y := &YAMLConfigContainer{data: make(map[string]interface{})}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	if len(buf) < 3 {
		return nil, errors.New("len(buf) < 3")
	}

	_map, _err := goyaml2.Read(bytes.NewBuffer(buf))
	if _err != nil {
		return nil, _err
	}

	if _map == nil {
		return nil, errors.New("Goyaml2 Read failed.")
	}

	r, ok := _map.(map[string]interface{})
	if !ok {
		return nil, errors.New("Result not a map")
	}
	y.data = r
	return y, nil
}

func (y *YAMLConfigContainer) Int(key string) (int, error) {
	if v, ok := y.data[key].(int64); ok {
		return int(v), nil
	}
	return 0, errors.New("not int value")
}

func (y *YAMLConfigContainer) String(key string) string {
	if v, ok := y.data[key].(string); ok {
		return v
	}
	return ""
}

type Logging struct {
	*log.Logger
	baseLevel int
}

func (l *Logging) Printf(level int, format string, v ...interface{}) error {
	sFlag := [...]string{"ERROR", " INFO", " WARN", "DEBUG"}

	if level < 0 || level >= len(sFlag) {
		return fmt.Errorf("bad level value,%d", level)
	}
	s := fmt.Sprintf(format, v...)

	_, file, line, _ := runtime.Caller(1)
	base := path.Base(file)
	l.Logger.Printf("[%s] [%s,%d] %s", sFlag[level], base, line, s)
	return nil
}

func NewLogging(LogName string, baseLevel int) (*Logging, error) {
	f, e := os.OpenFile(LogName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if e != nil {
		return nil, e
	}
	return &Logging{log.New(f, "", log.LstdFlags), baseLevel}, nil
}

type RedisSession struct {
	RedisConn *redis.Client
	RedisAddr string
	Log       *Logging
}

type RedisConnectionPool struct {
	Sessions []*RedisSession
	MaxConn  int
	WChan    chan *RedisSession
	RChan    chan *RedisSession
	InitChan chan error
	Host     string
	Port     int
	L        *Logging
}

func NewRedisConnectionPool(maxConn int, host string, port int, l *Logging) *RedisConnectionPool {
	return &RedisConnectionPool{
		Sessions: make([]*RedisSession, maxConn),
		MaxConn:  maxConn,
		WChan:    make(chan *RedisSession),
		RChan:    make(chan *RedisSession),
		InitChan: make(chan error),
		Host:     host,
		Port:     port,
		L:        l}
}

func (r *RedisConnectionPool) Run() {
	freeList := list.New()
	freeList.Init()
	var e error

	for i := 0; i < len(r.Sessions); i++ {
		r.Sessions[i] = NewRedisSession(r.Host, r.Port, r.L)
		e = r.Sessions[i].Connect()
		if e != nil {
			break
		}
		freeList.PushBack(r.Sessions[i])
		r.L.Printf(INFO, "[%d]Connect redis server %s", i+1, r.Sessions[i].RedisAddr)
	}
	r.InitChan <- e
	if e != nil {
		return
	}
	r.L.Printf(INFO, "RedisConnectionPool ready ok,Number of pool is %d", len(r.Sessions))

	var t *RedisSession = nil
	for {
		e := freeList.Front()
		if e != nil {
			t = e.Value.(*RedisSession)
		} else {
			t = nil
		}

		select {
		case r.WChan <- t:
			if t != nil {
				freeList.Remove(e)
			}
		case s := <-r.RChan:
			freeList.PushBack(s)
		}
	}
}

func (r *RedisConnectionPool) Init() error {
	go r.Run()
	e := <-r.InitChan
	return e
}

func (r *RedisConnectionPool) Acquire() *RedisSession {
	for {
		session := <-r.WChan
		if session == nil {
			time.Sleep(time.Millisecond * 30)
			r.L.Printf(WARN, "Wait for get session,30 msec")
		} else {
			return session
		}
	}
	return nil
}

func (r *RedisConnectionPool) Release(s *RedisSession) {
	if s != nil {
		r.RChan <- s
	} else {
		r.L.Printf(ERROR, "PutSession  nil.")
	}
}

func NewRedisSession(host string, port int, l *Logging) *RedisSession {
	s := new(RedisSession)
	s.RedisAddr = fmt.Sprintf("%s:%d", host, port)
	s.Log = l
	return s
}

func (s *RedisSession) Connect() error {
	c, e := redis.Dial("tcp", s.RedisAddr)
	if e != nil {
		return fmt.Errorf("Connect redis server %s failed,%s ", s.RedisAddr, e.Error())
	}
	s.RedisConn = c
	return nil
}

func (s *RedisSession) SendCommand(commandName string, args ...interface{}) *redis.Reply {
	var r *redis.Reply
	for i := 0; i < 3; i++ {
		r = s.RedisConn.Cmd(commandName, args...)
		if r.Err != nil && r.Err == io.EOF {
			s.Log.Printf(ERROR, "redis disconnect,%s", r.Err.Error())
			s.Close()
			s.Connect()
		} else {
			break
		}
	}
	return r
}

func (s *RedisSession) Close() {
	s.RedisConn.Close()
}
