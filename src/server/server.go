package main

import(
	"fmt"
	"util"
	"script"
	"net/http"
	"syscall"
	"os"
	"encoding/json"
	"time"
    "github.com/fzzy/radix/redis"
)


const (
 	ERR_NONE			= 0
	ERR_INVALID_URL		= 1
	ERR_REDIS			= 2
	ERR_LUA				= 3
)

var ErrMsg = map[int]string{
	ERR_NONE				:"Success",
	ERR_INVALID_URL			:"Failure: some field not exist!",
	ERR_REDIS				:"Failure: redis error!",
	ERR_LUA					:"Failure: lua error!",
}

type HttpServer struct {
	  ListenHost string `json:"listenhost"`
      ListenPort int `json:"listenport"`
	  Redis []struct {
		  RedisPool *util.RedisConnectionPool
		  RedisHost string	`json:"redishost"`
		  RedisPort int		`json:"redisport"`
		  RedisName string 	`json:"name"`
	  }`json:"redis"`
	  Log 		*util.Logging
	  LuaScript *script.Script
	  W 		http.ResponseWriter
	  R 		*http.Request
	  MaxRedisConn int
}


func New(lf string,sf string,maxRedisConn int) *HttpServer {
	if l, e := util.NewLogging(lf, util.INFO); e != nil {
		fmt.Printf("Create log file error,%s",e.Error())
		return nil
	}else {
		return &HttpServer{Log:l,LuaScript:script.New(sf,l),MaxRedisConn:maxRedisConn}
	}
}


func (S *HttpServer) Reload(s os.Signal, v interface{}) {
	S.Log.Printf(util.DEBUG,"Signal: %v",s)
}

func (S *HttpServer) Load(cf string) error{
	if e := util.LoadConf(cf, S); e != nil {
		return e
	}
	for i := 0; i < len(S.Redis); i++ {
		S.Redis[i].RedisPool = util.NewRedisConnectionPool(S.MaxRedisConn,S.Redis[i].RedisHost,S.Redis[i].RedisPort,S.Log)
		if e := S.Redis[i].RedisPool.Init(); e != nil {
			return e 
		}
	}
	return nil
}

func (S *HttpServer) Reply(format string,v ...interface{}) {
	fmt.Fprintf(S.W,format,v...)
}

func (S *HttpServer) ReplyError(code int, format string, v ...interface{}){
	s := fmt.Sprintf(format, v...)
	S.Log.Printf(util.ERROR,s)
	m := map[string]interface{}{ "code": code,"msg":  s,}
	b, _ := json.Marshal(m)
	S.Reply(string(b))
}

func (S *HttpServer) GetRedisPool(n string) *util.RedisConnectionPool {
	for _,v := range S.Redis {
		if v.RedisName == n {
			return v.RedisPool
		}
	}
	return nil
}


func (S *HttpServer)ParseRedisLua(r *redis.Reply,e error){
	if e != nil {
		S.ReplyError(ERR_REDIS,"redis: %s",e.Error())
	}else{
		if s, er := r.Str(); er != nil {
			S.ReplyError(ERR_REDIS,"redis: %s",e.Error())
		}else{
			var rj interface{}
			if e := json.Unmarshal([]byte(s), &rj); e != nil {
				S.ReplyError(ERR_LUA,"\"%s\"should be a json",s)
			}else{
				S.Reply(s)
			}
		}
	}
}

func (S *HttpServer) OnSignup(jdata string) {
	RedisPool := S.GetRedisPool("master")
	if RedisPool == nil {
		S.ReplyError(ERR_REDIS,ErrMsg[ERR_REDIS])
		return
	}
	rSession := RedisPool.Acquire()
	now := time.Now().Unix()
	S.ParseRedisLua(S.LuaScript.Run(rSession,"Signup.lua",0,jdata,now))
	RedisPool.Release(rSession)
}

func (S *HttpServer) ServeHTTP(w http.ResponseWriter, r *http.Request){
	S.W,S.R = w,r
	defer util.CrashStack(S.Log)	
	r.ParseForm()

	body := r.PostFormValue("json_data")
	var jdata map[string] interface{}

	if e := json.Unmarshal([]byte(body), &jdata); e != nil {
		S.ReplyError(ERR_INVALID_URL,ErrMsg[ERR_INVALID_URL])
		return
	}

	action := r.FormValue("action")
	switch(action){
		case "sign_up":
			S.OnSignup(body)

		default:
			S.ReplyError(ERR_INVALID_URL,ErrMsg[ERR_INVALID_URL])
			return
	}
}


func (S *HttpServer) Run() {	
	a := fmt.Sprintf("%s:%d", S.ListenHost, S.ListenPort)
	
	http.Handle("/v1/go/interactive",S)

	e := http.ListenAndServe(a, nil)
	if e != nil {
		S.Log.Printf(util.ERROR, e.Error())
	}
}

func main(){
	var S *HttpServer
	if S = New("log","./script",3); S == nil {
		return
	}
	if e := S.Load("./conf"); e != nil {
		S.Log.Printf(util.ERROR,"HttpServer load failed,%s",e.Error())
		return
	}
	go func() {
		Signal := util.SignalNew(S.Log)
		Signal.Register(syscall.SIGUSR1, S.Reload, S)
		Signal.Listen()
	}()
	
	S.Run()
}
