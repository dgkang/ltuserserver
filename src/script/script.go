package script

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"util"
	"strings"
	"time"
	"github.com/fzzy/radix/redis"
)

type ScriptFile struct {
	LastModTime time.Time
	PathFile    string
	Buffer      []byte
	ScriptSHA1  string
}

type Script struct {
	Path      string
	L         *util.Logging
	ScriptBuf map[string]*ScriptFile
}

func New(Path string, L *util.Logging) *Script {
	if Path[len(Path)-1] == '/' {
		Path = Path[:len(Path)-1]
	}
	s := &Script{Path, L, make(map[string]*ScriptFile)}
	return s
}

func (S *Script) Load(name string) (*ScriptFile, error) {
	scriptFile := fmt.Sprintf("%s/%s", S.Path, name)
	f, e := os.Open(scriptFile)
	if e != nil {
		return nil, e
	}

	var LastTime time.Time
	if i, e := f.Stat(); e == nil {
		LastTime = i.ModTime()
	} else {
		return nil, e
	}

	if b, e := ioutil.ReadAll(f); e == nil {
		h := sha1.New()
		h.Write(b)
		F := &ScriptFile{LastTime, scriptFile, b, hex.EncodeToString(h.Sum(nil))}
		S.ScriptBuf[name] = F
		return F, nil
	} else {
		return nil, e
	}
}

func (S *ScriptFile) IsModfiy() (b bool, err error) {
	f, e := os.Open(S.PathFile)
	if e != nil {
		err = e
		return
	}

	var LastTime time.Time
	if i, e := f.Stat(); e == nil {
		LastTime = i.ModTime()
	} else {
		err = e
		return
	}
	b = !LastTime.Equal(S.LastModTime)
	return
}

func (F *ScriptFile) Cmd (cmd string,redisSession *util.RedisSession,v []interface{}) (*redis.Reply,error) {
	if r := redisSession.SendCommand(cmd, v...); r.Type == redis.ErrorReply {
		if strings.Contains(r.Err.Error(), "NOSCRIPT") {
			v[0] = string(F.Buffer)
			return F.Cmd("eval",redisSession,v)
		}else{
			return nil,r.Err
		}
	}else{
		return r, nil
	}
}

func (S *Script) Run(redisSession *util.RedisSession, name string, args ...interface{}) (*redis.Reply, error) {
	var F *ScriptFile
	var err error
	var ok bool

	if F, ok = S.ScriptBuf[name]; !ok {
		if F, err = S.Load(name); err != nil {
			return nil, err
		}
	} else {
		if b, e := F.IsModfiy(); e != nil {
			return nil, e
		} else if b {
			if F, err = S.Load(name); err != nil {
				return nil, err
			}
		}
	}

	a := []interface{}{F.ScriptSHA1}
	for _, v := range args {
		a = append(a, v)
	}
	return F.Cmd("evalsha",redisSession,a)
}
