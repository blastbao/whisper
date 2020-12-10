package common

import (
	"bytes"
	"io/ioutil"
	"strings"
)

var SP []byte = []byte{',', '.', '!'}
var CL []byte = []byte{'\r', '\n'}

var CheckAliveInterval int = 10
var ConfFilePath = "/tmp/whisper.properties"

const (
	SERVER_PORT_MEDIATOR    = 9777
	SERVER_PORT_CENTER      = 9770
	SERVER_PORT_AGENT       = 9771
	SERVER_HTTP_PORT_CLIENT = 8097

	// *** *** center package need
	STATUS_RECORD_BLOCK_BEGIN = 1 // every block init will create a record with this status
	STATUS_RECORD_DEL         = 10
	STATUS_RECORD_DISABLE     = 20

	MIME_JPG = 1
	MIME_PNG = 2
	MIME_GIF = 3
	MIME_BMP = 4

	LOCALHOST = "localhost"

	ROLE_MEDIATOR = 1
	ROLE_CENTER   = 2
	ROLE_AGENT    = 3
	ROLE_CLIENT   = 4
)

type Conf struct {
	Role                    int
	Debug                   bool
	MediatorHost            string
	BaseDir                 string
	MediatorControlBodyFile string
}

var conf *Conf

func GetConf() *Conf {
	if conf == nil {
		conf = &Conf{}

		r, e := GetProperties(ConfFilePath)
		if e != nil {
			Log.Error("get conf error", e)
		} else {
			switch r["role"] {
			case "mediator":
				conf.Role = ROLE_MEDIATOR
			case "center":
				conf.Role = ROLE_CENTER
			case "agent":
				conf.Role = ROLE_AGENT
			case "client":
				conf.Role = ROLE_CLIENT
			default:
				Log.Warning("get conf role required")
			}

			conf.Debug = "true" == r["debug"]
			conf.BaseDir = r["baseDir"]
			conf.MediatorHost = r["mediatorHost"]
			conf.MediatorControlBodyFile = r["mediatorControlBodyFile"]
		}
	}

	return conf
}

func GetProperties(fn string) (r map[string]string, err error) {

	r = make(map[string]string)

	bb, e := ioutil.ReadFile(fn)
	if e != nil {
		err = e
		return
	}

	arr := bytes.Split(bb, []byte{'\n'})
	for _, b := range arr {
		line := strings.TrimSpace(string(b))
		subArr := strings.Split(line, "=")

		if len(subArr) == 2 {
			r[strings.TrimSpace(subArr[0])] = strings.TrimSpace(subArr[1])
		}
	}

	return
}
