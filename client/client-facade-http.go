package client

import (
	"github.com/blastbao/whisper/common"
	"net/http"
	"strconv"
)

// http for other clients TODO
func (this *Client) getFromHttp(rw http.ResponseWriter, req *http.Request) {
}

func (this *Client) saveFromHttp(rw http.ResponseWriter, req *http.Request) {
}

func (this *Client) Listen() error {
	http.HandleFunc("/get", this.getFromHttp)
	http.HandleFunc("/save", this.saveFromHttp)

	return http.ListenAndServe(":"+strconv.Itoa(common.SERVER_HTTP_PORT_CLIENT), nil)
}
