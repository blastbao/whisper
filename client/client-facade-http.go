package client

import (
	"github.com/blastbao/whisper/common"
	"net/http"
	"strconv"
)

// http for other clients TODO
func (c *Client) getFromHttp(rw http.ResponseWriter, req *http.Request) {
}

func (c *Client) saveFromHttp(rw http.ResponseWriter, req *http.Request) {
}

func (c *Client) Listen() error {
	http.HandleFunc("/get", c.getFromHttp)
	http.HandleFunc("/save", c.saveFromHttp)

	return http.ListenAndServe(":"+strconv.Itoa(common.SERVER_HTTP_PORT_CLIENT), nil)
}
