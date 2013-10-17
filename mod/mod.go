// mod is the entry point to all of the etcd modules.
package mod

import (
	"net/http"

	"github.com/coreos/etcd/mod/dashboard"
	"github.com/gorilla/mux"
)

var ServeMux *http.Handler

func HttpHandler() (handler http.Handler) {
	modMux := mux.NewRouter()
	modMux.Handle("/dashboard", dashboard.HttpHandler())
	return modMux
}
