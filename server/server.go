package server

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	etcdErr "github.com/coreos/etcd/error"
	"github.com/coreos/etcd/log"
	"github.com/coreos/etcd/mod"
	"github.com/coreos/etcd/server/v1"
	"github.com/coreos/etcd/server/v2"
	"github.com/coreos/etcd/store"
	_ "github.com/coreos/etcd/store/v2"
	"github.com/coreos/go-raft"
	"github.com/gorilla/mux"
)

// This is the default implementation of the Server interface.
type Server struct {
	http.Server
	peerServer  *PeerServer
	registry    *Registry
	listener    net.Listener
	store       store.Store
	name        string
	url         string
	tlsConf     *TLSConfig
	tlsInfo     *TLSInfo
	corsOrigins map[string]bool
}

// Creates a new Server.
func New(name string, urlStr string, listenHost string, tlsConf *TLSConfig, tlsInfo *TLSInfo, peerServer *PeerServer, registry *Registry, store store.Store) *Server {
	s := &Server{
		Server: http.Server{
			Handler:   mux.NewRouter(),
			TLSConfig: &tlsConf.Server,
			Addr:      listenHost,
		},
		name:       name,
		store:      store,
		registry:   registry,
		url:        urlStr,
		tlsConf:    tlsConf,
		tlsInfo:    tlsInfo,
		peerServer: peerServer,
	}

	// Install the routes.
	s.handleFunc("/version", s.GetVersionHandler).Methods("GET")
	s.installV1()
	s.installV2()
	s.installMod()

	return s
}

// The current state of the server in the cluster.
func (s *Server) State() string {
	return s.peerServer.RaftServer().State()
}

// The node name of the leader in the cluster.
func (s *Server) Leader() string {
	return s.peerServer.RaftServer().Leader()
}

// The current Raft committed index.
func (s *Server) CommitIndex() uint64 {
	return s.peerServer.RaftServer().CommitIndex()
}

// The current Raft term.
func (s *Server) Term() uint64 {
	return s.peerServer.RaftServer().Term()
}

// The server URL.
func (s *Server) URL() string {
	return s.url
}

// Retrives the Peer URL for a given node name.
func (s *Server) PeerURL(name string) (string, bool) {
	return s.registry.PeerURL(name)
}

// Returns a reference to the Store.
func (s *Server) Store() store.Store {
	return s.store
}

func (s *Server) installV1() {
	s.handleFuncV1("/v1/keys/{key:.*}", v1.GetKeyHandler).Methods("GET")
	s.handleFuncV1("/v1/keys/{key:.*}", v1.SetKeyHandler).Methods("POST", "PUT")
	s.handleFuncV1("/v1/keys/{key:.*}", v1.DeleteKeyHandler).Methods("DELETE")
	s.handleFuncV1("/v1/watch/{key:.*}", v1.WatchKeyHandler).Methods("GET", "POST")
	s.handleFunc("/v1/leader", s.GetLeaderHandler).Methods("GET")
	s.handleFunc("/v1/machines", s.GetMachinesHandler).Methods("GET")
	s.handleFunc("/v1/stats/self", s.GetStatsHandler).Methods("GET")
	s.handleFunc("/v1/stats/leader", s.GetLeaderStatsHandler).Methods("GET")
	s.handleFunc("/v1/stats/store", s.GetStoreStatsHandler).Methods("GET")
}

func (s *Server) installV2() {
	s.handleFuncV2("/v2/keys/{key:.*}", v2.GetHandler).Methods("GET")
	s.handleFuncV2("/v2/keys/{key:.*}", v2.PostHandler).Methods("POST")
	s.handleFuncV2("/v2/keys/{key:.*}", v2.PutHandler).Methods("PUT")
	s.handleFuncV2("/v2/keys/{key:.*}", v2.DeleteHandler).Methods("DELETE")
	s.handleFunc("/v2/leader", s.GetLeaderHandler).Methods("GET")
	s.handleFunc("/v2/machines", s.GetMachinesHandler).Methods("GET")
	s.handleFunc("/v2/stats/self", s.GetStatsHandler).Methods("GET")
	s.handleFunc("/v2/stats/leader", s.GetLeaderStatsHandler).Methods("GET")
	s.handleFunc("/v2/stats/store", s.GetStoreStatsHandler).Methods("GET")
	s.handleFunc("/v2/speedTest", s.SpeedTestHandler).Methods("GET")
}

func (s *Server) installMod() {
	r := s.Handler.(*mux.Router)
	r.PathPrefix("/etcd/mod").Handler(http.StripPrefix("/etcd/mod", mod.HttpHandler()))
}

// Adds a v1 server handler to the router.
func (s *Server) handleFuncV1(path string, f func(http.ResponseWriter, *http.Request, v1.Server) error) *mux.Route {
	return s.handleFunc(path, func(w http.ResponseWriter, req *http.Request) error {
		return f(w, req, s)
	})
}

// Adds a v2 server handler to the router.
func (s *Server) handleFuncV2(path string, f func(http.ResponseWriter, *http.Request, v2.Server) error) *mux.Route {
	return s.handleFunc(path, func(w http.ResponseWriter, req *http.Request) error {
		return f(w, req, s)
	})
}

// Adds a server handler to the router.
func (s *Server) handleFunc(path string, f func(http.ResponseWriter, *http.Request) error) *mux.Route {
	r := s.Handler.(*mux.Router)

	// Wrap the standard HandleFunc interface to pass in the server reference.
	return r.HandleFunc(path, func(w http.ResponseWriter, req *http.Request) {
		// Log request.
		log.Debugf("[recv] %s %s %s [%s]", req.Method, s.url, req.URL.Path, req.RemoteAddr)

		// Write CORS header.
		if s.OriginAllowed("*") {
			w.Header().Add("Access-Control-Allow-Origin", "*")
		} else if origin := req.Header.Get("Origin"); s.OriginAllowed(origin) {
			w.Header().Add("Access-Control-Allow-Origin", origin)
		}

		// Execute handler function and return error if necessary.
		if err := f(w, req); err != nil {
			if etcdErr, ok := err.(*etcdErr.Error); ok {
				log.Debug("Return error: ", (*etcdErr).Error())
				etcdErr.Write(w)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}
	})
}

// Start to listen and response etcd client command
func (s *Server) ListenAndServe() error {
	log.Infof("etcd server [name %s, listen on %s, advertised url %s]", s.name, s.Server.Addr, s.url)

	if s.tlsConf.Scheme == "http" {
		return s.listenAndServe()
	} else {
		return s.listenAndServeTLS(s.tlsInfo.CertFile, s.tlsInfo.KeyFile)
	}
}

// Overridden version of net/http added so we can manage the listener.
func (s *Server) listenAndServe() error {
	addr := s.Server.Addr
	if addr == "" {
		addr = ":http"
	}
	l, e := net.Listen("tcp", addr)
	if e != nil {
		return e
	}
	s.listener = l
	return s.Server.Serve(l)
}

// Overridden version of net/http added so we can manage the listener.
func (s *Server) listenAndServeTLS(certFile, keyFile string) error {
	addr := s.Server.Addr
	if addr == "" {
		addr = ":https"
	}
	config := &tls.Config{}
	if s.Server.TLSConfig != nil {
		*config = *s.Server.TLSConfig
	}
	if config.NextProtos == nil {
		config.NextProtos = []string{"http/1.1"}
	}

	var err error
	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}

	conn, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	tlsListener := tls.NewListener(conn, config)
	s.listener = tlsListener
	return s.Server.Serve(tlsListener)
}

// Stops the server.
func (s *Server) Close() {
	if s.listener != nil {
		s.listener.Close()
		s.listener = nil
	}
}

func (s *Server) Dispatch(c raft.Command, w http.ResponseWriter, req *http.Request) error {
	ps := s.peerServer
	if ps.raftServer.State() == raft.Leader {
		result, err := ps.raftServer.Do(c)
		if err != nil {
			return err
		}

		if result == nil {
			return etcdErr.NewError(300, "Empty result from raft", store.UndefIndex, store.UndefTerm)
		}

		// response for raft related commands[join/remove]
		if b, ok := result.([]byte); ok {
			w.WriteHeader(http.StatusOK)
			w.Write(b)
			return nil
		}

		var b []byte
		if strings.HasPrefix(req.URL.Path, "/v1") {
			b, _ = json.Marshal(result.(*store.Event).Response())
			w.WriteHeader(http.StatusOK)
		} else {
			e, _ := result.(*store.Event)
			b, _ = json.Marshal(e)

			if e.IsCreated() {
				w.WriteHeader(http.StatusCreated)
			} else {
				w.WriteHeader(http.StatusOK)
			}
		}

		w.Write(b)

		return nil

	} else {
		leader := ps.raftServer.Leader()

		// No leader available.
		if leader == "" {
			return etcdErr.NewError(300, "", store.UndefIndex, store.UndefTerm)
		}

		var url string
		switch c.(type) {
		case *JoinCommand, *RemoveCommand:
			url, _ = ps.registry.PeerURL(leader)
		default:
			url, _ = ps.registry.ClientURL(leader)
		}
		redirect(url, w, req)

		return nil
	}
}

// Sets a comma-delimited list of origins that are allowed.
func (s *Server) AllowOrigins(origins []string) error {
	// Construct a lookup of all origins.
	m := make(map[string]bool)
	for _, v := range origins {
		if v != "*" {
			if _, err := url.Parse(v); err != nil {
				return fmt.Errorf("Invalid CORS origin: %s", err)
			}
		}
		m[v] = true
	}
	s.corsOrigins = m

	return nil
}

// Determines whether the server will allow a given CORS origin.
func (s *Server) OriginAllowed(origin string) bool {
	return s.corsOrigins["*"] || s.corsOrigins[origin]
}

// Handler to return the current version of etcd.
func (s *Server) GetVersionHandler(w http.ResponseWriter, req *http.Request) error {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "etcd %s", ReleaseVersion)
	return nil
}

// Handler to return the current leader's raft address
func (s *Server) GetLeaderHandler(w http.ResponseWriter, req *http.Request) error {
	leader := s.peerServer.RaftServer().Leader()
	if leader == "" {
		return etcdErr.NewError(etcdErr.EcodeLeaderElect, "", store.UndefIndex, store.UndefTerm)
	}
	w.WriteHeader(http.StatusOK)
	url, _ := s.registry.PeerURL(leader)
	w.Write([]byte(url))
	return nil
}

// Handler to return all the known machines in the current cluster.
func (s *Server) GetMachinesHandler(w http.ResponseWriter, req *http.Request) error {
	machines := s.registry.ClientURLs(s.peerServer.RaftServer().Leader(), s.name)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(strings.Join(machines, ", ")))
	return nil
}

// Retrieves stats on the Raft server.
func (s *Server) GetStatsHandler(w http.ResponseWriter, req *http.Request) error {
	w.Write(s.peerServer.Stats())
	return nil
}

// Retrieves stats on the leader.
func (s *Server) GetLeaderStatsHandler(w http.ResponseWriter, req *http.Request) error {
	if s.peerServer.RaftServer().State() == raft.Leader {
		w.Write(s.peerServer.PeerStats())
		return nil
	}

	leader := s.peerServer.RaftServer().Leader()
	if leader == "" {
		return etcdErr.NewError(300, "", store.UndefIndex, store.UndefTerm)
	}
	hostname, _ := s.registry.ClientURL(leader)
	redirect(hostname, w, req)
	return nil
}

// Retrieves stats on the leader.
func (s *Server) GetStoreStatsHandler(w http.ResponseWriter, req *http.Request) error {
	w.Write(s.store.JsonStats())
	return nil
}

// Executes a speed test to evaluate the performance of update replication.
func (s *Server) SpeedTestHandler(w http.ResponseWriter, req *http.Request) error {
	count := 1000
	c := make(chan bool, count)
	for i := 0; i < count; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				c := s.Store().CommandFactory().CreateSetCommand("foo", "bar", time.Unix(0, 0))
				s.peerServer.RaftServer().Do(c)
			}
			c <- true
		}()
	}

	for i := 0; i < count; i++ {
		<-c
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("speed test success"))
	return nil
}
