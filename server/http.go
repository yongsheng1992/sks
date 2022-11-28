package server

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
)

type httpKVAPI struct {
	store *kvstore
}

func (h *httpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI
	switch r.Method {
	case http.MethodGet:
		if value, ok := h.store.Get(key); ok {
			if _, err := w.Write([]byte(value)); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		} else {
			http.Error(w, "Failed to Get", http.StatusNotFound)
		}
	case http.MethodPut:
		val, err := ioutil.ReadAll(r.Body)
		fmt.Println(string(val))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err = h.store.Put(key, string(val))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func serverHTTP(kv *kvstore, errorC <-chan error) {
	srv := http.Server{
		Addr: ":" + strconv.Itoa(8090),
		Handler: &httpKVAPI{
			store: kv,
		},
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}
