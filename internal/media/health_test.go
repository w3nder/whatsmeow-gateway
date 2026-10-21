package media

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRootAnswers200AndOtherPathsStay404(t *testing.T) {
	srv := NewServer(nil, ServerConfig{Addr: ":0", Secret: "s"}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	cases := []struct {
		path string
		want int
	}{
		{"/", http.StatusOK},
		{"/health", http.StatusNotFound},
		{"/calls", http.StatusNotFound},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, c.path, nil))
		if rec.Code != c.want {
			t.Errorf("GET %s = %d, want %d", c.path, rec.Code, c.want)
		}
	}
}
