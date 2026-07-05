package api

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"

	"github.com/aleskxyz/tgtv/internal/config"
	"github.com/aleskxyz/tgtv/internal/discovery"
	"github.com/aleskxyz/tgtv/internal/thumbnails"
)

var uriInTag = regexp.MustCompile(`URI="([^"]+)"`)
var hlsFilenamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func validHLSFilename(name string) bool {
	base := strings.TrimSpace(strings.Split(name, "?")[0])
	return base != "" && hlsFilenamePattern.MatchString(base)
}

func MediamtxHeaders(cdnSecret string) map[string]string {
	return map[string]string{"Authorization": "Bearer " + cdnSecret}
}

func BuildPlaylist(registry *discovery.Registry, thumbs *thumbnails.Store, cfg config.Settings, pathSecret string, skipStream func(string) bool) string {
	lines := []string{"#EXTM3U"}
	base := cfg.PublicBaseURL + "/p/" + pathSecret
	entries := registry.ActiveLives()
	slices.SortFunc(entries, func(a, b discovery.LiveEntry) int {
		return strings.Compare(strings.ToLower(a.Title), strings.ToLower(b.Title))
	})
	for _, entry := range entries {
		if skipStream != nil && skipStream(entry.StreamID) {
			continue
		}
		name := strings.ReplaceAll(entry.Title, `"`, "'")
		logo := fmt.Sprintf("%s/thumbnails/%s.jpg", base, entry.StreamID)
		if thumbs != nil {
			if v := thumbs.PhotoVersion(entry.ChatID); v != 0 {
				logo = fmt.Sprintf("%s?v=%d", logo, v)
			}
		}
		lines = append(lines,
			fmt.Sprintf(`#EXTINF:-1 tvg-id="%s" tvg-name="%s" tvg-logo="%s" group-title="TGTV",%s`,
				entry.StreamID, name, logo, name),
			fmt.Sprintf("#EXTALBUMARTURL:%s", logo),
			fmt.Sprintf("%s/streams/%s/play.m3u8", base, entry.StreamID),
		)
	}
	return strings.Join(lines, "\n") + "\n"
}

func RewriteHLSPlaylist(text, base string) string {
	base = strings.TrimRight(base, "/")
	text = stripLLHLSGaps(text)
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			out = append(out, line)
			continue
		}
		if strings.HasPrefix(line, "#") {
			out = append(out, uriInTag.ReplaceAllStringFunc(line, func(s string) string {
				m := uriInTag.FindStringSubmatch(s)
				if len(m) < 2 {
					return s
				}
				return `URI="` + rewriteURI(m[1], base) + `"`
			}))
			continue
		}
		out = append(out, rewriteURI(line, base))
	}
	return strings.Join(out, "\n") + "\n"
}

func rewriteURI(uri, base string) string {
	uri = strings.TrimSpace(strings.Split(uri, "?")[0])
	if uri == "" || strings.HasPrefix(uri, "http://") || strings.HasPrefix(uri, "https://") {
		return uri
	}
	path := strings.TrimPrefix(uri, "/")
	if strings.HasPrefix(path, "live/") {
		parts := strings.SplitN(path, "/", 3)
		if len(parts) == 3 {
			path = parts[2]
		}
	}
	return base + "/" + path
}

func stripLLHLSGaps(text string) string {
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	for i := 0; i < len(lines); i++ {
		if lines[i] == "#EXT-X-GAP" {
			i++
			if i < len(lines) && strings.HasPrefix(lines[i], "#EXTINF") {
				i++
			}
			if i < len(lines) && (strings.TrimSpace(lines[i]) == "gap.mp4" || strings.TrimSpace(lines[i]) == "gap.ts") {
				continue
			}
		}
		out = append(out, lines[i])
	}
	return strings.Join(out, "\n") + "\n"
}

func SlateMasterPlaylist(hlsBase string) string {
	base := strings.TrimRight(hlsBase, "/")
	return "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-STREAM-INF:BANDWIDTH=1\n" + base + "/main_stream.m3u8\n"
}

func SlateMediaPlaylist() string {
	return "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n"
}

// InjectDiscontinuity prepends #EXT-X-DISCONTINUITY before the next segment entry
// in a media playlist. Master playlists (play/index) are left unchanged.
func InjectDiscontinuity(text string) string {
	if !strings.Contains(text, "#EXTINF") {
		return text
	}
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		if strings.HasPrefix(line, "#EXTINF") || (line != "" && !strings.HasPrefix(line, "#")) {
			if i > 0 && lines[i-1] == "#EXT-X-DISCONTINUITY" {
				return text
			}
			lines = append(lines[:i], append([]string{"#EXT-X-DISCONTINUITY"}, lines[i:]...)...)
			return strings.Join(lines, "\n") + "\n"
		}
	}
	return text
}

func ProxyMediamtx(client *http.Client, url string, headers map[string]string) (body []byte, contentType string, status int, err error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, "", 0, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", resp.StatusCode, err
	}
	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	return data, ct, resp.StatusCode, nil
}
