package serviceenrich

import (
	"regexp"
	"strings"
)

var (
	reHTMLTitle  = regexp.MustCompile(`(?is)<title[^>]*>([^<]+)</title>`)
	reOGDesc     = regexp.MustCompile(`(?is)<meta[^>]+property=["']og:description["'][^>]+content=["']([^"']+)["']`)
	reMetaDesc   = regexp.MustCompile(`(?is)<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["']`)
	reOGDescAlt  = regexp.MustCompile(`(?is)<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:description["']`)
	reOGTitle    = regexp.MustCompile(`(?is)<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)["']`)
	reOGTitleAlt = regexp.MustCompile(`(?is)<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:title["']`)
	reOGImage    = regexp.MustCompile(`(?is)<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']`)
	reOGImageAlt = regexp.MustCompile(`(?is)<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:image["']`)
)

// HTMLMeta holds title, description and image extracted from HTML bodies.
type HTMLMeta struct {
	PageTitle       string
	PageDescription string
	PageImage       string
}

// EnrichHTML extracts page title, description and image from HTML snippets.
// og:title / og:image are preferred over <title> when present, since they are
// curated for the page's identity rather than site-wide branding.
func EnrichHTML(html string) HTMLMeta {
	html = strings.TrimSpace(html)
	if html == "" {
		return HTMLMeta{}
	}
	var out HTMLMeta
	if m := reOGTitle.FindStringSubmatch(html); len(m) > 1 {
		out.PageTitle = strings.TrimSpace(m[1])
	} else if m := reOGTitleAlt.FindStringSubmatch(html); len(m) > 1 {
		out.PageTitle = strings.TrimSpace(m[1])
	} else if m := reHTMLTitle.FindStringSubmatch(html); len(m) > 1 {
		out.PageTitle = strings.TrimSpace(m[1])
	}
	if m := reOGDesc.FindStringSubmatch(html); len(m) > 1 {
		out.PageDescription = strings.TrimSpace(m[1])
	} else if m := reOGDescAlt.FindStringSubmatch(html); len(m) > 1 {
		out.PageDescription = strings.TrimSpace(m[1])
	} else if m := reMetaDesc.FindStringSubmatch(html); len(m) > 1 {
		out.PageDescription = strings.TrimSpace(m[1])
	}
	if m := reOGImage.FindStringSubmatch(html); len(m) > 1 {
		out.PageImage = strings.TrimSpace(m[1])
	} else if m := reOGImageAlt.FindStringSubmatch(html); len(m) > 1 {
		out.PageImage = strings.TrimSpace(m[1])
	}
	return out
}
