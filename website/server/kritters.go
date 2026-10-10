package main

import (
	"bytes"
	"embed"
	"image"
	"image/color"
	"image/png"
	"log/slog"
	"net/http"
	"sync"
)

// Kritter art is served from the binary, not the static bundle. nori is the
// revealed kubagachi mascot and ships in full color. The finale — yscale 000 —
// is the phoenix, but we grayscale it HERE, on the server, before a single byte
// leaves: the color original is embedded in the binary and is never network-
// reachable, so the "who's that kritter?" reveal can't be spoiled by
// view-source or by toggling off a CSS filter.
//
//go:embed assets/nori.png assets/yscale-phoenix.png assets/nori-keyed.png assets/phoenix-keyed.png
var kritterAssets embed.FS

// grayscaleMaxDim caps the longest side of the grayscaled finale. The source
// is 1536² — far more than a teaser needs — so we box-average it down, which
// both shrinks the payload and softens it a touch (more mysterious).
const grayscaleMaxDim = 640

var (
	kritterOnce sync.Once
	kritterArt  map[string][]byte
)

// loadKritterArt builds the ready-to-serve PNG bytes for each kritter path
// exactly once, then hands back the shared map. Building is fail-open: a bad
// asset logs and is simply absent from the map (a 404), never a crash-loop —
// the marketing site must not die over a mascot.
func loadKritterArt() map[string][]byte {
	kritterOnce.Do(func() {
		art := map[string][]byte{}

		// nori — the kubagachi drop's mascot, revealed, full color.
		if b, err := kritterAssets.ReadFile("assets/nori.png"); err != nil {
			slog.Error("kritter: nori asset unreadable", "err", err)
		} else {
			art["nori.png"] = b
		}

		// nori-sheet — the nori keyed sheet, full color, raw bytes.
		if b, err := kritterAssets.ReadFile("assets/nori-keyed.png"); err != nil {
			slog.Error("kritter: nori-sheet asset unreadable", "err", err)
		} else {
			art["nori-sheet.png"] = b
		}

		// yscale-000 — the phoenix, grayscaled server-side into a locked teaser.
		// Named yscale-000, not phoenix, so the URL itself gives nothing away.
		if b, err := grayscaleFinale(); err != nil {
			slog.Error("kritter: finale grayscale failed", "err", err)
		} else {
			art["yscale-000.png"] = b
		}

		// yscale-000-sheet — the phoenix keyed sheet, grayscaled.
		if b, err := grayscalePhoenixSheet(); err != nil {
			slog.Error("kritter: yscale-000-sheet grayscale failed", "err", err)
		} else {
			art["yscale-000-sheet.png"] = b
		}

		kritterArt = art
	})
	return kritterArt
}

// grayscaleFinale decodes the embedded color phoenix and returns a grayscale,
// downscaled PNG. This is the whole point of serving from the server: the color
// bytes exist only in memory here and are never emitted.
func grayscaleFinale() ([]byte, error) {
	f, err := kritterAssets.Open("assets/yscale-phoenix.png")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	src, _, err := image.Decode(f)
	if err != nil {
		return nil, err
	}
	gray := grayscaleDownscale(src, grayscaleMaxDim)
	var buf bytes.Buffer
	enc := png.Encoder{CompressionLevel: png.BestCompression}
	if err := enc.Encode(&buf, gray); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// grayscalePhoenixSheet decodes the embedded color phoenix keyed sprite sheet,
// grayscales it while preserving transparency, and returns the PNG bytes.
func grayscalePhoenixSheet() ([]byte, error) {
	f, err := kritterAssets.Open("assets/phoenix-keyed.png")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	src, _, err := image.Decode(f)
	if err != nil {
		return nil, err
	}
	b := src.Bounds()
	dst := image.NewNRGBA(b)
	for y := b.Min.Y; y < b.Max.Y; y++ {
		for x := b.Min.X; x < b.Max.X; x++ {
			c := src.At(x, y)
			nrgba := color.NRGBAModel.Convert(c).(color.NRGBA)
			// Rec.601 luma formula
			luma := (uint32(nrgba.R)*299 + uint32(nrgba.G)*587 + uint32(nrgba.B)*114) / 1000
			dst.SetNRGBA(x, y, color.NRGBA{
				R: uint8(luma),
				G: uint8(luma),
				B: uint8(luma),
				A: nrgba.A,
			})
		}
	}
	var buf bytes.Buffer
	enc := png.Encoder{CompressionLevel: png.BestCompression}
	if err := enc.Encode(&buf, dst); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// grayscaleDownscale converts src to grayscale and box-averages it down so its
// longest side is at most maxDim. Rec.601 luma; averaging each source block
// keeps edges clean instead of the aliasing a nearest-neighbour pick would give.
func grayscaleDownscale(src image.Image, maxDim int) *image.Gray {
	b := src.Bounds()
	sw, sh := b.Dx(), b.Dy()

	scale := 1
	for max(sw, sh)/scale > maxDim {
		scale++
	}
	dw, dh := max(sw/scale, 1), max(sh/scale, 1)

	dst := image.NewGray(image.Rect(0, 0, dw, dh))
	for dy := 0; dy < dh; dy++ {
		for dx := 0; dx < dw; dx++ {
			var sum, n uint32
			for oy := 0; oy < scale; oy++ {
				for ox := 0; ox < scale; ox++ {
					sx, sy := b.Min.X+dx*scale+ox, b.Min.Y+dy*scale+oy
					if sx >= b.Max.X || sy >= b.Max.Y {
						continue
					}
					// RGBA() is 16-bit, alpha-premultiplied; the phoenix is fully
					// opaque so premultiplied == straight here.
					r, g, bl, _ := src.At(sx, sy).RGBA()
					sum += (299*r + 587*g + 114*bl) / 1000 >> 8 // 16-bit luma -> 8-bit
					n++
				}
			}
			if n == 0 {
				n = 1
			}
			dst.SetGray(dx, dy, color.Gray{Y: uint8(sum / n)})
		}
	}
	return dst
}

// handleKritter serves a kritter's PNG bytes by path segment. GET also answers
// HEAD (net/http maps it), so we suppress the body for HEAD explicitly.
func (a *app) handleKritter(w http.ResponseWriter, r *http.Request) {
	b, ok := a.kritters[r.PathValue("name")]
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Cache-Control", "public, max-age=86400")
	if r.Method == http.MethodHead {
		return
	}
	w.Write(b) //nolint:errcheck // client went away; nothing to do
}
