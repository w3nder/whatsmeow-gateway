package call

import (
	"context"
	"fmt"
	"time"

	"github.com/w3nder/whatsmeow-gateway/internal/amqp"
)

const defaultVideoFrameRate = 30

const (
	nalSlice      = 1
	nalIDR        = 5
	nalSEI        = 6
	nalSPS        = 7
	nalPPS        = 8
	nalDelimiter  = 9
	nalTypeMask   = 0x1F
	minNALPayload = 1
)

func SplitAnnexB(stream []byte) [][]byte {
	starts := nalStarts(stream)
	if len(starts) == 0 {
		return nil
	}

	var units [][]byte
	unitStart := starts[0].offset
	hasSlice := false

	for i, nal := range starts {
		end := len(stream)
		if i+1 < len(starts) {
			end = starts[i+1].offset
		}

		if boundary(nal.nalType, hasSlice) && nal.offset > unitStart {
			units = append(units, stream[unitStart:nal.offset])
			unitStart = nal.offset
			hasSlice = false
		}

		if nal.nalType == nalSlice || nal.nalType == nalIDR {
			hasSlice = true
		}

		if i == len(starts)-1 && end > unitStart {
			units = append(units, stream[unitStart:end])
		}
	}

	return units
}

func boundary(nalType byte, hasSlice bool) bool {
	switch nalType {
	case nalDelimiter, nalSPS, nalPPS, nalSEI, nalSlice, nalIDR:
		return hasSlice
	default:
		return false
	}
}

type nalStart struct {
	offset  int
	nalType byte
}

func nalStarts(stream []byte) []nalStart {
	var starts []nalStart
	for i := 0; i+3 < len(stream)+1; i++ {
		var codeLen int
		switch {
		case i+3 < len(stream) && stream[i] == 0 && stream[i+1] == 0 && stream[i+2] == 0 && stream[i+3] == 1:
			codeLen = 4
		case i+2 < len(stream) && stream[i] == 0 && stream[i+1] == 0 && stream[i+2] == 1:
			codeLen = 3
		default:
			continue
		}

		header := i + codeLen
		if header+minNALPayload > len(stream) {
			break
		}
		starts = append(starts, nalStart{offset: i, nalType: stream[header] & nalTypeMask})
		i += codeLen - 1
	}
	return starts
}

func PlayAnnexB(ctx context.Context, lc LiveCall, units [][]byte, frameRate int) error {
	if frameRate <= 0 {
		frameRate = defaultVideoFrameRate
	}
	step := time.Second / time.Duration(frameRate)

	ticker := time.NewTicker(step)
	defer ticker.Stop()

	for _, unit := range units {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := lc.SendVideo(unit, step); err != nil {
			return fmt.Errorf("call: send video access unit: %w", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	return nil
}

func (m *Manager) playVideo(
	ctx context.Context,
	t *Tracked,
	cmd amqp.GatewayCallCommand,
	fetch MediaFetcher,
) error {
	if cmd.MediaURL == "" {
		return fetchError{fmt.Errorf("call: video.play requires a mediaUrl")}
	}

	data, err := fetch(ctx, cmd.MediaURL)
	if err != nil {
		return fetchError{fmt.Errorf("call: fetch video media: %w", err)}
	}

	units := SplitAnnexB(data)
	if len(units) == 0 {
		return fetchError{fmt.Errorf("call: %s carries no H.264 access unit", cmd.MediaURL)}
	}

	go func() {
		if err := PlayAnnexB(context.Background(), t.Live, units, defaultVideoFrameRate); err != nil {
			m.log.Error("call: video playback stopped",
				"channel_id", t.ChannelID, "call_id", t.CallID, "error", err)
		}
	}()

	return nil
}
