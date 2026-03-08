package snowflake

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	nodeIDBits    uint8 = 10
	sequenceBits  uint8 = 12
	maxNodeID           = int64(1<<nodeIDBits - 1)
	maxSequence         = int64(1<<sequenceBits - 1)
	timeShift           = nodeIDBits + sequenceBits
	nodeIDShift         = sequenceBits
	maxRollbackMs int64 = 5
)

var epochMs = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

type Generator struct {
	mu            sync.Mutex
	nodeID        int64
	lastTimestamp int64
	sequence      int64
}

func NewFromEnv() (*Generator, error) {
	raw := os.Getenv("NODE_ID")
	if raw == "" {
		return nil, fmt.Errorf("NODE_ID is required")
	}

	nodeID, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid NODE_ID: %w", err)
	}
	if nodeID < 0 || nodeID > maxNodeID {
		return nil, fmt.Errorf("NODE_ID out of range: %d", nodeID)
	}

	return &Generator{nodeID: nodeID}, nil
}

func (g *Generator) NextID() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now().UnixMilli()
	if now < g.lastTimestamp {
		delta := g.lastTimestamp - now
		if delta > maxRollbackMs {
			panic(fmt.Sprintf("clock moved backwards by %dms", delta))
		}
		time.Sleep(time.Duration(delta) * time.Millisecond)
		now = time.Now().UnixMilli()
	}

	if now == g.lastTimestamp {
		g.sequence = (g.sequence + 1) & maxSequence
		if g.sequence == 0 {
			for now <= g.lastTimestamp {
				now = time.Now().UnixMilli()
			}
		}
	} else {
		g.sequence = 0
	}

	g.lastTimestamp = now
	timestamp := now - epochMs
	return (timestamp << timeShift) | (g.nodeID << nodeIDShift) | g.sequence
}
