package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// SAD HELPER FUNCTION BOX
func sumSlice(aSlice []int) int {
	i := 0
	for _, val := range aSlice {
		i += val
	}
	return i
}

//

type NodeState struct {
	lastValue   int
	updateQueue []int
	topo        map[string][]string
	mu          sync.Mutex
}

func (state *NodeState) getValue() int {
	//fmt.Fprintf(os.Stderr, "Waiting for lock in getStateArray...\n")
	state.mu.Lock()
	//fmt.Fprintf(os.Stderr, "Acquired lock in getStateArray\n")
	defer state.mu.Unlock()

	return state.lastValue + sumSlice(state.updateQueue)
}

func (state *NodeState) setLastValue(newLastValue int) {
	//fmt.Fprintf(os.Stderr, "Waiting for lock in updateState...\n")
	state.mu.Lock()
	//fmt.Fprintf(os.Stderr, "Acquired lock in updateState\n")
	defer state.mu.Unlock()

	state.lastValue = newLastValue
}

func (state *NodeState) startUpdate(newVal int) {
	//fmt.Fprintf(os.Stderr, "Waiting for lock in updateState...\n")
	state.mu.Lock()
	//fmt.Fprintf(os.Stderr, "Acquired lock in updateState\n")
	defer state.mu.Unlock()

	state.updateQueue = append(state.updateQueue, newVal)
}

func (state *NodeState) finishUpdate(removeUpTo, newVal int) {
	//fmt.Fprintf(os.Stderr, "Waiting for lock in updateState...\n")
	state.mu.Lock()
	//fmt.Fprintf(os.Stderr, "Acquired lock in updateState\n")
	defer state.mu.Unlock()

	if len(state.updateQueue) <= removeUpTo {
		state.updateQueue = make([]int, 0)
	} else {
		state.updateQueue = state.updateQueue[removeUpTo:]
	}
	state.lastValue = newVal
}

func (state *NodeState) getUpdates() []int {
	//fmt.Fprintf(os.Stderr, "Waiting for lock in updateState...\n")
	state.mu.Lock()
	//fmt.Fprintf(os.Stderr, "Acquired lock in updateState\n")
	defer state.mu.Unlock()

	return state.updateQueue
}

func (state *NodeState) getTopo() map[string][]string {
	//fmt.Fprintf(os.Stderr, "Waiting for lock in updateState...\n")
	state.mu.Lock()
	//fmt.Fprintf(os.Stderr, "Acquired lock in updateState\n")
	defer state.mu.Unlock()

	return state.topo
}

func (state *NodeState) updateTopo(newTopo map[string][]string) {
	//fmt.Fprintf(os.Stderr, "Waiting for lock in updateTopo...\n")
	state.mu.Lock()
	//fmt.Fprintf(os.Stderr, "Acquired lock in updateTopo\n")
	defer state.mu.Unlock()
	state.topo = newTopo
}

func tryIncrement(kvStore *maelstrom.KV, state *NodeState, updateLen, lastVal, delta int) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := kvStore.CompareAndSwap(ctx, "counter", lastVal, lastVal+delta, true)
	if err != nil {
		return
	}

	state.finishUpdate(updateLen, lastVal+delta)
}

func getCounterValue(kvStore *maelstrom.KV, state *NodeState) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	value, err := kvStore.ReadInt(ctx, "counter")
	if err != nil {
		return 0, err
	}
	state.setLastValue(value)
	return value, nil
}

func timeLoop(n *maelstrom.Node, state *NodeState, keyStore *maelstrom.KV, quit chan struct{}, timer *time.Ticker) {
	for {
		select {
		case <-timer.C:
			// Get current counter value
			curVal, _ := getCounterValue(keyStore, state)

			// Check if we need to update kv
			updateQueue := state.getUpdates()

			if len(updateQueue) == 0 {
				continue
			}

			// Update counter
			tryIncrement(keyStore, state, len(updateQueue), curVal, sumSlice(updateQueue))
		case <-quit:
			return
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func main() {
	n := maelstrom.NewNode()

	kv := maelstrom.NewSeqKV(n)
	logger := log.New(os.Stderr, n.ID(), log.Lmicroseconds)

	nodeState := NodeState{topo: make(map[string][]string), updateQueue: make([]int, 0), lastValue: 0}

	n.Handle("echo", func(msg maelstrom.Message) error {
		// Unmarchal the message body as an loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "echo_ok"

		// Echo the original message back with the updated message
		return n.Reply(msg, body)
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		if err := n.Reply(msg, map[string]string{
			"type": "add_ok",
		}); err != nil {
			return err
		}

		// Unmarshal the message body as an loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))
		nodeState.startUpdate(delta)

		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Send value back
		if err := n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": nodeState.getValue(),
		}); err != nil {
			return err
		}
		return nil
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		newTopo := make(map[string][]string)
		rawTopo := body["topology"].(map[string]interface{})
		for k := range rawTopo {
			var neighbors []string
			for _, face := range rawTopo[k].([]interface{}) {
				neighbors = append(neighbors, face.(string))
			}
			newTopo[k] = neighbors
		}
		nodeState.updateTopo(newTopo)

		logger.Printf("My neighbors: %v\n", nodeState.getTopo()[n.ID()])

		// Send message back
		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	logger.Printf("Starting up worker %s !!!\n", n.ID())
	ticker := time.NewTicker(50 * time.Millisecond)
	quit := make(chan struct{})

	go timeLoop(n, &nodeState, kv, quit, ticker)

	if err := n.Run(); err != nil {
		quit <- struct{}{}

		log.Fatal(err)

	}
}
