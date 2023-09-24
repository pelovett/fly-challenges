package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// SAD HELPER FUNCTION BOX
func max(x, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}

//

type NodeState struct {
	msgStore    []int
	topo        map[string][]string
	neighbState map[string]int
	mu          sync.Mutex
}

func (state *NodeState) getStateArray() []int {
	//fmt.Fprintf(os.Stderr, "Waiting for lock in getStateArray...\n")
	state.mu.Lock()
	//fmt.Fprintf(os.Stderr, "Acquired lock in getStateArray\n")
	defer state.mu.Unlock()

	return state.msgStore
}

func (state *NodeState) updateState(candidates []int) bool {
	//fmt.Fprintf(os.Stderr, "Waiting for lock in updateState...\n")
	state.mu.Lock()
	//fmt.Fprintf(os.Stderr, "Acquired lock in updateState\n")
	defer state.mu.Unlock()

	// Check if any signals are new
	newSignals := make([]int, 0)
	for _, sig := range candidates {
		seenSig := false
		for _, knownSig := range state.msgStore {
			if knownSig == sig {
				seenSig = true
				break
			}
		}
		if !seenSig {
			newSignals = append(newSignals, sig)
			state.msgStore = append(state.msgStore, sig)
		}
	}

	return len(newSignals) > 0
}

func (state *NodeState) getTopo() map[string][]string {
	//fmt.Fprintf(os.Stderr, "Waiting for lock in getTopo...\n")
	state.mu.Lock()
	//fmt.Fprintf(os.Stderr, "Acquired lock in getTopo\n")
	defer state.mu.Unlock()
	return state.topo
}

// func (state *NodeState) getUpdateMap(src string) (map[string][]int, int) {
// 	state.mu.Lock()
// 	defer state.mu.Unlock()
// 	updateMap := make(map[string][]int)
// 	newLastProcessed := len(state.msgStore) - 1

// 	for _, node := range state.topo[src] {
// 		lastProcessed, statePresent := state.neighbState[node]
// 		if !statePresent {
// 			updateMap[node] = state.msgStore[:]
// 		} else if lastProcessed < newLastProcessed {
// 			updateMap[node] = state.msgStore[lastProcessed:]
// 		}
// 	}

// 	return updateMap, newLastProcessed
// }

// func (state *NodeState) getSingleUpdateMap(node string) ([]int, int) {
// 	state.mu.Lock()
// 	defer state.mu.Unlock()
// 	var updateSignals []int
// 	newLastProcessed := len(state.msgStore) - 1

// 	lastProcessed, statePresent := state.neighbState[node]
// 	if !statePresent {
// 		updateSignals = state.msgStore[:]
// 	} else if lastProcessed < newLastProcessed {
// 		updateSignals = state.msgStore[lastProcessed:]
// 	}

// 	return updateSignals, newLastProcessed
// }

// func (state *NodeState) updateNeighbState(node string, newLastProcessed int) {
// 	state.mu.Lock()
// 	defer state.mu.Unlock()

// 	oldProcessed, statePresent := state.neighbState[node]
// 	if statePresent {
// 		state.neighbState[node] = max(newLastProcessed, oldProcessed)
// 	} else {
// 		state.neighbState[node] = newLastProcessed
// 	}
// }

func (state *NodeState) updateTopo(newTopo map[string][]string) {
	//fmt.Fprintf(os.Stderr, "Waiting for lock in updateTopo...\n")
	state.mu.Lock()
	//fmt.Fprintf(os.Stderr, "Acquired lock in updateTopo\n")
	defer state.mu.Unlock()
	//state.topo = newTopo

	// state.topo = map[string][]string{
	// 	"n0":  {"n1", "n2", "n3", "n4", "n5", "n10", "n15", "n20"},
	// 	"n1":  {"n0", "n2", "n3", "n4"},
	// 	"n2":  {"n0", "n1", "n3", "n4"},
	// 	"n3":  {"n0", "n1", "n2", "n4"},
	// 	"n4":  {"n0", "n1", "n2", "n3"},
	// 	"n5":  {"n6", "n7", "n8", "n9", "n0", "n10", "n15", "n20"},
	// 	"n6":  {"n5", "n7", "n8", "n9"},
	// 	"n7":  {"n5", "n6", "n8", "n9"},
	// 	"n8":  {"n5", "n6", "n7", "n9"},
	// 	"n9":  {"n5", "n6", "n7", "n8"},
	// 	"n10": {"n11", "n12", "n13", "n14", "n0", "n5", "n15", "n20"},
	// 	"n11": {"n10", "n12", "n13", "n14"},
	// 	"n12": {"n10", "n11", "n13", "n14"},
	// 	"n13": {"n10", "n11", "n12", "n14"},
	// 	"n14": {"n10", "n11", "n12", "n13"},
	// 	"n15": {"n16", "n17", "n18", "n19", "n0", "n5", "n10", "n20"},
	// 	"n16": {"n15", "n17", "n18", "n19"},
	// 	"n17": {"n15", "n16", "n18", "n19"},
	// 	"n18": {"n15", "n16", "n17", "n19"},
	// 	"n19": {"n15", "n16", "n17", "n18"},
	// 	"n20": {"n21", "n22", "n23", "n24", "n0", "n5", "n10", "n15"},
	// 	"n21": {"n20", "n22", "n23", "n24"},
	// 	"n22": {"n20", "n21", "n23", "n24"},
	// 	"n23": {"n20", "n21", "n22", "n24"},
	// 	"n24": {"n20", "n21", "n22", "n23"},
	// }

	// state.topo = map[string][]string{
	// 	"n0":  {"n1", "n2", "n3", "n4", "n5", "n20"},
	// 	"n1":  {"n0"},
	// 	"n2":  {"n0"},
	// 	"n3":  {"n0"},
	// 	"n4":  {"n0"},
	// 	"n5":  {"n6", "n7", "n8", "n9", "n0", "n10"},
	// 	"n6":  {"n5"},
	// 	"n7":  {"n5"},
	// 	"n8":  {"n5"},
	// 	"n9":  {"n5"},
	// 	"n10": {"n11", "n12", "n13", "n14", "n5", "n15"},
	// 	"n11": {"n10"},
	// 	"n12": {"n10"},
	// 	"n13": {"n10"},
	// 	"n14": {"n10"},
	// 	"n15": {"n16", "n17", "n18", "n19", "n10", "n20"},
	// 	"n16": {"n15"},
	// 	"n17": {"n15"},
	// 	"n18": {"n15"},
	// 	"n19": {"n15"},
	// 	"n20": {"n21", "n22", "n23", "n24", "n0", "n15"},
	// 	"n21": {"n20"},
	// 	"n22": {"n20"},
	// 	"n23": {"n20"},
	// 	"n24": {"n20"},
	// }

	state.topo = map[string][]string{
		"n0":  {"n5", "n10", "n15", "n20"},
		"n1":  {"n5"},
		"n2":  {"n5"},
		"n3":  {"n5"},
		"n4":  {"n5"},
		"n5":  {"n0", "n1", "n2", "n3", "n4", "n6"},
		"n6":  {"n5"},
		"n7":  {"n10"},
		"n8":  {"n10"},
		"n9":  {"n10"},
		"n10": {"n0", "n7", "n8", "n9", "n11", "n12"},
		"n11": {"n10"},
		"n12": {"n10"},
		"n13": {"n15"},
		"n14": {"n15"},
		"n15": {"n0", "n13", "n14", "n16", "n17", "n18"},
		"n16": {"n15"},
		"n17": {"n15"},
		"n18": {"n15"},
		"n19": {"n20"},
		"n20": {"n0", "n19", "n21", "n22", "n23", "n24"},
		"n21": {"n20"},
		"n22": {"n20"},
		"n23": {"n20"},
		"n24": {"n20"},
	}

	// state.topo = map[string][]string{
	// 	"n0": {"n1"},
	// 	"n1": {"n0", "n2"},
	// 	"n2": {"n1", "n3"},
	// 	"n3": {"n2", "n4"},
	// 	"n4": {"n3"},
	// }

}

func timeLoop(n *maelstrom.Node, state *NodeState, quit chan struct{}, timer *time.Ticker) {
	for {
		select {
		case <-timer.C:
			heartbeat(n, state)
		case <-quit:
			return
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func heartbeat(n *maelstrom.Node, state *NodeState) {
	curMsgs := state.getStateArray()
	if len(curMsgs) < 1 {
		return
	}

	message := map[string]any{"type": "broadcast",
		"message": curMsgs[0],
	}
	if len(curMsgs) > 1 {
		message["otherMessages"] = curMsgs[1:]
	}
	// Send echo to each neighbor
	for _, node := range state.topo[n.ID()] {
		n.Send(node, message)
	}
}

// func sendBroadcast(node string, n *maelstrom.Node, state *NodeState, signals []int, newLastProcessed int) {
// 	msgContent := map[string]any{
// 		"type":    "broadcast",
// 		"message": signals[0],
// 	}
// 	if len(signals) > 1 {
// 		msgContent["otherMessages"] = signals[1:]
// 	}

// 	n.RPC(node, msgContent, func(msg maelstrom.Message) error {
// 		if msg.Type() != "broadcast_ok" {
// 			return errors.New("FAILED TO BROADCAST")
// 		}
// 		state.updateNeighbState(node, newLastProcessed)
// 		return nil
// 	})
// }

// func broadCastNewMsgs(src string, n *maelstrom.Node, state *NodeState, logger *log.Logger) {

// 	updateMap, newLastProcessed := state.getUpdateMap(n.ID())

// 	for node, updateSignals := range updateMap {
// 		if node == src || node == n.ID() {
// 			continue
// 		}
// 		sendBroadcast(node, n, state, updateSignals, newLastProcessed)
// 	}
// }

func main() {
	n := maelstrom.NewNode()
	logger := log.New(os.Stderr, n.ID(), log.Lmicroseconds)

	nodeState := NodeState{msgStore: make([]int, 0), topo: make(map[string][]string), neighbState: make(map[string]int, 0)}

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

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		if err := n.Reply(msg, map[string]string{
			"type": "broadcast_ok",
		}); err != nil {
			return err
		}

		// Unmarshal the message body as an loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Grab all signals contained in message
		signals := make([]int, 0)
		signals = append(signals, int(body["message"].(float64)))
		otherMsgs, otherMsgsPresent := body["otherMessages"].([]interface{})
		if otherMsgsPresent {
			for _, other := range otherMsgs {
				signals = append(signals, int(other.(float64)))
			}
		}

		// Update state
		nodeState.updateState(signals)
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		curMsgs := nodeState.getStateArray()

		// Send message back
		if err := n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": curMsgs,
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

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	logger.Printf("Starting up worker %s !!!\n", n.ID())
	ticker := time.NewTicker(100 * time.Millisecond)
	quit := make(chan struct{})

	go timeLoop(n, &nodeState, quit, ticker)

	if err := n.Run(); err != nil {
		quit <- struct{}{}
		log.Fatal(err)

	}
}
