package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func convertMap(original map[string]any) map[string]int {
	result := make(map[string]int)
	for key, value := range original {
		result[key] = int(value.(float64))
	}
	return result
}

// Node state functions

type NodeState struct {
	msgs          map[string][][]int
	potentialMsgs map[string][][]int
	commited      map[string]int
	topo          map[string][]string
	mu            sync.Mutex
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

func (state *NodeState) startPublish(topic string, msg int) int {
	state.mu.Lock()
	// If we already have messages
	// if inProgMsgs, present := state.potentialMsgs[topic]; present {

	// }
	maxOffset := 0
	curMsgs, present := state.msgs[topic]
	if present {
		if len(curMsgs) > 0 {
			maxOffset = curMsgs[len(curMsgs)-1][0]
		}
		state.msgs[topic] = append(curMsgs, []int{maxOffset + 1, msg})
	} else {
		state.msgs[topic] = [][]int{{maxOffset + 1, msg}}
	}
	state.mu.Unlock()

	// Get consensus on msg

	return maxOffset + 1
}

func (state *NodeState) getMsgMap(offsets map[string]int) map[string][][]int {
	state.mu.Lock()
	defer state.mu.Unlock()

	msgsToReturn := make(map[string][][]int)
	for topic, offset := range offsets {
		if msgs, present := state.msgs[topic]; present {
			seenMsgs := 0
			for _, msg := range msgs {
				if msg[0] >= offset {
					break
				}
				seenMsgs += 1
			}

			if seenMsgs <= len(msgs) {
				msgsToReturn[topic] = msgs[seenMsgs:]
			}
		}
	}

	return msgsToReturn
}

func (state *NodeState) commitOffsets(offsets map[string]int) {
	state.mu.Lock()
	defer state.mu.Unlock()

	for topic, offset := range offsets {
		curOffset, present := state.commited[topic]
		if !present || (present && curOffset < offset) {
			state.commited[topic] = offset
		}
	}
}

func (state *NodeState) getOffsets(keys []string) map[string]int {
	state.mu.Lock()
	defer state.mu.Unlock()

	outputMap := make(map[string]int)
	for _, key := range keys {
		commit, present := state.commited[key]
		if present {
			outputMap[key] = commit
		}
	}

	return outputMap
}

func main() {
	n := maelstrom.NewNode()

	logger := log.New(os.Stderr, n.ID(), log.Lmicroseconds)

	nodeState := NodeState{msgs: make(map[string][][]int),
		potentialMsgs: make(map[string][][]int),
		commited:      make(map[string]int),
		topo:          make(map[string][]string)}

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

	n.Handle("send", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key := body["key"].(string)
		message := int(body["msg"].(float64))
		offset := nodeState.startPublish(key, message)

		return n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": offset,
		})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsetMap := convertMap(body["offsets"].(map[string]any))
		return n.Reply(msg,
			map[string]any{
				"type": "poll_ok",
				"msgs": nodeState.getMsgMap(offsetMap)},
		)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offsets := convertMap(body["offsets"].(map[string]any))
		nodeState.commitOffsets(offsets)

		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		keys := make([]string, len(body["keys"].([]any)))
		for _, rawKey := range body["keys"].([]any) {
			keys = append(keys, rawKey.(string))
		}

		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok",
			"offsets": nodeState.getOffsets(keys),
		})
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

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
