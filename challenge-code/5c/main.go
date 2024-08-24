package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

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
	msgs          map[string][]Message
	potentialMsgs map[string][]Message
	commited      map[string]int
	lastOffset    map[string]int
	kv            *maelstrom.KV
	mu            sync.Mutex
	log           *log.Logger
}

type Message struct {
	offset int
	value  int
}

func (state *NodeState) getOffset(topic string) (int, error) {

	// Try to reserve next offset
	attempts := 0
	for attempts < 5 {
		state.mu.Lock()
		currentVal := state.lastOffset[topic]
		state.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Get current max offset
		err := state.kv.CompareAndSwap(
			ctx,
			topic,
			currentVal,
			currentVal+int(math.Pow(2, float64(attempts))), // Exponential backoff
			true,
		)
		if err != nil {
			if rpcErr, ok := err.(*maelstrom.RPCError); ok {
				switch rpcErr.Code {
				case maelstrom.PreconditionFailed:
					// Try to parse error text and grab current kv value
					substrings := strings.Split(rpcErr.Text, " ")
					if len(substrings) > 3 {
						newVal, err := strconv.Atoi(substrings[2])
						if err == nil {
							state.mu.Lock()
							state.lastOffset[topic] = newVal
							state.mu.Unlock()
						} else {
							state.log.Printf("failed to parse string: '%s' with value [2] as: %s", rpcErr.Text, substrings[2])
						}
					}
					attempts += 1
					continue
				default:
					return -1, rpcErr
				}
			} else {
				return -1, err
			}
		} else {
			state.mu.Lock()
			state.lastOffset[topic] = state.lastOffset[topic] + int(math.Pow(2, float64(attempts)))
			state.mu.Unlock()

			return state.lastOffset[topic], nil
		}
	}
	return -1, fmt.Errorf("failed to get offset after 5 attempts")
}

func (state *NodeState) promoteMessage(topic string, offset int) {
	state.mu.Lock()
	defer state.mu.Unlock()

	// Remove from potential messages
	curMsgs, present := state.potentialMsgs[topic]
	if !present {
		panic(fmt.Sprintf(
			"Missing topic when promoting potential messages: %s offset: %d",
			topic,
			offset,
		))
	}

	// Find index of message
	msgIdx := -1
	var ourMsg Message
	for idx, aMsg := range curMsgs {
		if aMsg.offset == offset {
			msgIdx = idx
			ourMsg = aMsg
			break
		}
	}
	if msgIdx == -1 {
		panic(fmt.Sprintf(
			"Missing message when promoting potential messages: %v offset: %d",
			curMsgs,
			offset,
		))
	}

	// Remove from potential messages
	if msgIdx == 0 {
		state.potentialMsgs[topic] = curMsgs[1:]
	} else if msgIdx == len(curMsgs)-1 {
		state.potentialMsgs[topic] = curMsgs[:len(curMsgs)-1]
	} else {
		state.potentialMsgs[topic] = append(curMsgs[:msgIdx], curMsgs[msgIdx+1:]...)
	}

	// Add to promoted messages
	curMsgs, present = state.msgs[topic]
	if !present {
		state.msgs[topic] = []Message{ourMsg}
	} else {
		state.msgs[topic] = append(curMsgs, ourMsg)
	}

	// Sort promoted messages
	slices.SortFunc(state.msgs[topic], func(a, b Message) int {
		return cmp.Compare(a.offset, b.offset)
	})
}

func (state *NodeState) savePotential(topic string, offset int, msg int) {
	state.mu.Lock()
	defer state.mu.Unlock()

	curMsgs, present := state.msgs[topic]
	if !present {
		state.msgs[topic] = []Message{{offset, msg}}
		state.lastOffset[topic] = offset
		return
	}

	// Check if message already exists
	msgIdx := -1
	for idx, aMsg := range curMsgs {
		if aMsg.offset == offset {
			msgIdx = idx
			break
		}
	}
	// Message already exists, do nothing
	if msgIdx != -1 {
		return
	}

	state.msgs[topic] = append(curMsgs, Message{offset, msg})

	// Update seen offset map
	state.lastOffset[topic] = max(state.lastOffset[topic], offset)
}

func (state *NodeState) startPublish(n *maelstrom.Node, topic string, msg int) (int, error) {
	offset, err := state.getOffset(topic)
	if err != nil {
		return -1, err
	}

	// Save to our in progress nodes
	state.mu.Lock()
	curMsgs, present := state.potentialMsgs[topic]
	if present {
		state.potentialMsgs[topic] = append(curMsgs, Message{offset, msg})
	} else {
		state.potentialMsgs[topic] = []Message{{offset, msg}}
	}
	state.mu.Unlock()

	// Get consensus on our proposed message
	done := make(chan bool)
	errChan := make(chan error)
	for _, node := range n.NodeIDs() {
		if node == n.ID() {
			continue
		}
		n.RPC(
			node,
			map[string]any{"type": "send_potential", "key": topic, "msg": msg, "offset": offset},
			func(msg maelstrom.Message) error {
				var err error
				if msg.Type() != "send_potential_ok" {
					err = errors.New("error failed to send potential")
					errChan <- err
				}
				done <- true
				return err
			})
	}
	for i := 0; i < len(n.NodeIDs())-1; i++ {
		<-done
	}
	select {
	case err := <-errChan:
		return -1, err
	default:
		// Continue execution
	}

	// Promote potential message to real message
	state.promoteMessage(topic, offset)

	return offset, nil
}

func (state *NodeState) getMsgMap(offsets map[string]int) map[string][][]int {
	state.mu.Lock()
	defer state.mu.Unlock()

	msgsToReturn := make(map[string][][]int)
	for topic, offset := range offsets {
		if msgs, present := state.msgs[topic]; present {
			seenMsgs := 0
			for _, msg := range msgs {
				if msg.offset >= offset {
					break
				}
				seenMsgs += 1
			}

			if seenMsgs <= len(msgs) {
				for _, msg := range msgs[seenMsgs:] {
					msgsToReturn[topic] = append(
						msgsToReturn[topic],
						[]int{msg.offset, msg.value},
					)
				}
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

	kv := maelstrom.NewLinKV(n)

	nodeState := NodeState{
		msgs:          make(map[string][]Message),
		potentialMsgs: make(map[string][]Message),
		commited:      make(map[string]int),
		lastOffset:    make(map[string]int),
		kv:            kv,
		log:           logger,
	}

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
		offset, err := nodeState.startPublish(n, key, message)
		if err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": offset,
		})
	})

	n.Handle("send_potential", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key := body["key"].(string)
		message := int(body["msg"].(float64))
		offset := int(body["offset"].(float64))
		nodeState.savePotential(key, offset, message)

		return n.Reply(msg, map[string]any{"type": "send_potential_ok"})
	})

	n.Handle("promote_potential", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key := body["key"].(string)
		offset := int(body["offset"].(float64))
		nodeState.promoteMessage(key, offset)

		return n.Reply(msg, map[string]any{"type": "promote_potential_ok"})
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

	logger.Printf("Starting up worker %s !!!\n", n.ID())

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
