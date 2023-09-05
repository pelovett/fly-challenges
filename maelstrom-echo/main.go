package main

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func intMapToSlice(aMap map[int]struct{}) []int {
	curMsgs := make([]int, len(aMap))
	i := 0
	for k := range aMap {
		curMsgs[i] = k
		i++
	}
	return curMsgs
}

type NodeState struct {
	msgStore map[int]struct{}
	curHash  string
	topo     map[string][]string
	mu       sync.Mutex
}

func (state *NodeState) getStateArray() []int {
	//fmt.Fprintf(os.Stderr, "Waiting for lock in getStateArray...\n")
	state.mu.Lock()
	//fmt.Fprintf(os.Stderr, "Acquired lock in getStateArray\n")
	defer state.mu.Unlock()

	return intMapToSlice(state.msgStore)
}

func (state *NodeState) getStateHash() string {
	//fmt.Fprintf(os.Stderr, "Waiting for lock in getStateHash...\n")
	state.mu.Lock()
	//fmt.Fprintf(os.Stderr, "Acquired lock in getStateHash\n")
	defer state.mu.Unlock()
	return state.curHash
}

func (state *NodeState) updateState(candidates []int) []int {
	//fmt.Fprintf(os.Stderr, "Waiting for lock in updateState...\n")
	state.mu.Lock()
	//fmt.Fprintf(os.Stderr, "Acquired lock in updateState\n")
	defer state.mu.Unlock()

	// Check if any signals are new
	newSignals := make([]int, 0)
	for _, sig := range candidates {
		_, seenMsg := state.msgStore[sig]
		if !seenMsg {
			newSignals = append(newSignals, sig)
			state.msgStore[sig] = struct{}{}
		}
	}

	// Recalc hash if needed
	if len(newSignals) > 0 {
		msgArr := intMapToSlice(state.msgStore)
		sort.Ints(msgArr)
		md5Hash := md5.New()
		for _, i := range msgArr {
			io.WriteString(md5Hash, strconv.Itoa(i))
		}
		state.curHash = fmt.Sprintf("%x", md5Hash.Sum(nil))
	}

	return newSignals
}

func (state *NodeState) getTopo() map[string][]string {
	//fmt.Fprintf(os.Stderr, "Waiting for lock in getTopo...\n")
	state.mu.Lock()
	//fmt.Fprintf(os.Stderr, "Acquired lock in getTopo\n")
	defer state.mu.Unlock()
	return state.topo
}

func (state *NodeState) updateTopo(newTopo map[string][]string) {
	//fmt.Fprintf(os.Stderr, "Waiting for lock in updateTopo...\n")
	state.mu.Lock()
	//fmt.Fprintf(os.Stderr, "Acquired lock in updateTopo\n")
	defer state.mu.Unlock()
	state.topo = map[string][]string{
		"n0":  {"n1", "n2", "n3", "n4", "n5", "n10", "n15", "n20"},
		"n1":  {"n0", "n2", "n3", "n4"},
		"n2":  {"n0", "n1", "n3", "n4"},
		"n3":  {"n0", "n1", "n2", "n4"},
		"n4":  {"n0", "n1", "n2", "n3"},
		"n5":  {"n6", "n7", "n8", "n9", "n0", "n10", "n15", "n20"},
		"n6":  {"n5", "n7", "n8", "n9"},
		"n7":  {"n5", "n6", "n8", "n9"},
		"n8":  {"n5", "n6", "n7", "n9"},
		"n9":  {"n5", "n6", "n7", "n8"},
		"n10": {"n11", "n12", "n13", "n14", "n0", "n5", "n15", "n20"},
		"n11": {"n10", "n12", "n13", "n14"},
		"n12": {"n10", "n11", "n13", "n14"},
		"n13": {"n10", "n11", "n12", "n14"},
		"n14": {"n10", "n11", "n12", "n13"},
		"n15": {"n16", "n17", "n18", "n19", "n0", "n5", "n10", "n20"},
		"n16": {"n15", "n17", "n18", "n19"},
		"n17": {"n15", "n16", "n18", "n19"},
		"n18": {"n15", "n16", "n17", "n19"},
		"n19": {"n15", "n16", "n17", "n18"},
		"n20": {"n21", "n22", "n23", "n24", "n0", "n5", "n10", "n15"},
		"n21": {"n20", "n22", "n23", "n24"},
		"n22": {"n20", "n21", "n23", "n24"},
		"n23": {"n20", "n21", "n22", "n24"},
		"n24": {"n20", "n21", "n22", "n23"},
	}
}

func broadCastNewMsgs(src string, n *maelstrom.Node, newMsgs []int, stateHash string, dests []string, logger log.Logger) error {
	msgContent := map[string]any{
		"type":      "broadcast",
		"message":   newMsgs[0],
		"stateHash": stateHash,
	}
	if len(newMsgs) > 1 {
		msgContent["otherMessages"] = newMsgs[1:]
	}

	logger.Printf("Broadcasting message: %d to neighbors: %v", newMsgs[0], dests)

	// TODO remove when challenge requires we use given topology
	for _, node := range dests {
		// Don't send signal back to source of signal
		if node == src || node == n.ID() {
			continue
		}

		n.RPC(node, msgContent, func(msg maelstrom.Message) error {
			if msg.Type() != "broadcast_ok" {
				return errors.New("FAILED TO BROADCAST")
			}
			return nil
		})
	}
	return nil
}

func main() {
	n := maelstrom.NewNode()
	logger := log.New(os.Stderr, n.ID(), log.Lmicroseconds)

	nodeState := NodeState{msgStore: make(map[int]struct{}), topo: make(map[string][]string)}

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
		// Unmarshal the message body as an loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if err := n.Reply(msg, map[string]string{
			"type": "broadcast_ok",
		}); err != nil {
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

		// Check if any signals are new
		newSignals := nodeState.updateState(signals)
		msgId := int(body["msg_id"].(float64))
		logger.Printf("msg_id: %d New Signals: %v\n", msgId, newSignals)

		// If they sent a hash, check if it matches our hash
		srcHash, hashPresent := body["stateHash"]
		ourHash := nodeState.getStateHash()

		// Check if we need to sync before broadcasting updates
		if hashPresent {
			logger.Printf("from: %s msg_id: %d Our hash %s their hash %s\n", msg.Src, msgId, ourHash, srcHash.(string))
			// If hashes don't match, trigger sync
			if ourHash != srcHash.(string) {
				ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*3)
				response, err := n.SyncRPC(ctxTimeout, msg.Src, map[string]any{
					"type":     "read",
					"cur_hash": nodeState.getStateHash(),
					"messages": nodeState.getStateArray(),
				})
				cancel()
				if err != nil {
					return err
				}

				// Unmarshal the message body as an loosely-typed map
				var body map[string]any
				if err := json.Unmarshal(response.Body, &body); err != nil {
					return err
				}

				if body["type"].(string) != "read_ok" {
					return errors.New(fmt.Sprintf("Failed to read from node %s", msg.Src))
				}

				// Get slice of new messages and update our stored messages
				parsedSignals := make([]int, 0)
				for _, rawSignal := range body["messages"].([]interface{}) {
					parsedSignals = append(parsedSignals, int(rawSignal.(float64)))
				}

				newMsgs := nodeState.updateState(parsedSignals)
				for _, signal := range newMsgs {
					newSignals = append(newSignals, signal)
				}
			}
		}

		if len(newSignals) < 1 {
			logger.Printf("msg_id: %d No new signals, not broadcasting...\n", msgId)
			return nil
		}

		// Broadcast new signals to all neighbors
		err := broadCastNewMsgs(msg.Src, n, newSignals, ourHash, nodeState.getTopo()[n.ID()], *logger)
		if err != nil {
			return err
		}
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

		// Unmarshal the message body as an loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Check if read from other node and if this was secondary read
		response, keyPresent := body["read_response"]
		otherHash, hashPresent := body["cur_hash"]
		messages, _ := body["messages"]
		if !keyPresent {
			return nil
		} else if response.(string) == "Y" {
			logger.Printf("from: %s msg_id: %d No need to create read, second read msg\n", msg.Src, int(body["msg_id"].(float64)))
			return nil
		} else if hashPresent && otherHash == nodeState.getStateHash() {
			logger.Printf("from: %s msg_id: %d No need to create read, matching state\n", msg.Src, int(body["msg_id"].(float64)))
			return nil
		}

		parsedMessages := make([]int, 0)
		for _, message := range messages.([]interface{}) {
			parsedMessages = append(parsedMessages, int(message.(float64)))
		}
		newSignals := nodeState.updateState(parsedMessages)

		// If no new signals, we're done
		if len(newSignals) < 1 {
			return nil
		}

		// Otherwise calculate hash and broadcast new signals
		ourHash := nodeState.getStateHash()

		// Broadcast new signals to all neighbors
		if err := broadCastNewMsgs(msg.Src, n, newSignals, ourHash, nodeState.getTopo()[n.ID()], *logger); err != nil {
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

		logger.Println(fmt.Sprintf("My neighbors: %v", nodeState.getTopo()[n.ID()]))

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
