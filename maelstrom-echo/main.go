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
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func getMsgArr(msgMap *map[int]struct{}) []int {
	curMsgs := make([]int, len(*msgMap))
	i := 0
	for k := range *msgMap {
		curMsgs[i] = k
		i++
	}
	return curMsgs
}

func broadCastNewMsgs(src string, n *maelstrom.Node, newMsgs []int, stateHash string, dests []string) error {
	msgContent := map[string]any{
		"type":    "broadcast",
		"message": newMsgs[0],
		"body": map[string]any{
			"stateHash": stateHash,
		},
	}
	if len(newMsgs) > 1 {
		msgContent["body"].(map[string]any)["otherMessages"] = newMsgs[1:]
	}

	for _, node := range dests {
		// Don't send signal back to source of signal
		if node == src {
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

// This function requests the full state from a node then records & returns any messages that are novel
func checkOtherState(syncTarget string, n *maelstrom.Node, msgStoreAddr *map[int]struct{}, readResponse string) ([]int, error) {
	ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	response, err := n.SyncRPC(ctxTimeout, syncTarget, map[string]string{
		"type":          "read",
		"read_response": readResponse,
	})
	if err != nil {
		return nil, err
	}

	// Unmarshal the message body as an loosely-typed map
	var body map[string]any
	if err := json.Unmarshal(response.Body, &body); err != nil {
		return nil, err
	}

	if body["type"].(string) != "read_ok" {
		return nil, errors.New(fmt.Sprintf("Failed to read from node %s", syncTarget))
	}

	// Get slice of new messages and update our stored messages
	newMessages := make([]int, 0)
	for _, signal := range body["messages"].([]int) {
		_, seenBefore := (*msgStoreAddr)[signal]
		if !seenBefore {
			newMessages = append(newMessages, signal)
			(*msgStoreAddr)[signal] = struct{}{}
		}
	}

	if len(newMessages) > 0 {
		return newMessages, nil
	} else {
		return nil, nil
	}
}

func hashState(state *map[int]struct{}) string {
	// Hash our current state
	msgArr := getMsgArr(state)
	sort.Ints(msgArr)
	md5Hash := md5.New()
	for _, i := range msgArr {
		io.WriteString(md5Hash, strconv.Itoa(i))
	}
	return fmt.Sprintf("%x", md5Hash.Sum(nil))
}

func main() {
	n := maelstrom.NewNode()
	logger := log.New(os.Stderr, n.ID(), log.Lmicroseconds)

	msgStore := make(map[int]struct{})
	topo := make(map[string][]string)

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
		otherMsgs, otherMsgsPresent := body["otherMessages"].([]int)
		if otherMsgsPresent {
			for _, other := range otherMsgs {
				signals = append(signals, other)
			}
		}

		// Check if any signals are new
		newSignals := make([]int, 0)
		for _, sig := range signals {
			_, seenMsg := msgStore[sig]
			if !seenMsg {
				newSignals = append(newSignals, sig)
				msgStore[sig] = struct{}{}
			}
		}

		// If they sent a hash, check if it matches our hash
		srcHash, hashPresent := body["stateHash"]
		ourHash := hashState(&msgStore)

		// If no hash and we've seen all signals in cur message, then we're done
		if !hashPresent && len(newSignals) < 1 {
			return nil
		}

		// Check if we need to sync before broadcasting updates
		if hashPresent {
			// If hashes don't match, trigger sync
			if ourHash != srcHash.(string) {
				var err error
				alsoNewSignals, err := checkOtherState(msg.Src, n, &msgStore, "N")
				if err != nil {
					logger.Println("Hit an error when syncing")
				}

				// Add new signals from sync to our slice for sharing
				if len(alsoNewSignals) > 0 {
					for _, signal := range alsoNewSignals {
						newSignals = append(newSignals, signal)
					}

					// Recalculate hash
					ourHash = hashState(&msgStore)
				}
			}
		}

		// Broadcast new signals to all neighbors
		if err := broadCastNewMsgs(msg.Src, n, newSignals, ourHash, topo[n.ID()]); err != nil {
			return err
		}
		return nil

	})

	n.Handle("read", func(msg maelstrom.Message) error {
		curMsgs := getMsgArr(&msgStore)

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
		if !keyPresent {
			return nil
		} else if response == 'Y' {
			return nil
		}

		// If fresh read, we should read from them too, but set response to Y to stop infinite loop
		var err error
		newSignals, err := checkOtherState(msg.Src, n, &msgStore, "Y")
		if err != nil {
			logger.Println("Hit an error when syncing")
		}

		// If no new signals, we're done
		if len(newSignals) < 1 {
			return nil
		}

		// Otherwise calculate hash and broadcast new signals
		ourHash := hashState(&msgStore)

		// Broadcast new signals to all neighbors
		if err := broadCastNewMsgs(msg.Src, n, newSignals, ourHash, topo[n.ID()]); err != nil {
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

		rawTopo := body["topology"].(map[string]interface{})
		newTopo := make(map[string][]string)
		for k := range rawTopo {
			var neighbors []string
			for _, face := range rawTopo[k].([]interface{}) {
				neighbors = append(neighbors, face.(string))
			}
			newTopo[k] = neighbors
		}
		topo = newTopo

		logger.Println(fmt.Sprintf("My neighbors: %v", topo[n.ID()]))

		// Send message back
		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
