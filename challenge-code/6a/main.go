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
	state map[int]int
	kv    *maelstrom.KV
	mu    sync.Mutex
	log   *log.Logger
}

type Op struct {
	opType  string
	key     int
	val     int
	valNull bool
}

func (node *NodeState) saveTxn(ops []Op) []any {
	node.mu.Lock()
	defer node.mu.Unlock()

	var result []any
	for _, op := range ops {
		if op.opType == "r" {
			val, present := node.state[op.key]
			if !present {
				result = append(result, []any{op.opType, op.key, nil})
			} else {
				result = append(result, []any{op.opType, op.key, val})
			}
		} else if op.opType == "w" {
			node.state[op.key] = op.val
			result = append(result, []any{op.opType, op.key, op.val})
		}
	}
	return result
}

func main() {
	n := maelstrom.NewNode()

	logger := log.New(os.Stderr, n.ID(), log.Lmicroseconds)

	kv := maelstrom.NewLinKV(n)

	nodeState := NodeState{
		state: make(map[int]int),
		kv:    kv,
		log:   logger,
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

	n.Handle("txn", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		raw_txns := body["txn"].([]any)
		var txns []Op
		for _, raw_txn := range raw_txns {
			txn := raw_txn.([]any)
			if txn[2] == nil {
				txns = append(txns, Op{
					txn[0].(string),
					int(txn[1].(float64)),
					0,
					true,
				})
			} else {
				txns = append(
					txns,
					Op{
						txn[0].(string),
						int(txn[1].(float64)),
						int(txn[2].(float64)),
						false,
					},
				)
			}
		}
		result := nodeState.saveTxn(txns)

		return n.Reply(msg, map[string]any{
			"type": "txn_ok",
			"txn":  result,
		})
	})

	logger.Printf("Starting up worker %s !!!\n", n.ID())

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
