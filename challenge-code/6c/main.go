package main

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func convertRawTxns(raw_txns []any) []Op {
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
	return txns
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

func (node *NodeState) saveTxns(ops []Op) []any {
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

func (node *NodeState) shareTxns(n *maelstrom.Node, ops []Op) error {

	// Remove reads from messages
	var sharedTxns []any
	for _, op := range ops {
		if op.opType == "w" {
			sharedTxns = append(sharedTxns, []any{op.opType, op.key, op.val})
		}
	}
	// If only reads then stop
	if len(sharedTxns) == 0 {
		return nil
	}

	// Share messages
	done := make(chan bool)
	errChan := make(chan error)
	for _, node := range n.NodeIDs() {
		if node == n.ID() {
			continue
		}
		n.RPC(
			node,
			map[string]any{"type": "share_txn", "txn": sharedTxns},
			func(msg maelstrom.Message) error {
				var err error
				if msg.Type() != "share_txn_ok" {
					err = errors.New("error failed to share txns")
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
		return err
	default:
		return nil
	}
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

		txns := convertRawTxns(body["txn"].([]any))
		result := nodeState.saveTxns(txns)
		err := n.Reply(msg, map[string]any{
			"type": "txn_ok",
			"txn":  result,
		})

		// Share with other nodes
		nodeState.shareTxns(n, txns)

		return err
	})

	n.Handle("share_txn", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		raw_txns := body["txn"].([]any)
		nodeState.saveTxns(convertRawTxns(raw_txns))

		return n.Reply(msg, map[string]any{"type": "share_txn_ok"})
	})

	logger.Printf("Starting up worker %s !!!\n", n.ID())

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
