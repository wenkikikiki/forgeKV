// Package porcupine provides linearizability checking for concurrent histories.
package porcupine

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// Operation represents a concurrent operation with call/return times.
type Operation struct {
	ClientId int
	Input    interface{}
	Output   interface{}
	Call     int64
	Return   int64
}

// Model defines the sequential specification for a system.
type Model struct {
	Partition func([]Operation) [][]Operation
	Init      func() interface{}
	Step      func(state, input, output interface{}) (bool, interface{})
}

// CheckOperations verifies linearizability of the given history against the model.
func CheckOperations(model Model, history []Operation) bool {
	if model.Init == nil || model.Step == nil {
		return false
	}

	parts := [][]Operation{history}
	if model.Partition != nil {
		parts = model.Partition(history)
	}

	for _, part := range parts {
		if len(part) == 0 {
			continue
		}
		if !checkPartition(model, part) {
			return false
		}
	}

	return true
}

func checkPartition(model Model, ops []Operation) bool {
	n := len(ops)
	if n == 0 {
		return true
	}

	succ := make([][]int, n)
	indeg := make([]int, n)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			if ops[i].Return < ops[j].Call {
				succ[i] = append(succ[i], j)
				indeg[j]++
			}
		}
	}

	avail := make([]int, 0, n)
	for i := 0; i < n; i++ {
		if indeg[i] == 0 {
			avail = append(avail, i)
		}
	}
	orderAvail(ops, avail)

	bitset := make([]uint64, (n+63)/64)
	memo := make(map[memoKey]struct{}, 1024)

	return dfs(model, ops, succ, indeg, avail, 0, model.Init(), bitset, memo)
}

type memoKey struct {
	state string
	bits  string
}

func dfs(model Model, ops []Operation, succ [][]int, indeg []int, avail []int, done int, state interface{}, bitset []uint64, memo map[memoKey]struct{}) bool {
	if done == len(ops) {
		return true
	}
	if len(avail) == 0 {
		return false
	}

	key := memoKey{state: stateKey(state), bits: bitsetKey(bitset)}
	if _, ok := memo[key]; ok {
		return false
	}

	for idx, opIndex := range avail {
		op := ops[opIndex]
		ok, nextState := model.Step(state, op.Input, op.Output)
		if !ok {
			continue
		}

		nextIndeg := append([]int(nil), indeg...)
		nextAvail := make([]int, 0, len(avail)+len(succ[opIndex]))
		for j, cand := range avail {
			if j == idx {
				continue
			}
			nextAvail = append(nextAvail, cand)
		}

		nextBits := append([]uint64(nil), bitset...)
		setBit(nextBits, opIndex)

		for _, s := range succ[opIndex] {
			nextIndeg[s]--
			if nextIndeg[s] == 0 {
				nextAvail = append(nextAvail, s)
			}
		}
		orderAvail(ops, nextAvail)

		if dfs(model, ops, succ, nextIndeg, nextAvail, done+1, nextState, nextBits, memo) {
			return true
		}
	}

	memo[key] = struct{}{}
	return false
}

func orderAvail(ops []Operation, avail []int) {
	sort.Slice(avail, func(i, j int) bool {
		opI := ops[avail[i]]
		opJ := ops[avail[j]]
		if opI.Call == opJ.Call {
			return opI.Return < opJ.Return
		}
		return opI.Call < opJ.Call
	})
}

func setBit(bits []uint64, idx int) {
	word := idx / 64
	offset := uint(idx % 64)
	bits[word] |= 1 << offset
}

func bitsetKey(bits []uint64) string {
	if len(bits) == 0 {
		return ""
	}
	buf := make([]byte, len(bits)*8)
	for i, v := range bits {
		binary.LittleEndian.PutUint64(buf[i*8:], v)
	}
	return string(buf)
}

func stateKey(state interface{}) string {
	if state == nil {
		return "<nil>"
	}
	v := reflect.ValueOf(state)
	if v.Kind() == reflect.Map && v.Type().Key().Kind() == reflect.String && v.Type().Elem().Kind() == reflect.String {
		keys := v.MapKeys()
		sorted := make([]string, 0, len(keys))
		for _, k := range keys {
			sorted = append(sorted, k.String())
		}
		sort.Strings(sorted)
		var b strings.Builder
		for _, k := range sorted {
			b.WriteString(k)
			b.WriteByte('=')
			b.WriteString(v.MapIndex(reflect.ValueOf(k)).String())
			b.WriteByte(';')
		}
		return b.String()
	}

	return fmt.Sprintf("%#v", state)
}
