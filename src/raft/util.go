package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = 0

func DPrintf(me int , currentTerm int , state string, v ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf("[Server: %d Term: %d role: %s] %s", me, currentTerm, state, fmt.Sprintln(v...))
	}
	return
}

const ShortDebug = 0

func DShortPrintf(me int, v ...interface{}) (n int, err error) {
	if ShortDebug > 0 {
		log.Printf("[Server: %d] %s", me, fmt.Sprintln(v...))
	}
	return
}
