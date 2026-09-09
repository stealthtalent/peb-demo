//go:build ignore

package main

import (
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	ws, _, err := websocket.DefaultDialer.Dial("ws://localhost:7700/listen/webui_events", nil)
	if err != nil {
		fmt.Println("connect error:", err)
		return
	}
	defer ws.Close()
	fmt.Println("connected to pg_eventserv")

	timeout := time.After(8 * time.Second)
	for {
		select {
		case <-timeout:
			return
		default:
			_, msg, err := ws.ReadMessage()
			if err != nil {
				fmt.Println("read error:", err)
				return
			}
			fmt.Println("RECEIVED:", string(msg))
		}
	}
}
