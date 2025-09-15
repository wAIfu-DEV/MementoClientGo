package memento

// go get github.com/gorilla/websocket

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"os/exec"
	"path"
	"sync"
	"syscall"
	"time"

	websocket "github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second    // Time allowed to write a message to the peer.
	pongWait       = 10 * time.Second    // Time allowed to read the next pong message from the peer.
	pingPeriod     = (pongWait * 9) / 10 // Send pings to peer with this period. Must be less than pongWait.
	maxMessageSize = 1024 * 10           // Maximum message size allowed from peer.
)

type Memory struct {
	Id      string
	Content string
	Time    int64

	User     *string
	Score    *float64
	Lifetime *int64
}

func (m *Memory) SetFromMap(obj map[string]any) error {
	var ok bool
	if m.Id, ok = obj["id"].(string); !ok {
		return errors.New("missing field \"id\" from received memory object")
	}
	if m.Content, ok = obj["content"].(string); !ok {
		return errors.New("missing field \"content\" from received memory object")
	}
	if fltTime, ok := obj["time"].(float64); ok {
		m.Time = int64(math.Round(fltTime))
	} else {
		return errors.New("missing field \"time\" from received memory object")
	}

	if maybeUser, ok := obj["user"].(string); ok {
		m.User = &maybeUser
	}
	if maybeScore, ok := obj["score"].(float64); ok {
		m.Score = &maybeScore
	}
	if maybeLifetime, ok := obj["lifetime"].(float64); ok {
		iVal := int64(math.Round(maybeLifetime))
		m.Lifetime = &iVal
	}
	return nil
}

func (m *Memory) ToMap() map[string]any {
	var ret map[string]any = map[string]any{
		"id":      m.Id,
		"content": m.Content,
		"time":    m.Time,
	}

	if m.User != nil {
		ret["user"] = *m.User
	}
	if m.Score != nil {
		ret["score"] = *m.Score
	}
	if m.Lifetime != nil {
		ret["lifetime"] = *m.Lifetime
	}
	return ret
}

type QueriedMemory struct {
	Memory   Memory
	Distance float64
}

func (q *QueriedMemory) SetFromMap(m map[string]any) error {
	memObj, ok := m["memory"].(map[string]any)
	if !ok {
		return errors.New("missing field \"memory\" from received queried memory object")
	}
	err := q.Memory.SetFromMap(memObj)
	if err != nil {
		return err
	}
	if q.Distance, ok = m["distance"].(float64); !ok {
		return errors.New("missing field \"distance\" from received queried memory object")
	}
	return nil
}

func (q *QueriedMemory) ToMap() map[string]any {
	return map[string]any{
		"memory":   q.Memory.ToMap(),
		"distance": q.Distance,
	}
}

type OpenLlmMessage struct {
	Role    string
	Content string
	Name    *string
}

func (m *OpenLlmMessage) ToMap() map[string]any {
	ret := map[string]any{
		"role":    m.Role,
		"content": m.Content,
	}
	if m.Name != nil {
		ret["name"] = *m.Name
	}
	return ret
}

type QueryResult struct {
	Stm  []QueriedMemory
	Ltm  []QueriedMemory
	User []Memory
}

type CountResult struct {
	StmCount *int64
	LtmCount *int64
}

type genericResult[T any] struct {
	Result T
	Err    error
}

type messageHandlers struct {
	query map[string]chan genericResult[QueryResult]
	count map[string]chan genericResult[CountResult]
}

type Client struct {
	conn        *websocket.Conn
	backendProc *os.Process
	url         url.URL
	handlers    messageHandlers
	mutex       sync.Mutex
	connected   bool
}

func (c *Client) Disconnect(timeout time.Duration) {
	if c.conn != nil {
		c.mutex.Lock()
		c.connected = false
		c.mutex.Unlock()

		c.conn.WriteControl(websocket.CloseMessage, []byte{}, time.Now().Add(timeout))
		c.conn.Close()
	}
}

func (c *Client) handleMessage(jsonMsg map[string]any, msgType string, msgId string) {
	switch msgType {
	case "query":

		var msgHandler chan genericResult[QueryResult]
		{ // MUTEX CTX =========================================================
			c.mutex.Lock()
			defer c.mutex.Unlock()

			var ok bool
			msgHandler, ok = c.handlers.query[msgId]
			if !ok {
				fmt.Println("memento error: missing handler for query response")
				return
			}
			delete(c.handlers.query, msgId)
		}

		res := QueryResult{}

		if maybeStm, ok := jsonMsg["stm"].([]any); ok {
			for _, ent := range maybeStm {
				qm := QueriedMemory{}
				err := qm.SetFromMap(ent.(map[string]any))
				if err != nil {
					msgHandler <- genericResult[QueryResult]{Err: err}
					goto switch_query_end
				}
				res.Stm = append(res.Stm, qm)
			}
		}

		if maybeLtm, ok := jsonMsg["ltm"].([]any); ok {
			for _, ent := range maybeLtm {
				qm := QueriedMemory{}
				err := qm.SetFromMap(ent.(map[string]any))
				if err != nil {
					msgHandler <- genericResult[QueryResult]{Err: err}
					goto switch_query_end
				}
				res.Ltm = append(res.Ltm, qm)
			}
		}

		if maybeUsers, ok := jsonMsg["users"].([]any); ok {
			for _, ent := range maybeUsers {
				m := Memory{}
				err := m.SetFromMap(ent.(map[string]any))
				if err != nil {
					msgHandler <- genericResult[QueryResult]{Err: err}
					goto switch_query_end
				}
				res.User = append(res.User, m)
			}
		}

		msgHandler <- genericResult[QueryResult]{Result: res} // deliver result

	switch_query_end: // cleanup
		close(msgHandler)
		return
	case "count":

		var msgHandler chan genericResult[CountResult]
		{ // MUTEX CTX =========================================================
			c.mutex.Lock()
			defer c.mutex.Unlock()

			var ok bool
			msgHandler, ok = c.handlers.count[msgId]
			if !ok {
				fmt.Println("memento error: missing handler for count response")
				return
			}
			delete(c.handlers.count, msgId)
		}

		res := CountResult{}

		if maybeStm, ok := jsonMsg["stm"].(float64); ok {
			val := int64(math.Round(maybeStm))
			res.StmCount = &val
		}

		if maybeLtm, ok := jsonMsg["ltm"].(float64); ok {
			val := int64(math.Round(maybeLtm))
			res.LtmCount = &val
		}

		msgHandler <- genericResult[CountResult]{Result: res} // deliver result

		// cleanup
		close(msgHandler)
		return
	}
}

func (c *Client) pingLoop() {
	for {
		time.Sleep(pingPeriod)

		if c.isConnDead() {
			fmt.Printf("memento: connection dead, exited ping loop")
			return
		}
		c.conn.SetWriteDeadline(time.Time{})
		c.conn.WriteMessage(websocket.PingMessage, nil)
	}
}

func (c *Client) recvLoop() {
	for {
		if c.isConnDead() {
			fmt.Printf("memento: cannot read from closed connection, ending recv loop.\n")
			return
		}

		c.conn.SetReadLimit(maxMessageSize)
		c.conn.SetReadDeadline(time.Time{})
		_, p, err := c.conn.ReadMessage()
		if err != nil {
			closeErr := &websocket.CloseError{}
			if errors.As(err, &closeErr) {
				c.mutex.Lock()
				c.connected = false
				c.mutex.Unlock()
				return // quit goroutine
			}

			fmt.Printf("memento error on recv: %s\n", err.Error())
			continue
		}

		var jsonMsg map[string]any
		err = json.Unmarshal(p, &jsonMsg)
		if err != nil {
			fmt.Printf("memento: failed to convert message to json.\n")
			continue
		}

		msgType, ok := jsonMsg["type"].(string)
		if !ok {
			fmt.Println("memento error: missing field \"type\" in received json message")
			continue
		}

		msgId, ok := jsonMsg["uid"].(string)
		if !ok {
			fmt.Println("memento error: missing field \"uid\" in received json message")
			continue
		}

		c.handleMessage(jsonMsg, msgType, msgId)
	}
}

func NewClient(host string, port int, absPath string) (*Client, error) {
	c := &Client{}
	c.url = url.URL{Scheme: "ws", Host: fmt.Sprintf("%s:%d", host, port)}

	c.handlers = messageHandlers{
		query: map[string]chan genericResult[QueryResult]{},
		count: map[string]chan genericResult[CountResult]{},
	}

	c.backendProc = nil
	c.mutex = sync.Mutex{}

	customDialer := *websocket.DefaultDialer
	customDialer.HandshakeTimeout = 3 * time.Second

	fmt.Printf("memento: connecting to %s\n", c.url.String())

	conn, _, err := customDialer.Dial(c.url.String(), nil)
	if err != nil {
		if absPath == "" {
			return nil, err
		} else {
			fmt.Printf("memento: failed to connect, launching Memento backend.\n")

			pythonPath := path.Join(absPath, "venv", "Scripts", "python.exe")
			mainPath := path.Join(absPath, "main.py")

			fmt.Printf("memento: at %s\n", absPath)

			cmd := exec.Command("cmd.exe", "/C", fmt.Sprintf("%s %s", pythonPath, mainPath))
			cmd.Dir = absPath

			cmd.SysProcAttr = &syscall.SysProcAttr{
				CreationFlags:    0x00000010,
				HideWindow:       false,
				NoInheritHandles: true,
			}

			err = cmd.Start()
			if err != nil {
				return nil, err
			}

			deadline := time.Now().Add(10 * time.Second)
			var lastErr error = nil

			fmt.Printf("memento: retrying connecting...\n")
			for time.Now().Before(deadline) {
				conn, _, err = customDialer.Dial(c.url.String(), nil)
				if err == nil {
					break
				}
				lastErr = err
				time.Sleep(500 * time.Millisecond)
			}

			if conn == nil {
				cmd.Process.Kill()
				return nil, lastErr
			}
			c.backendProc = cmd.Process
		}
	}
	fmt.Printf("memento: successfully connected.\n")

	c.conn = conn
	c.connected = true

	c.conn.SetCloseHandler(func(code int, text string) error {
		fmt.Printf("memento: connection closed with code: %d and reason: %s\n", code, text)
		return &websocket.CloseError{Code: code, Text: text}
	})

	c.conn.SetPingHandler(func(_ string) error {
		fmt.Printf("memento: recieved ping.\n")

		c.conn.SetWriteDeadline(time.Time{})
		return c.conn.WriteMessage(websocket.PongMessage, nil)
	})

	c.conn.SetPongHandler(func(_ string) error {
		return nil
	})

	go c.recvLoop() // recv handler
	go c.pingLoop() // ping loop

	return c, nil
}

type DbEnum = string

const (
	DB_SHORT_TERM DbEnum = "stm"
	DB_LONG_TERM  DbEnum = "ltm"
	DB_USERS      DbEnum = "users"
)

func (c *Client) generateUid() string {
	randBytes := make([]byte, 16)
	rand.Read(randBytes)
	return base64.StdEncoding.EncodeToString(randBytes)
}

func (c *Client) isConnDead() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return !c.connected
}

func (c *Client) Query(
	queryStr string,
	collectionName string,
	user *string,
	fromDb []DbEnum,
	n []int64,
	timeout time.Duration,
) (QueryResult, error) {

	var zero QueryResult
	var retErr error

	if c.isConnDead() {
		return zero, errors.New("memento: client not connected")
	}

	uniqueId := c.generateUid()

	resultChan := make(chan genericResult[QueryResult], 1)

	c.mutex.Lock() //===========================================================
	c.handlers.query[uniqueId] = resultChan
	c.mutex.Unlock() //=========================================================

	data := map[string]any{
		"uid":     uniqueId,
		"type":    "query",
		"query":   queryStr,
		"ai_name": collectionName,
		"from":    fromDb,
		"n":       n,
		"user":    user,
	}

	c.conn.SetWriteDeadline(time.Time{})
	err := c.conn.WriteJSON(data)
	if err != nil {
		retErr = err
		goto end_with_cleanup
	}

	select {
	case qRes := <-resultChan:
		return qRes.Result, qRes.Err
	case <-time.After(timeout):
		retErr = errors.New("timeout")
		goto end_with_cleanup
	}

end_with_cleanup:
	c.mutex.Lock() //===========================================================
	if ch, exists := c.handlers.query[uniqueId]; exists {
		delete(c.handlers.query, uniqueId)
		close(ch) // Safe to close since we still own it
	}
	c.mutex.Unlock() //=========================================================
	return zero, retErr
}

func (c *Client) Count(
	collectionName string,
	fromDb []DbEnum,
	timeout time.Duration,
) (CountResult, error) {

	var zero CountResult
	var retErr error

	if c.isConnDead() {
		return zero, errors.New("memento: client not connected")
	}

	uniqueId := c.generateUid()

	resultChan := make(chan genericResult[CountResult], 1)

	c.mutex.Lock() //===========================================================
	c.handlers.count[uniqueId] = resultChan
	c.mutex.Unlock() //=========================================================

	c.conn.SetWriteDeadline(time.Time{})
	err := c.conn.WriteJSON(map[string]any{
		"uid":     uniqueId,
		"type":    "count",
		"ai_name": collectionName,
		"from":    fromDb,
	})
	if err != nil {
		retErr = err
		goto end_with_cleanup
	}

	select {
	case cRes := <-resultChan:
		return cRes.Result, cRes.Err
	case <-time.After(timeout):
		retErr = errors.New("timeout")
		goto end_with_cleanup
	}

end_with_cleanup:
	c.mutex.Lock() //===========================================================
	if ch, exists := c.handlers.count[uniqueId]; exists {
		delete(c.handlers.count, uniqueId)
		close(ch) // Safe to close since we still own it
	}
	c.mutex.Unlock() //=========================================================
	return zero, retErr
}

func (c *Client) Store(
	memories []Memory,
	collectionName string,
	to []DbEnum,
) error {
	if c.isConnDead() {
		return errors.New("memento: client not connected")
	}

	mems := []map[string]any{}
	for _, mem := range memories {
		mems = append(mems, mem.ToMap())
	}

	c.conn.SetWriteDeadline(time.Time{})
	return c.conn.WriteJSON(map[string]any{
		"uid":      c.generateUid(),
		"type":     "store",
		"memories": mems,
		"ai_name":  collectionName,
		"to":       to,
	})
}

func (c *Client) Process(
	messages []OpenLlmMessage,
	context []OpenLlmMessage,
	collectionName string,
) error {
	if c.isConnDead() {
		return errors.New("memento: client not connected")
	}

	msgMaps := make([]map[string]any, len(messages))
	for i, msg := range messages {
		msgMaps[i] = msg.ToMap()
	}

	ctxMaps := make([]map[string]any, len(context))
	for i, msg := range context {
		ctxMaps[i] = msg.ToMap()
	}

	c.conn.SetWriteDeadline(time.Time{})
	return c.conn.WriteJSON(map[string]any{
		"uid":      c.generateUid(),
		"type":     "process",
		"messages": msgMaps,
		"context":  ctxMaps,
		"ai_name":  collectionName,
	})
}

func (c *Client) CloseBackend() error {
	if c.isConnDead() {
		return errors.New("memento: client not connected")
	}

	c.conn.SetWriteDeadline(time.Time{})
	return c.conn.WriteJSON(map[string]any{
		"uid":  c.generateUid(),
		"type": "close",
	})
}

func (c *Client) Evict(collectionName string) error {
	if c.isConnDead() {
		return errors.New("memento: client not connected")
	}

	c.conn.SetWriteDeadline(time.Time{})
	return c.conn.WriteJSON(map[string]any{
		"uid":     c.generateUid(),
		"type":    "evict",
		"ai_name": collectionName,
	})
}

func (c *Client) Clear(
	collectionName string,
	user *string,
	target []DbEnum,
) error {
	if c.isConnDead() {
		return errors.New("memento: client not connected")
	}

	data := map[string]any{
		"uid":     c.generateUid(),
		"type":    "clear",
		"ai_name": collectionName,
		"target":  target,
		"user":    user,
	}

	c.conn.SetWriteDeadline(time.Time{})
	return c.conn.WriteJSON(data)
}
