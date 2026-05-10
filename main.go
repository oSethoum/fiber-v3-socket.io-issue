package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/contrib/v3/socketio"
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/cors"
	"github.com/gofiber/fiber/v3/middleware/logger"
	"github.com/gofiber/fiber/v3/middleware/static"
)

// Message struct for JSON payloads
type Message struct {
	From      string    `json:"from"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

// AuthPayload for handshake authentication
type AuthPayload struct {
	Token string `json:"token"`
	User  string `json:"user"`
}

func main() {
	// === POLLING CONFIGURATION ===
	// Enable HTTP long-polling fallback support
	socketio.EnablePolling = true

	// Optional: Tune polling parameters (defaults shown)
	socketio.PollingMaxBufferSize = 1_000_000 // Max POST body size
	socketio.MaxPollWait = 30 * time.Second   // Max long-poll block time
	socketio.PollQueueMaxFrames = 1024        // Max frames in poll queue

	// Optional: Enable library logging for debugging
	socketio.Logger = func(level, msg string, fields ...any) {
		log.Printf("[%s] %s %v", level, msg, fields)
	}

	// Create Fiber app
	app := fiber.New(fiber.Config{
		AppName: "Socket.IO Test Server (WS + Polling)",
	})

	// === MIDDLEWARE ===
	// CORS is REQUIRED for polling (XHR/fetch requests)
	app.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"http://localhost:3000"},
		AllowMethods:     []string{"GET", "POST", "OPTIONS"},
		AllowHeaders:     []string{"Content-Type", "Authorization"},
		AllowCredentials: true,
	}))
	app.Use(logger.New())

	// Serve embedded test HTML page
	app.Get("/", static.New("./index.html"))

	// Health check endpoint
	app.Get("/health", func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status":          "ok",
			"time":            time.Now(),
			"polling_enabled": socketio.EnablePolling,
		})
	})

	// === SOCKET.IO HANDLER ===
	socketConfig := socketio.New(func(kws *socketio.Websocket) {
		log.Printf("🔌 New connection: %s (transport: %s)",
			kws.GetUUID(),
			getTransport(kws))

		// Check auth payload if provided
		if auth := kws.HandshakeAuth(); auth != nil {
			var payload AuthPayload
			if err := json.Unmarshal(auth, &payload); err == nil {
				log.Printf("🔐 Auth: user=%s, token=%s", payload.User, payload.Token)
				if payload.Token == "" {
					log.Printf("❌ Invalid auth for %s", kws.GetUUID())
					kws.Close()
					return
				}
				kws.SetAttribute("user", payload.User)
			}
		}

		// Store connection metadata
		kws.SetAttribute("connected_at", time.Now())

		// ✅ Get IP: works for both WebSocket and Polling
		// Client can send ?ip= via query, or we use a placeholder
		if ip := kws.Query("ip", ""); ip != "" {
			kws.SetAttribute("ip", ip)
		} else {
			kws.SetAttribute("ip", "unknown")
		}
		kws.SetAttribute("client_id", kws.GetUUID())

		// === EVENT LISTENERS ===

		// Handle "message" event (raw text/binary message)
		socketio.On(socketio.EventMessage, func(payload *socketio.EventPayload) {
			log.Printf("📨 Received 'message' from %s: %s",
				payload.SocketUUID, string(payload.Data))

			var msg Message
			if err := json.Unmarshal(payload.Data, &msg); err == nil {
				msg.Timestamp = time.Now()
				response, _ := json.Marshal(msg)

				if payload.HasAck {
					log.Printf("✅ Sending ack for message from %s", payload.SocketUUID)
					_ = payload.Ack(response)
				}
				kws.Broadcast(response, true)
			}
		})

		// Handle custom "chat" event with multiple arguments
		socketio.On("chat", func(payload *socketio.EventPayload) {
			log.Printf("💬 Chat event from %s, args: %d",
				payload.SocketUUID, len(payload.Args))

			if len(payload.Args) >= 2 {
				room := string(payload.Args[0])
				message := string(payload.Args[1])
				log.Printf("📢 Room [%s]: %s", room, message)

				if payload.HasAck {
					_ = payload.Ack(
						[]byte(`{"status":"delivered"}`),
						[]byte(fmt.Sprintf(`{"room":"%s"}`, room)),
					)
				}
			}
		})

		// Handle "ping" event with ack
		socketio.On("ping", func(payload *socketio.EventPayload) {
			log.Printf("🏓 Received ping from %s", payload.SocketUUID)
			if payload.HasAck {
				_ = payload.Ack([]byte(`{"pong":true,"server_time":"` +
					time.Now().Format(time.RFC3339) + `"}`))
			}
		})

		// Handle disconnect
		socketio.On(socketio.EventDisconnect, func(payload *socketio.EventPayload) {
			log.Printf("🔌 Disconnected: %s (reason: %v, transport: %s)",
				payload.SocketUUID, payload.Error, getTransport(kws))
		})

		// Handle errors
		socketio.On(socketio.EventError, func(payload *socketio.EventPayload) {
			log.Printf("❌ Error on %s: %v", payload.SocketUUID, payload.Error)
		})

		// === SERVER-INITIATED EVENTS ===

		// Send welcome message
		welcome := Message{
			From: "server",
			Content: fmt.Sprintf("Welcome! ID: %s, Transport: %s",
				kws.GetUUID(), getTransport(kws)),
			Timestamp: time.Now(),
		}
		welcomeJSON, _ := json.Marshal(welcome)
		kws.EmitEvent("welcome", welcomeJSON)

		// Server-initiated event with ack (after 2 seconds)
		go func() {
			time.Sleep(2 * time.Second)
			if !kws.IsAlive() {
				return
			}

			log.Printf("🔄 Sending server event with ack to %s", kws.GetUUID())

			kws.EmitWithAckTimeout("server_event",
				[]byte(`{"from":"server","action":"heartbeat"}`),
				10*time.Second,
				func(ack []byte, err error) {
					if err != nil {
						log.Printf("⚠️ Ack error for %s: %v", kws.GetUUID(), err)
						return
					}
					log.Printf("✅ Received ack from %s: %s", kws.GetUUID(), string(ack))
				},
			)
		}()
	})

	// === MOUNT SOCKET.IO HANDLER ===
	// 🔑 CRITICAL: Must mount for BOTH GET and POST when polling is enabled
	// This handles:
	// - GET /socket.io/?EIO=4&transport=polling (long-poll)
	// - POST /socket.io/?EIO=4&transport=polling&sid=xxx (send data)
	// - GET /socket.io/?EIO=4&transport=websocket (WS upgrade)

	// Option 1: Explicit GET + POST (recommended for clarity)
	app.Get("/socket.io/*", socketConfig)
	app.Post("/socket.io/*", socketConfig)

	// Option 2: Use All() for all methods (also works)
	// app.All("/socket.io/*", socketConfig)

	// === START SERVER ===
	go func() {
		port := ":3000"
		log.Printf("🚀 Server starting on http://localhost%s", port)
		log.Printf("🧪 Test client: http://localhost%s/", port)
		log.Printf("🔗 Socket.IO endpoint: http://localhost%s/socket.io/", port)
		log.Printf("📡 Polling enabled: %v", socketio.EnablePolling)

		if err := app.Listen(port); err != nil {
			log.Fatalf("❌ Server error: %v", err)
		}
	}()

	// === GRACEFUL SHUTDOWN ===
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("🛑 Shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := socketio.Shutdown(ctx); err != nil {
		log.Printf("⚠️ Socket.IO shutdown error: %v", err)
	}

	if err := app.ShutdownWithContext(ctx); err != nil {
		log.Printf("⚠️ Fiber shutdown error: %v", err)
	}

	log.Println("✅ Server stopped")
}

// getTransport returns a hint about the transport type.
// Note: The library doesn't expose transport directly, but we can infer
// from connection characteristics. This is for logging only.
func getTransport(kws *socketio.Websocket) string {
	// Polling sessions have a pollQ; WebSocket sessions don't
	// This is an internal detail - in production, you might store
	// the transport type as an attribute during handshake.
	if kws != nil {
		// Heuristic: if we can't get a "real" connection, likely polling
		// This is approximate; the library may expose transport info in future
		return "auto" // Let client decide
	}
	return "unknown"
}
