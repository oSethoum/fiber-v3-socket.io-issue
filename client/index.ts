import { io } from "socket.io-client";

// The URL parameter 'id' is required by your Fiber route: /ws/:id
const userId = "test_user_123";
const serverUrl = `http://localhost:3000/ws/${userId}`;

console.log(`Connecting to: ${serverUrl}`);

const socket = io(serverUrl, {
  // socket.io-client tries to use polling first by default.
  // Fiber's contrib/socketio usually expects immediate upgrade.
  transports: ["websocket"], // unsupported polling and webtransport for Fiber
});

// Connection attempt
socket.on("connect", () => {
  console.log("Connected! ID:", socket.id);

  // Attempt to send a message matching your Go struct
  const payload = {
    from: userId,
    to: "recipient_456",
    data: "Testing compatibility...",
  };

  socket.emit("message", payload);
});

// This is likely where it will fail if they are incompatible
socket.on("connect_error", (err) => {
  console.error("Connection Error Type:", err.message);
  console.error("Error Detail:", err);
});

socket.on("disconnect", (reason) => {
  console.log("Disconnected:", reason);
});
