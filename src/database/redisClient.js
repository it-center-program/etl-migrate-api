// redisClient.js
import { createClient } from "redis";

export const redis = createClient({
  url: "redis://:ETLRedis2025!@10.0.0.114:6379",
  socket: {
    reconnectStrategy: (retries) => {
      if (retries > 10) {
        console.error("❌ Redis reconnect failed too many times");
        return new Error("Redis reconnect failed");
      }
      // retry delay
      return Math.min(retries * 100, 3000);
    },
  },
});

// handle error
redis.on("error", (err) => {
  console.error("❌ Redis Error:", err);
});

// handle connection
redis.on("connect", () => {
  console.log("🔌 Redis connected");
});

// handle reconnect
redis.on("reconnecting", () => {
  console.log("🔄 Redis reconnecting...");
});

// must connect
await redis.connect();
