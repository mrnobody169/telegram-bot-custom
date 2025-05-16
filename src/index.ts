import express, { Express, Request, Response } from "express";
import TelegramBot from "node-telegram-bot-api";
import Bottleneck from "bottleneck";
import { createClient } from "redis";

// Configuration
const TELEGRAM_BOT_TOKEN = "8075560315:AAEGhKXrEMC6xAi4O56Lkr0tc0XqMZ8xPFM";
const CHAT_ID = "-1002679504089";
const REDIS_URL = "redis://localhost:6379";
const QUEUE_KEY = "telegram-messages";

// Initialize Telegram Bot
const bot = new TelegramBot(TELEGRAM_BOT_TOKEN, { polling: false });

// Initialize Redis
const redis = createClient({ url: REDIS_URL });
redis.on("error", (err) => console.error("Redis Client Error:", err));

// Rate limiter: 1 message/second
const limiter = new Bottleneck({
  minTime: 1000, // 1000ms = 1 second
  maxConcurrent: 1, // Only 1 message at a time
  reservoir: 1, // Allow only 1 message initially
  reservoirRefreshAmount: 1, // Refresh 1 message
  reservoirRefreshInterval: 1000, // Every 1000ms
});

// Function to send Telegram message
const sendTelegramMessage = async (message: string): Promise<void> => {
  try {
    await new Promise((resolve) => setTimeout(resolve, 2000));
    await limiter.schedule(() =>
      bot.sendMessage(CHAT_ID, message, { parse_mode: "HTML" })
    );
  } catch (error: any) {
    console.log(error?.message);
  }
};

// Function to process messages from Redis queue
const processQueue = async (): Promise<void> => {
  await redis.connect();
  console.log("Connected to Redis, processing queue...");

  while (true) {
    try {
      // Pop message from the right end of the queue (RPOP)
      const message = await redis.rPop(QUEUE_KEY);
      if (message) {
        // Convert Buffer to string if necessary
        const messageStr = Buffer.isBuffer(message)
          ? message.toString("utf8")
          : message;
        await sendTelegramMessage(messageStr);
        await new Promise((resolve) => setTimeout(resolve, 200));
      } else {
        // Wait briefly if queue is empty to avoid busy looping
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    } catch (error) {
      console.error("Error processing queue:", error);
      // Wait before retrying to prevent infinite loop on error
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
};

// Initialize Express server
const app: Express = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// Endpoint to receive messages from other servers
app.post(
  "/send-message",
  async (req: Request, res: Response): Promise<void> => {
    const { message } = req.body;
    if (!message || typeof message !== "string") {
      res.status(400).json({ error: "Invalid message" });
      return;
    }

    try {
      // Push message to Redis queue (LPUSH)
      await redis.lPush(QUEUE_KEY, message);
      res.status(200).json({ status: "Message queued" });
    } catch (error) {
      console.error("Error queuing message:", error);
      res.status(500).json({ error: "Failed to queue message" });
    }
  }
);

// Health check endpoint
app.get("/health", (req: Request, res: Response): void => {
  res.status(200).json({ status: "OK" });
});

// Start server and queue processor
app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  await processQueue();
});
