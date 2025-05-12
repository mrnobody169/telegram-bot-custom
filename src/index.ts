import express, { Express, Request, Response } from "express";
import TelegramBot from "node-telegram-bot-api";
import Bottleneck from "bottleneck";
import { createClient } from "redis";

// Configuration
const TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"; // Replace with your bot token
const CHAT_ID = "YOUR_CHAT_ID"; // Replace with your chat ID
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
});

// Function to send Telegram message
const sendTelegramMessage = async (message: string): Promise<void> => {
  try {
    await limiter.schedule(() =>
      bot.sendMessage(CHAT_ID, message, { parse_mode: "HTML" })
    );
    console.log(`Message sent to Telegram: ${message}`);
  } catch (error) {
    console.error("Error sending Telegram message:", error);
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
        console.log(`Processing message: ${messageStr}`);
        await sendTelegramMessage(messageStr);
      } else {
        // Wait briefly if queue is empty to avoid busy looping
        await new Promise((resolve) => setTimeout(resolve, 100));
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
      console.log(`Message queued: ${message}`);
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
