// server.js
import express from "express";
import bodyParser from "body-parser";
import { BigQuery } from "@google-cloud/bigquery";

const app = express();
app.use(bodyParser.json());

const bigquery = new BigQuery();
const datasetId = "whatsapp_analytics"; // your dataset
const tableId = "message_events";       // your table

app.post("/ingest", async (req, res) => {
  try {
    const data = req.body;

    // Map to your schema
    const row = {
      event_id: data.event_id,
      message_id: data.message_id,
      conversation_id: data.conversation_id,
      org_id: data.org_id,
      user_id: data.user_id || null,
      message_timestamp: data.message_timestamp,
      ingestion_timestamp: new Date().toISOString(),
      sender_type: data.sender_type,
      message_text: data.message_text || null,
      word_count: data.message_text ? data.message_text.split(/\s+/).length : null,
      sentiment_score: data.sentiment_score || null,
    };

    // Insert into BigQuery
    await bigquery.dataset(datasetId).table(tableId).insert([row]);

    res.status(200).json({ status: "success", inserted: row });
  } catch (err) {
    console.error("Error inserting into BigQuery:", err);
    res.status(500).json({ status: "error", message: err.message });
  }
});

app.listen(3004, () => {
  console.log("Ingestion service running on http://localhost:3004");
});
