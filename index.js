// server.js
import express from "express";
import bodyParser from "body-parser";
import { BigQuery } from "@google-cloud/bigquery";
import { v4 as uuidv4 } from 'uuid';

const app = express();
app.use(bodyParser.json());

const bigquery = new BigQuery({
    keyFilename: "./gcp-key.json",
    projectId: "waba-454907",
  });
const datasetId = "whatsapp_analytics"; // your dataset
const tableId = "message_events";       // your table

/**
 * Transforms a single chat object from the source data into the format 
 * required for the BigQuery table schema.
 * @param {object} chat - The input chat object from dateAccChats.
 * @param {string} orgId - The organization ID for this batch.
 * @returns {object} The formatted row object for BigQuery.
 */
const transformChatToRow = (chat, orgId) => {
  const messageText = chat.Message || null;

  const eventId = `${orgId}-${uuidv4()}`;
  return {
    // REQUIRED fields
    event_id: eventId, // Generate a unique ID for each event
    message_id: chat.MessageId,
    chat_id: chat.Chatid,
    org_id: orgId, // Pass org_id from the function's context
    message_timestamp: new Date(chat.Datetime).toISOString(),
    ingestion_timestamp: new Date().toISOString(),
    direction: chat.Direction,
    type: chat.Type,

    // NULLABLE fields
    user_id: chat.CreatedByUser || null,
    sender_number: chat.SentByNumber || null,
    ack: chat.Ack || null,
    message_text: messageText,
    
    // Media fields (from root and SpecialData)
    file_url: chat.File || null,

    // Broadcast fields
    is_broadcast: chat.isBroadcast || null,
    
    // Other nested data and calculated fields
    special_data: chat.SpecialData ? JSON.stringify(chat.SpecialData) : null,
    word_count: messageText ? messageText.trim().split(/\s+/).length : null,
  };
};

/**
 * Flattens a nested chat data object and inserts the records into BigQuery.
 * @param {object} dateAccChats - The nested object containing chat data.
 * @param {string} orgId - The organization ID to associate with these records.
 */
const bigQueryProcessor = async (dateAccChats, orgId) => {
  try {
    // Flatten the nested object structure into a single array of chats
    const allChats = Object.values(dateAccChats).flatMap(chatterObj => Object.values(chatterObj).flat());

    if (allChats.length === 0) {
      console.log("No rows to insert");
      return "Success";
    }

    // Transform the array of chats into an array of BigQuery rows
    const rows = allChats.map(chat => transformChatToRow(chat, orgId));

    await bigquery.dataset(datasetId).table(tableId).insert(rows);
    console.log(`Inserted ${rows.length} rows into BigQuery`);
    
    return "Success";
  } catch (error) {
    console.error("BigQuery insert error:", JSON.stringify(error, null, 2));
    throw new Error("Failed to insert rows into BigQuery.");
  }
};

// New endpoint for batch processing with dateAccChats and orgId
app.post("/ingest", async (req, res) => {
  try {
    const { dateAccChats, orgId } = req.body;

    if (!dateAccChats || !orgId) {
      return res.status(400).json({ 
        status: "error", 
        message: "dateAccChats and orgId are required in request body" 
      });
    }

    const result = await bigQueryProcessor(dateAccChats, orgId);
    res.status(200).json({ status: "success", message: result });
  } catch (err) {
    console.error("Error processing batch data:", err);
    res.status(500).json({ status: "error", message: err.message });
  }
});

app.listen(3004, () => {
  console.log("Ingestion service running on http://localhost:3004");
});
