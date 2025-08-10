#!/usr/bin/env python3
import os
import json
import asyncio
import argparse
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

def normalize_webhook_payload(raw):
    """
    Convert a raw WhatsApp webhook payload (metaData.entry[].changes[].value)
    to a normalized small payload of type "message" or "status".
    If `raw` is already in normalized format, return it as-is.
    Return None if can't parse.
    """
    if not isinstance(raw, dict):
        return None

    # Already normalized
    if raw.get("type") in ("message", "status"):
        return raw

    # Try webhook metaData structure
    meta = raw.get("metaData") or raw.get("meta_data") or {}
    if meta:
        try:
            entry = meta.get("entry", [])[0]
            change = entry.get("changes", [])[0]
            value = change.get("value", {})

            # inbound messages
            if value.get("messages"):
                msg = value["messages"][0]
                contacts = value.get("contacts", [])
                wa_id = None
                if contacts:
                    wa_id = contacts[0].get("wa_id") or contacts[0].get("waId")

                # text body
                text = ""
                if isinstance(msg.get("text"), dict):
                    text = msg["text"].get("body", "")
                elif isinstance(msg.get("message"), dict):
                    text = msg["message"].get("body", "")

                meta_msg_id = msg.get("id") or msg.get("mid")
                ts = msg.get("timestamp") or msg.get("ts")
                timestamp_iso = None
                if ts:
                    try:
                        timestamp_iso = datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
                    except Exception:
                        timestamp_iso = None

                return {
                    "type": "message",
                    "wa_id": wa_id,
                    "meta_msg_id": meta_msg_id,
                    "text": text,
                    "timestamp": timestamp_iso or datetime.now(timezone.utc).isoformat(),
                    "direction": "inbound",
                    "status": "delivered"
                }

            # status updates (webhook)
            if value.get("statuses"):
                st = value["statuses"][0]
                meta_msg_id = st.get("id")
                status = st.get("status")
                ts = st.get("timestamp")
                timestamp_iso = None
                if ts:
                    try:
                        timestamp_iso = datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
                    except Exception:
                        timestamp_iso = None

                return {
                    "type": "status",
                    "meta_msg_id": meta_msg_id,
                    "status": status,
                    "timestamp": timestamp_iso or datetime.now(timezone.utc).isoformat()
                }

        except Exception as e:
            print(f"[WARN] webhook parse error: {e}")
            return None

    # Not recognized
    return None


async def process_path(path: str):
    client = AsyncIOMotorClient(os.getenv("MONGODB_URI"))
    db_name = os.getenv("MONGODB_NAME", "whatsapp")
    db = client[db_name]
    messages_col = db.processed_messages
    pending_col = db.pending_status_updates

    processed = 0
    skipped = 0
    errors = 0

    # build file list (single file or recursive directory)
    files = []
    if os.path.isfile(path) and path.lower().endswith(".json"):
        files = [path]
    elif os.path.isdir(path):
        for root, _, filenames in os.walk(path):
            for fn in filenames:
                if fn.lower().endswith(".json"):
                    files.append(os.path.join(root, fn))
    else:
        print(f"Invalid path: {path}")
        return

    # process files in stable order
    for filepath in sorted(files):
        try:
            with open(filepath, encoding="utf-8") as f:
                raw = json.load(f)
        except Exception as e:
            print(f"[ERROR] Can't read {filepath}: {e}")
            errors += 1
            continue

        payload = normalize_webhook_payload(raw)
        if not payload:
            print(f"[SKIP] Unknown payload structure: {os.path.basename(filepath)}")
            skipped += 1
            continue

        try:
            if payload["type"] == "message":
                mid = payload.get("meta_msg_id")
                if not mid:
                    print(f"[SKIP] message missing meta_msg_id: {os.path.basename(filepath)}")
                    skipped += 1
                    continue

                existing = await messages_col.find_one({"meta_msg_id": mid})
                if existing:
                    print(f"[SKIP] Duplicate message {mid}")
                    skipped += 1
                    # apply pending status if any
                    pending = await pending_col.find_one({"meta_msg_id": mid})
                    if pending:
                        await messages_col.update_one({"meta_msg_id": mid}, {"$set": {"status": pending["new_status"]}})
                        await pending_col.delete_one({"meta_msg_id": mid})
                        print(f"[APPLIED] Pending status '{pending['new_status']}' for {mid}")
                    continue

                doc = {
                    "wa_id": payload.get("wa_id"),
                    "text": payload.get("text"),
                    "meta_msg_id": mid,
                    "direction": payload.get("direction", "inbound"),
                    "status": payload.get("status", "delivered"),
                    "timestamp": payload.get("timestamp") or datetime.now(timezone.utc).isoformat()
                }

                await messages_col.insert_one(doc)
                print(f"[INSERTED] {mid}")
                processed += 1

                # apply pending status if any
                pending = await pending_col.find_one({"meta_msg_id": mid})
                if pending:
                    await messages_col.update_one({"meta_msg_id": mid}, {"$set": {"status": pending["new_status"]}})
                    await pending_col.delete_one({"meta_msg_id": mid})
                    print(f"[APPLIED] Pending status '{pending['new_status']}' for {mid}")

            elif payload["type"] == "status":
                mid = payload.get("meta_msg_id")
                if not mid:
                    print(f"[SKIP] status missing meta_msg_id: {os.path.basename(filepath)}")
                    skipped += 1
                    continue

                result = await messages_col.update_one({"meta_msg_id": mid}, {"$set": {"status": payload.get("status")}})
                if result.modified_count > 0:
                    print(f"[UPDATED] Status for {mid} -> {payload.get('status')}")
                    processed += 1
                else:
                    # store pending status to apply later
                    await pending_col.update_one(
                        {"meta_msg_id": mid},
                        {"$set": {"new_status": payload.get("status"), "received_at": datetime.now(timezone.utc).isoformat()}},
                        upsert=True
                    )
                    print(f"[PENDING] Stored status '{payload.get('status')}' for future message {mid}")
                    processed += 1
            else:
                print(f"[SKIP] Unknown payload.type in {os.path.basename(filepath)}")
                skipped += 1

        except Exception as e:
            print(f"[ERROR] Processing {os.path.basename(filepath)}: {e}")
            errors += 1

    print("=== Summary ===")
    print(f"Processed: {processed}")
    print(f"Skipped:   {skipped}")
    print(f"Errors:    {errors}")

    client.close()


def main():
    parser = argparse.ArgumentParser(description="Process WhatsApp payload JSON file(s) or directory")
    parser.add_argument("path", help="Path to a JSON file or directory containing JSON payload files")
    args = parser.parse_args()
    asyncio.run(process_path(args.path))


if __name__ == "__main__":
    main()
