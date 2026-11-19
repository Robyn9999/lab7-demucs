import os
import sys
import hashlib
import base64
import json
import jsonpickle
import redis
from flask import Flask, request, jsonify, send_file

app = Flask(__name__)

# Configuration via environment variables (with sensible defaults)
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/data/output")
INPUT_DIR = os.getenv("INPUT_DIR", "/data/input")

# Redis keys
WORKER_QUEUE_KEY = "toWorker"
LOGGING_KEY = "logging"

# Connect to Redis DB 0
r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


def log_message(level: str, message: str):
    """Push structured logs to the Redis logging list."""
    entry = f"{sys.argv[0]}:{level}:{message}"
    print(entry, flush=True)
    try:
        r.lpush(LOGGING_KEY, entry)
    except Exception as e:
        print(f"Failed to write log to Redis: {e}", flush=True)


@app.route("/apiv1/separate", methods=["POST"])
def separate():
    """
    Expects JSON payload:
    {
        "mp3": "<base64 encoded mp3 string>",
        "model": "htdemucs",          # optional, defaults to htdemucs
        "callback": {                 # optional
            "url": "http://...",
            "data": {...}
        }
    }
    """
    payload = request.get_json(force=True, silent=True)
    if not payload or "mp3" not in payload:
        return jsonify({"error": "Missing 'mp3' base64 data in JSON payload"}), 400

    mp3_b64 = payload["mp3"]
    model = payload.get("model", "htdemucs")
    callback = payload.get("callback", None)

    try:
        mp3_bytes = base64.b64decode(mp3_b64)
    except Exception as e:
        log_message("ERROR", f"Base64 decode failed: {e}")
        return jsonify({"error": "Invalid base64 encoding"}), 400

    # Generate SHA-224 or SHA-256 hash (sample output shows 56 hex chars -> SHA-224)
    songhash = hashlib.sha224(mp3_bytes).hexdigest()

    # Save raw input MP3 (or pass via shared storage / Redis depending on worker design)
    input_path = os.path.join(INPUT_DIR, f"{songhash}.mp3")
    with open(input_path, "wb") as f:
        f.write(mp3_bytes)

    # Construct work task for the worker queue
    task = {
        "songhash": songhash,
        "model": model,
        "mp3": mp3_b64,          # Included if worker expects raw b64; remove if using shared volume only
        "input_path": input_path,
        "callback": callback
    }

    # Push serialized job onto the Redis worker list
    r.rpush(WORKER_QUEUE_KEY, jsonpickle.encode(task))
    log_message("INFO", f"Enqueued song {songhash} for separation with model {model}")

    return jsonify({
        "hash": songhash,
        "reason": "Song enqueued for separation"
    }), 200


@app.route("/apiv1/queue", methods=["GET"])
@app.route("/apiv1/queue/", methods=["GET"])
def get_queue():
    """Dump all queued song hashes from the Redis worker list."""
    raw_items = r.lrange(WORKER_QUEUE_KEY, 0, -1)
    queued_hashes = []

    for item in raw_items:
        try:
            decoded = jsonpickle.decode(item)
            if isinstance(decoded, dict) and "songhash" in decoded:
                queued_hashes.append(decoded["songhash"])
            else:
                queued_hashes.append(str(decoded))
        except Exception:
            queued_hashes.append(item)

    return jsonify({"queue": queued_hashes}), 200


@app.route("/apiv1/track/<songhash>/<track>", methods=["GET"])
def get_track(songhash: str, track: str):
    """
    Retrieve a separated track (bass.mp3, vocals.mp3, drums.mp3, other.mp3)
    as a binary MP3 download.
    """
    # Normalize filename (allow passing 'vocals' or 'vocals.mp3')
    filename = track if track.endswith(".mp3") else f"{track}.mp3"
    track_path = os.path.join(OUTPUT_DIR, songhash, filename)

    if not os.path.isfile(track_path):
        log_message("WARN", f"Track not found: {track_path}")
        return jsonify({"error": f"Track '{filename}' for hash '{songhash}' not found"}), 404

    log_message("INFO", f"Serving track {track_path}")
    return send_file(
        track_path,
        mimetype="audio/mpeg",
        as_attachment=True,
        download_name=filename
    )


@app.route("/apiv1/remove/<songhash>/<track>", methods=["GET", "DELETE"])
def remove_track(songhash: str, track: str):
    """Remove the corresponding separated track file."""
    filename = track if track.endswith(".mp3") else f"{track}.mp3"
    track_path = os.path.join(OUTPUT_DIR, songhash, filename)

    if not os.path.isfile(track_path):
        return jsonify({"error": f"Track '{filename}' for hash '{songhash}' not found"}), 404

    try:
        os.remove(track_path)
        log_message("INFO", f"Removed track {track_path}")
        return jsonify({"status": "removed", "hash": songhash, "track": filename}), 200
    except Exception as e:
        log_message("ERROR", f"Failed to remove {track_path}: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    # Listen on all interfaces on port 5000
    app.run(host="0.0.0.0", port=5000)