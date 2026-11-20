import io
import os
import sys
import hashlib
import base64
import json
import jsonpickle
import redis
from flask import Flask, request, jsonify, send_file
from minio import Minio
from minio.error import S3Error

app = Flask(__name__)

# Configuration via environment variables
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "rootuser")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "rootpass123")

INPUT_BUCKET = os.getenv("MINIO_INPUT_BUCKET", "queue")
OUTPUT_BUCKET = os.getenv("MINIO_OUTPUT_BUCKET", "output")

# Redis keys
WORKER_QUEUE_KEY = "toWorker"
LOGGING_KEY = "logging"

r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)


def ensure_bucket(bucket_name: str):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)


def log_message(level: str, message: str):
    """Push structured logs to the Redis logging list."""
    entry = f"{sys.argv[0]}:{level}:{message}"
    print(entry, flush=True)
    try:
        r.lpush(LOGGING_KEY, entry)
    except Exception as e:
        print(f"Failed to write log to Redis: {e}", flush=True)


@app.route("/", methods=["GET"])
def hello():
    return "<h1>Music Separation Server</h1><p>Use a valid endpoint</p>", 200


@app.route("/apiv1/separate", methods=["POST"])
def separate():
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

    songhash = hashlib.sha224(mp3_bytes).hexdigest()

    # Upload raw MP3 to MinIO INPUT_BUCKET
    try:
        ensure_bucket(INPUT_BUCKET)
        minio_client.put_object(
            INPUT_BUCKET,
            f"{songhash}.mp3",
            io.BytesIO(mp3_bytes),
            len(mp3_bytes),
            content_type="audio/mpeg",
        )
    except Exception as e:
        log_message("ERROR", f"MinIO upload failed for {songhash}: {e}")
        return jsonify({"error": f"Storage error: {e}"}), 500

    # Push lightweight job metadata onto Redis (without giant base64 blob)
    task = {
        "songhash": songhash,
        "bucket": INPUT_BUCKET,
        "model": model,
        "callback": callback,
    }

    r.rpush(WORKER_QUEUE_KEY, json.dumps(task))
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
            decoded = json.loads(item)
            if isinstance(decoded, dict) and "songhash" in decoded:
                queued_hashes.append(decoded["songhash"])
            else:
                queued_hashes.append(str(decoded))
        except Exception:
            queued_hashes.append(item)

    return jsonify({"queue": queued_hashes}), 200


@app.route("/apiv1/track/<songhash>/<track>", methods=["GET"])
def get_track(songhash: str, track: str):
    """Retrieve a separated track from MinIO OUTPUT_BUCKET as an MP3 download."""
    filename = track if track.endswith(".mp3") else f"{track}.mp3"
    object_name = f"{songhash}/{filename}"

    try:
        response = minio_client.get_object(OUTPUT_BUCKET, object_name)
        audio_bytes = response.read()
        response.close()
        response.release_conn()
    except S3Error:
        log_message("WARN", f"Track not found in MinIO: {OUTPUT_BUCKET}/{object_name}")
        return jsonify({"error": f"Track '{filename}' for hash '{songhash}' not found"}), 404
    except Exception as e:
        log_message("ERROR", f"MinIO read error for {object_name}: {e}")
        return jsonify({"error": str(e)}), 500

    log_message("INFO", f"Serving track {OUTPUT_BUCKET}/{object_name}")
    return send_file(
        io.BytesIO(audio_bytes),
        mimetype="audio/mpeg",
        as_attachment=True,
        download_name=filename,
    )


@app.route("/apiv1/remove/<songhash>/<track>", methods=["GET", "DELETE"])
def remove_track(songhash: str, track: str):
    """Remove the corresponding separated track object from MinIO OUTPUT_BUCKET."""
    filename = track if track.endswith(".mp3") else f"{track}.mp3"
    object_name = f"{songhash}/{filename}"

    try:
        minio_client.stat_object(OUTPUT_BUCKET, object_name)
        minio_client.remove_object(OUTPUT_BUCKET, object_name)
        log_message("INFO", f"Removed track {OUTPUT_BUCKET}/{object_name}")
        return jsonify({"status": "removed", "hash": songhash, "track": filename}), 200
    except S3Error:
        return jsonify({"error": f"Track '{filename}' for hash '{songhash}' not found"}), 404
    except Exception as e:
        log_message("ERROR", f"Failed to remove {object_name}: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
