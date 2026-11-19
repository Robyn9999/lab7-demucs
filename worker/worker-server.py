import json
import os
import platform
import shutil
import sys
import redis
import requests
from minio import Minio

# Environment variables with localhost defaults for local port-forward testing
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

MINIO_HOST = os.getenv("MINIO_HOST", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "rootuser")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "rootpass123")

INPUT_BUCKET = os.getenv("MINIO_INPUT_BUCKET", "queue")
OUTPUT_BUCKET = os.getenv("MINIO_OUTPUT_BUCKET", "output")

infoKey = f"{platform.node()}.worker.info"
debugKey = f"{platform.node()}.worker.debug"

r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)


def log_debug(message):
    print("DEBUG:", message, file=sys.stdout)
    r.lpush("logging", f"{debugKey}:{message}")


def log_info(message):
    print("INFO:", message, file=sys.stdout)
    r.lpush("logging", f"{infoKey}:{message}")


def ensure_bucket(bucket_name):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)


def process_job(job_data):
    songhash = job_data.get("songhash")
    input_bucket = job_data.get("bucket", INPUT_BUCKET)
    callback = job_data.get("callback")

    if not songhash:
        log_debug(f"Invalid job payload missing songhash: {job_data}")
        return

    ensure_bucket(input_bucket)
    ensure_bucket(OUTPUT_BUCKET)

    input_object = f"{songhash}.mp3"
    local_input_path = f"/tmp/{songhash}.mp3"
    local_output_dir = "/tmp/output"

    log_info(f"Downloading {input_bucket}/{input_object} to {local_input_path}")
    minio_client.fget_object(input_bucket, input_object, local_input_path)

    # Run DEMUCS waveform separation
    cmd = f"python3 -m demucs.separate --mp3 --out {local_output_dir} {local_input_path}"
    log_info(f"Executing: {cmd}")
    exit_code = os.system(cmd)

    if exit_code != 0:
        log_debug(f"DEMUCS failed with exit code {exit_code} for {songhash}")
        return

    # Upload separated stems: bass, drums, vocals, other
    tracks = ["bass", "drums", "vocals", "other"]
    for track in tracks:
        track_filename = f"{track}.mp3"
        local_track_path = os.path.join(
            local_output_dir, "mdx_extra_q", songhash, track_filename
        )
        remote_object_name = f"{songhash}/{track_filename}"

        if os.path.exists(local_track_path):
            log_info(f"Uploading {remote_object_name} to bucket '{OUTPUT_BUCKET}'")
            minio_client.fput_object(
                OUTPUT_BUCKET,
                remote_object_name,
                local_track_path,
                content_type="audio/mpeg",
            )
        else:
            log_debug(f"Expected output file missing: {local_track_path}")

    # Trigger optional webhook callback
    if callback and isinstance(callback, dict) and callback.get("url"):
        url = callback["url"]
        payload = callback.get("data", {"songhash": songhash, "status": "complete"})
        try:
            log_info(f"Sending webhook POST to {url}")
            requests.post(url, json=payload, timeout=5)
        except Exception as exc:
            log_debug(f"Webhook callback failed for {url}: {exc}")

    # Clean up /tmp files to prevent pod disk exhaustion
    if os.path.exists(local_input_path):
        os.remove(local_input_path)
    song_out_dir = os.path.join(local_output_dir, "mdx_extra_q", songhash)
    if os.path.exists(song_out_dir):
        shutil.rmtree(song_out_dir)

    log_info(f"Completed separation for {songhash}")


def main():
    log_info(f"Worker started. Listening on Redis {REDIS_HOST}:{REDIS_PORT} -> toWorker")
    while True:
        try:
            # blpop returns a tuple: ('toWorker', '<json_string>')
            _, raw_msg = r.blpop("toWorker")
            log_debug(f"Received task: {raw_msg}")
            job_data = json.loads(raw_msg)
            process_job(job_data)
        except Exception as exc:
            log_debug(f"Worker loop exception: {exc}")


if __name__ == "__main__":
    main()