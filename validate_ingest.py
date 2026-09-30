import hashlib
import json
import uuid
import boto3
import requests

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------
REGION = "ap-south-1"  #
API_ID = "dv5yemzeed"  #
STAGE_NAME = "poc"  # Replace with your deployed stage name
BUCKET_NAME = "poc-data-lake-reports-027742774025"  #
S3_PREFIX = "reports"  #[cite: 1, 3]

API_URL = f"https://{API_ID}.execute-api.{REGION}.amazonaws.com/{STAGE_NAME}/{S3_PREFIX}"  #[cite: 3]


def compute_sha256(data_bytes: bytes) -> str:
    """Computes SHA-256 hash to detect bit-level data loss or modification."""
    return hashlib.sha256(data_bytes).hexdigest()


def run_pipeline_test():
    filename = f"report-{uuid.uuid4().hex[:8]}.json"
    upload_url = f"{API_URL}/{filename}"  #[cite: 3]

    # 1. Generate sample payload
    payload_data = {
        "reportId": str(uuid.uuid4()),
        "status": "PROCESSED",
        "metrics": {
            "records": 50000,
            "errorCount": 0,
            "latencyMs": 142.8
        },
        "description": "POC data validation for direct API Gateway to S3 streaming."
    }
    raw_bytes = json.dumps(payload_data, separators=(",", ":")).encode("utf-8")
    sent_hash = compute_sha256(raw_bytes)
    sent_size = len(raw_bytes)

    print(f"[*] Preparing test payload: {filename} ({sent_size} bytes)")
    print(f"[*] Sent Payload SHA-256: {sent_hash}")

    # 2. Push Data to API Gateway (PUT)
    headers = {"Content-Type": "application/json"}
    print(f"[*] Uploading to API Gateway: {upload_url}")
    response = requests.put(upload_url, data=raw_bytes, headers=headers)

    print(f"[*] API Gateway Response: HTTP {response.status_code}")
    if response.status_code not in (200, 201):
        print(f"[!] Upload Failed: {response.text}")
        return

    # 3. Retrieve directly from S3 to verify persistence and integrity
    s3_key = f"{S3_PREFIX}/{filename}"  #[cite: 1, 3]
    print(f"[*] Fetching uploaded file directly from s3://{BUCKET_NAME}/{s3_key}...")

    s3_client = boto3.client("s3", region_name=REGION)  #[cite: 3]
    try:
        s3_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)  #[cite: 1, 3]
        downloaded_bytes = s3_obj["Body"].read()
        received_hash = compute_sha256(downloaded_bytes)
        received_size = len(downloaded_bytes)

        # 4. Check for Data Loss / Truncation
        print(f"[*] Received S3 Payload Size: {received_size} bytes")
        print(f"[*] Received S3 SHA-256:     {received_hash}")

        size_match = sent_size == received_size
        hash_match = sent_hash == received_hash

        if size_match and hash_match:
            print("\n SUCCESS: Zero data loss detected! Payloads match bit-for-bit.")
        else:
            print("\n INTEGRITY FAILURE: Data loss or corruption occurred.")
            if not size_match:
                print(f"    - Size mismatch: sent {sent_size} vs received {received_size}")
            if not hash_match:
                print("    - Checksum mismatch between original and S3 object.")

    except Exception as e:
        print(f"[!] Error verifying S3 object: {str(e)}")


if __name__ == "__main__":
    run_pipeline_test()