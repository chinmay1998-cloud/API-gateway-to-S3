import argparse
import hashlib
import json
import os
import sys
import threading
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

import boto3
import requests

# ==============================================================================
# CONFIGURATION -- supply your environment values below
# ==============================================================================

# AWS region hosting both the API Gateway and the S3 bucket
REGION = "us-east-13"

# REST API ID from the API Gateway console (the subdomain)
API_ID = "3"

# Deployed stage name, e.g. "poc"
STAGE_NAME = "dev"

# API Gateway resource path that accepts the POST
RESOURCE_PATH = "venti-event"

# Destination bucket configured in the S3 integration
BUCKET_NAME = ""

# Must match the folder in the integration Path Override ({bucket}/reports/venti-event/{item})
S3_PREFIX = "reports/venti-event"

# TLS verification for the API Gateway POST
TLS_VERIFY = True

# Zscaler intercepts AWS service endpoints, so boto3 must trust corporate root CA
AWS_CA_BUNDLE = os.path.expanduser("~/.certificates/zscaler.pem")
if os.path.exists(AWS_CA_BUNDLE):
    os.environ["AWS_CA_BUNDLE"] = AWS_CA_BUNDLE
    os.environ["REQUESTS_CA_BUNDLE"] = AWS_CA_BUNDLE

DEFAULT_FILE_COUNT = 1_000
DEFAULT_WORKERS = 20
REQUEST_TIMEOUT_SECONDS = 60

# POST goes to collection endpoint; API Gateway names the object itself
API_URL = f"https://{API_ID}.execute-api.{REGION}.amazonaws.com/{STAGE_NAME}/{RESOURCE_PATH}"

# Thread-safe lock for printing console output
print_lock = threading.Lock()


# ==============================================================================
# SHA-256
# ==============================================================================

def compute_sha256(data_bytes: bytes) -> str:
    return hashlib.sha256(data_bytes).hexdigest()


# ==============================================================================
# UPLOAD TO API GATEWAY
# ==============================================================================

def upload_to_api_gateway(raw_bytes: bytes, verbose: bool = True):
    """POST the payload and return the resolved S3 key, or None on failure."""
    if verbose:
        with print_lock:
            print("\n[*] Uploading payload to API Gateway...")
            print(f"[*] URL: {API_URL}")

    try:
        response = requests.post(
            API_URL,
            data=raw_bytes,
            headers={"Content-Type": "application/json"},
            timeout=REQUEST_TIMEOUT_SECONDS,
            verify=TLS_VERIFY
        )
    except requests.RequestException as e:
        if verbose:
            with print_lock:
                print(f"[!] API Gateway request error: {e}")
        return None

    if response.status_code not in (200, 201):
        if verbose:
            with print_lock:
                print("\n[!] API Gateway upload failed.")
                print(f"[!] Status code : {response.status_code}")
                print(f"[!] Body        : {response.text}")
                print(f"[!] Headers     : {dict(response.headers)}")
        return None

    # Retrieve API Gateway Request ID to locate file in S3
    request_id = (
        response.headers.get("x-amzn-requestid")
        or response.headers.get("x-amz-request-id")
        or response.headers.get("apigw-requestid")
    )

    if not request_id:
        if verbose:
            with print_lock:
                print("\n[!] No request ID header found in API Gateway response.")
                for name, value in response.headers.items():
                    print(f"    {name}: {value}")
        return None

    s3_key = f"{S3_PREFIX}/{request_id}"

    if verbose:
        with print_lock:
            print(f"[+] API Gateway upload completed (HTTP {response.status_code}).")
            print(f"[*] Request ID : {request_id}")
            print(f"[*] S3 key     : {s3_key}")

    return s3_key


# ==============================================================================
# VERIFY OBJECT IN S3
# ==============================================================================

def verify_s3_object(s3_key: str, sent_size: int, sent_hash: str, s3_client=None, verbose: bool = True) -> bool:
    if verbose:
        with print_lock:
            print("\n[*] Checking S3 object...")
            print(f"[*] Bucket: {BUCKET_NAME}")
            print(f"[*] Key   : {s3_key}")

    try:
        if s3_client is None:
            s3_client = boto3.client(
                "s3",
                region_name=REGION,
                verify=AWS_CA_BUNDLE if os.path.exists(AWS_CA_BUNDLE) else True
            )

        # Allow slight eventual-consistency buffer for S3 metadata propagation
        max_retries = 3
        for attempt in range(max_retries):
            try:
                s3_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
                break
            except s3_client.exceptions.NoSuchKey:
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                else:
                    raise

        downloaded_bytes = s3_obj["Body"].read()
        received_size = len(downloaded_bytes)
        received_hash = compute_sha256(downloaded_bytes)

        if verbose:
            with print_lock:
                print("\n[*] S3 verification results")
                print("---------------------------------------------")
                print(f"[*] Sent size     : {sent_size} bytes")
                print(f"[*] Received size : {received_size} bytes")
                print(f"[*] Sent SHA-256   : {sent_hash}")
                print(f"[*] S3 SHA-256     : {received_hash}")
                print("---------------------------------------------")

        size_match = sent_size == received_size
        hash_match = sent_hash == received_hash

        if size_match and hash_match:
            if verbose:
                with print_lock:
                    print("\n=============================================")
                    print("SUCCESS: ZERO DATA LOSS")
                    print("=============================================")
                    print("Payload matches S3 bit-for-bit.")
            return True

        if verbose:
            with print_lock:
                print("\n=============================================")
                print("INTEGRITY FAILURE")
                print("=============================================")
                if not size_match:
                    print(f"[!] Size mismatch: {sent_size} vs {received_size}")
                if not hash_match:
                    print("[!] SHA-256 checksum mismatch.")
        return False

    except Exception as e:
        if verbose:
            with print_lock:
                print(f"\n[!] S3 verification failed: {e}")
        return False


# ==============================================================================
# PAYLOAD GENERATOR
# ==============================================================================

def create_payload() -> bytes:
    payload_data = {
        "reportId": str(uuid.uuid4()),
        "status": "PROCESSED",
        "metrics": {
            "records": 50000,
            "errorCount": 0,
            "latencyMs": 142.8
        },
        "description": (
            "POC data validation for direct "
            "API Gateway to S3 streaming."
        ),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }
    return json.dumps(payload_data, separators=(",", ":")).encode("utf-8")


# ==============================================================================
# PIPELINE WORKER (FOR BATCH / 1000 FILES)
# ==============================================================================

def worker_task(file_index: int, s3_client) -> dict:
    payload_bytes = create_payload()
    sent_size = len(payload_bytes)
    sent_hash = compute_sha256(payload_bytes)

    # In batch mode, keep verbose=False to prevent terminal corruption
    s3_key = upload_to_api_gateway(payload_bytes, verbose=False)
    if not s3_key:
        return {"index": file_index, "status": "UPLOAD_FAILED", "s3_key": None}

    is_valid = verify_s3_object(s3_key, sent_size, sent_hash, s3_client=s3_client, verbose=False)
    if is_valid:
        return {"index": file_index, "status": "SUCCESS", "s3_key": s3_key}
    else:
        return {"index": file_index, "status": "INTEGRITY_FAILED", "s3_key": s3_key}


# ==============================================================================
# MAIN TEST HARNESS
# ==============================================================================

def run_single_test():
    print("\n--- Running Single End-to-End Validation ---")
    payload = create_payload()
    size = len(payload)
    sha = compute_sha256(payload)

    s3_key = upload_to_api_gateway(payload, verbose=True)
    if s3_key:
        verify_s3_object(s3_key, size, sha, verbose=True)
    else:
        print("[!] Aborting verification due to upload failure.")


def run_batch_test(total_files: int, max_workers: int):
    print(f"\n==================================================================")
    print(f"Starting Ingestion Batch: {total_files} files with {max_workers} concurrent workers")
    print(f"Target Endpoint : {API_URL}")
    print(f"Target Bucket   : s3://{BUCKET_NAME}/{S3_PREFIX}/")
    print(f"==================================================================\n")

    s3_client = boto3.client(
        "s3",
        region_name=REGION,
        verify=AWS_CA_BUNDLE if os.path.exists(AWS_CA_BUNDLE) else True
    )

    success_count = 0
    upload_fail_count = 0
    integrity_fail_count = 0

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker_task, i, s3_client): i for i in range(1, total_files + 1)}
        completed = 0

        while futures:
            done, _ = wait(futures, return_when=FIRST_COMPLETED)
            for future in done:
                completed += 1
                result = future.result()
                if result["status"] == "SUCCESS":
                    success_count += 1
                elif result["status"] == "UPLOAD_FAILED":
                    upload_fail_count += 1
                else:
                    integrity_fail_count += 1

                # Clean progress tracker
                percent = (completed / total_files) * 100
                sys.stdout.write(
                    f"\rProgress: [{completed}/{total_files}] ({percent:.1f}%) | "
                    f"Success: {success_count} | Upload Fail: {upload_fail_count} | Integrity Fail: {integrity_fail_count}"
                )
                sys.stdout.flush()
                del futures[future]

    duration = time.time() - start_time
    rps = total_files / duration if duration > 0 else 0

    print(f"\n\n==================================================================")
    print(f"BATCH EXECUTION REPORT")
    print(f"==================================================================")
    print(f"Total Sent          : {total_files}")
    print(f"Successful (Exact)  : {success_count}")
    print(f"Upload Failures     : {upload_fail_count}")
    print(f"Integrity Failures  : {integrity_fail_count}")
    print(f"Total Time Taken    : {duration:.2f} seconds")
    print(f"Effective Rate      : {rps:.2f} requests/sec")
    print(f"Zero Data Loss      : {'YES' if success_count == total_files else 'NO'}")
    print(f"==================================================================\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="API Gateway -> S3 High-Scale Ingestion Tester")
    parser.add_argument("--batch", action="store_true", help="Run batch test (1000 files)")
    parser.add_argument("--count", type=int, default=DEFAULT_FILE_COUNT, help=f"Total file count (default: {DEFAULT_FILE_COUNT})")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help=f"Concurrency workers (default: {DEFAULT_WORKERS})")

    args = parser.parse_args()

    if args.batch:
        run_batch_test(total_files=args.count, max_workers=args.workers)
    else:
        # Defaults to 1 single verbose validation if no flags passed
        run_single_test()
