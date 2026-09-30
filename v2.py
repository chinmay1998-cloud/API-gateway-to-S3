import hashlib
import json
import uuid
import subprocess
import boto3
import urllib3

# Suppress SSL warnings because corporate Zscaler/VPN
# is intercepting HTTPS traffic.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# =========================================================
# CONFIGURATION
# =========================================================

REGION = "us-east-2"

# Use the API ID that you successfully tested with curl
API_ID = "m0jqz3uzp9"

STAGE_NAME = "poc"

S3_PREFIX = "reports"

BUCKET_NAME = "lmig-grs-dev-diai-poc-data-lake-reports-833390816381"

API_URL = (
    f"https://{API_ID}.execute-api.{REGION}.amazonaws.com"
    f"/{STAGE_NAME}/{S3_PREFIX}"
)


# =========================================================
# SHA-256
# =========================================================

def compute_sha256(data_bytes):
    return hashlib.sha256(data_bytes).hexdigest()


# =========================================================
# UPLOAD TO API GATEWAY USING CURL
# =========================================================

def upload_to_api_gateway(upload_url, raw_bytes):

    print("\n[*] Uploading file to API Gateway...")
    print(f"[*] URL: {upload_url}")

    curl_command = [
        "curl.exe",
        "-k",
        "-sS",
        "-X",
        "PUT",
        upload_url,
        "-H",
        "Content-Type: application/json",
        "--data-binary",
        "@-"
    ]

    try:

        result = subprocess.run(
            curl_command,
            input=raw_bytes,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=60
        )

        response_body = result.stdout.decode(
            "utf-8",
            errors="replace"
        )

        error_output = result.stderr.decode(
            "utf-8",
            errors="replace"
        )

        print(f"[*] curl return code: {result.returncode}")

        if error_output:
            print(f"[*] curl message: {error_output}")

        if result.returncode != 0:

            print("\n[!] API Gateway upload failed.")
            print(f"[!] curl error: {error_output}")

            return False

        print("[+] API Gateway upload completed.")
        print(f"[*] API response: {response_body}")

        return True

    except subprocess.TimeoutExpired:

        print("[!] API Gateway request timed out.")

        return False

    except Exception as e:

        print(f"[!] API Gateway error: {e}")

        return False


# =========================================================
# VERIFY OBJECT IN S3
# =========================================================

def verify_s3_object(filename, sent_size, sent_hash):

    s3_key = f"{S3_PREFIX}/{filename}"

    print("\n[*] Checking S3 object...")
    print(f"[*] Bucket: {BUCKET_NAME}")
    print(f"[*] Key: {s3_key}")

    try:

        # verify=False is required temporarily because
        # corporate Zscaler SSL certificate is not trusted
        # by Python/boto3 on this machine.
        s3_client = boto3.client(
            "s3",
            region_name=REGION,
            verify=False
        )

        # First check that the object exists
        s3_obj = s3_client.get_object(
            Bucket=BUCKET_NAME,
            Key=s3_key
        )

        downloaded_bytes = s3_obj["Body"].read()

        received_size = len(downloaded_bytes)

        received_hash = compute_sha256(
            downloaded_bytes
        )

        print("\n[*] S3 verification results")
        print("---------------------------------------")
        print(f"[*] Sent size      : {sent_size} bytes")
        print(f"[*] Received size  : {received_size} bytes")
        print(f"[*] Sent SHA-256   : {sent_hash}")
        print(f"[*] S3 SHA-256     : {received_hash}")
        print("---------------------------------------")

        size_match = sent_size == received_size

        hash_match = sent_hash == received_hash

        if size_match and hash_match:

            print("\n=======================================")
            print(" SUCCESS: ZERO DATA LOSS")
            print("=======================================")
            print("Payload matches S3 bit-for-bit.")

            return True

        print("\n=======================================")
        print(" INTEGRITY FAILURE")
        print("=======================================")

        if not size_match:

            print(
                f"[!] Size mismatch: "
                f"{sent_size} vs {received_size}"
            )

        if not hash_match:

            print("[!] SHA-256 checksum mismatch.")

        return False

    except Exception as e:

        print("\n[!] S3 verification failed.")
        print(f"[!] Error: {e}")

        return False


# =========================================================
# MAIN PIPELINE TEST
# =========================================================

def run_pipeline_test():

    print("\n=======================================")
    print(" API GATEWAY -> S3 INGESTION TEST")
    print("=======================================")

    # -----------------------------------------------------
    # Generate unique filename
    # -----------------------------------------------------

    filename = (
        f"report-{uuid.uuid4().hex[:8]}.json"
    )

    upload_url = (
        f"{API_URL}/{filename}"
    )

    # -----------------------------------------------------
    # Create test payload
    # -----------------------------------------------------

    payload_data = {

        "reportId": str(uuid.uuid4()),

        "status": "PROCESSED",

        "metrics": {

            "records": 50000,

            "errorCount": 0,

            "latencyMs": 142.8
        },

        "description":
            "POC data validation for direct "
            "API Gateway to S3 streaming."
    }

    raw_bytes = json.dumps(
        payload_data,
        separators=(",", ":")
    ).encode("utf-8")

    sent_size = len(raw_bytes)

    sent_hash = compute_sha256(
        raw_bytes
    )

    print("\n[*] Test file:")
    print(f"    {filename}")

    print("\n[*] Payload size:")
    print(f"    {sent_size} bytes")

    print("\n[*] Payload SHA-256:")
    print(f"    {sent_hash}")

    # -----------------------------------------------------
    # Upload using curl
    # -----------------------------------------------------

    upload_success = upload_to_api_gateway(
        upload_url,
        raw_bytes
    )

    if not upload_success:

        print("\n[!] Stopping pipeline test.")

        return

    # -----------------------------------------------------
    # Verify directly from S3
    # -----------------------------------------------------

    verify_s3_object(
        filename,
        sent_size,
        sent_hash
    )


# =========================================================
# PROGRAM ENTRY
# =========================================================

if _name_ == "_main_":

    run_pipeline_test()
