import json
import requests

url = "https://m0j3uzp9.execute-api.us-east-2.amazonaws.com/poc/reports/test-python.json"

payload = {
    "test": "python-connection"
}

headers = {
    "Content-Type": "application/json"
}

# verify=False mimics the -k flag (insecure/skip SSL verification) from curl
response = requests.put(
    url=url,
    headers=headers,
    data=json.dumps(payload),
    verify=False
)

print(f"Status Code: {response.status_code}")
print(f"Response Body: {response.text}")
