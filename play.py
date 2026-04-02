import json
import requests
import time

API_KEY = "htr4t0oy2e6um0eeqjt9owlx1crab3se"
SOURCE = "918800171823"
TEMPLATE_ID = "02d0680f-4409-4f13-8021-03a3038095c9"

url = "https://api.gupshup.io/wa/api/v1/template/msg"

headers = {
    "apikey": API_KEY,
    "Content-Type": "application/x-www-form-urlencoded"
}

for i in ["919391198374"]:
    template_data = {
        "id": TEMPLATE_ID,
        "language": "hi",   # MUST match dashboard
        "params": ["CareEco Test Supplier Pvt Ltd", 'CareEco Test Supplier Pvt Ltd hi hello how are you man', "WSG9/EBS/0000003"]
    }

    payload = {
        "source": SOURCE,
        "destination": i,
        "channel": "whatsapp",
        "template": json.dumps(template_data)
    }

    response = requests.post(url, data=payload, headers=headers)
    print(i, response.text)

    time.sleep(1)