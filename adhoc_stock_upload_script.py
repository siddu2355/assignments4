import re
import json
import base64
import imghdr
from google import genai
import functions_framework
from google.genai import types
from google.cloud import pubsub_v1

ENV = "test"
GEMINI_MODEL = "gemini-2.5-flash-lite"

BROADCAST_TOPIC_PATH = f"{ENV}_audit_broadcast_topic"

PRODUCT_NAME_TAG = "Product name"
OTHER_PRODUCT_DATA_TAG = "MRP/Batch..etc"

project_id = "curepoint-deployment-380107"
publisher = pubsub_v1.PublisherClient()  # No credentials parameter needed here cause of same project!
topic_path = publisher.topic_path(project_id, BROADCAST_TOPIC_PATH)

@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    message_data = cloud_event.data.get("message", {}).get("data")
    decoded_msg = ""
    if message_data:
        decoded_msg = base64.b64decode(message_data).decode("utf-8")
        print(f"Received message: {decoded_msg}")
    
    data = json.loads(decoded_msg)

    images_b64 = []
    
    img1 = data.get("image1", None)
    img2 = data.get("image2", None)
    
    if img1:
        images_b64.append(img1)
    if img2:
        images_b64.append(img2)
        
    # tags = [data.get("tag1"), data.get("tag2")]

    text_prompt = """
        You are an Image-Text extraction system. Extract structured data from product packs where manufacturing, expiry, batch, and MRP information is printed on the back side.

        Return ONLY valid JSON and nothing else with this schema:
        {
            "items": [
                {"product_name" : "N/A", "expiry_date": "N/A", "batch": "N/A", "mrp": "N/A"}
            ]
        }

        Rules:
         - "Use only the visible printed text from the image.",
         - "For 'product_name': Extract the medicine brand or product name (usually printed in bold/red). If not visible, return 'N/A'.",
         - "For 'batch': Look for keywords like 'B.No', 'Batch No', 'Batch', etc. Return only the alphanumeric code that follows. Do not include the keyword itself.",
         - "For 'expiry_date': Only take the value next to 'EXP', 'Exp.', 'Expiry', 'Expiry Date'. DO NOT confuse 'MFG' with expiry. Return only the date portion in the format as-is (e.g., '05/2027').",
         - "For 'mrp': Look for 'M.R.P.', 'MRP', 'Price'. Extract only the numeric value (digits and decimals if present). Do not include Rs, ₹, or text like 'Incl. of all taxes'.",
         - "If any field is not present in the image, return 'N/A'.",
         - "Output must strictly match the schema and be valid JSON with no extra text."
  
    """

    output = {
        "product_name" : "N/A", 
        "expiry_date": "N/A",
        "batch": "N/A", 
        "mrp": "N/A"
    }

    parsed_json = generate_from_images(images_b64, text_prompt)
    
    # for idx, item in enumerate(parsed_json["items"]):
    #     if tags[idx] == PRODUCT_NAME_TAG:
    #         output["product_name"] = item["product_name"]
    #     else:
    #         del item["product_name"]
    #         output.update(item)
    
    output.update(parsed_json["items"][0])
    
    print("final output: ", output)
    
    final_data = {
        "storeName": data["storeName"],
        "uniqueID": data["uniqueID"],
        "message": "stock audit complete",
        "images_count": len(images_b64),
        "status": "success",
    }
    final_data.update(output)
    
    message_json = json.dumps(final_data)
    message_bytes = message_json.encode("utf-8")

    future = publisher.publish(
        topic_path,
        message_bytes
    )
    print(f"Published message with ID: {future.result()}")


def safe_parse_json(response_text: str):
    """Safely parse JSON from the model output without json-repair."""
    if not response_text:
        return None
    m = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", response_text)
    if not m:
        print("[WARN] No JSON-like substring found in model output.")
        return None
    candidate = m.group(0)

    try:
        return json.loads(candidate)
    except Exception:
        # Fallback parsing
        fallback = candidate.replace("'", '"')
        fallback = re.sub(r",\s*}", "}", fallback)
        fallback = re.sub(r",\s*]", "]", fallback)
        try:
            return json.loads(fallback)
        except Exception as e:
            print("[ERROR] Failed to parse JSON:", e)
            print("Candidate output:", candidate)
            return None


def generate_from_images(images_b64, text_prompt):
    """Call Gemini model and return parsed JSON."""
    genai_client = genai.Client(
        vertexai=True,
        project="curepoint-deployment-380107",
        location="us-central1",
    )

    try:
        text_part = types.Part.from_text(text=text_prompt)
    except Exception:
        text_part = types.Part(text=text_prompt)

    image_parts = []
    for b64 in images_b64:
        img_bytes = base64.b64decode(b64)
        img_type = imghdr.what(None, h=img_bytes)
        mime = f"image/{img_type}" if img_type else "application/octet-stream"
        try:
            image_parts.append(types.Part.from_bytes(data=img_bytes, mime_type=mime))
        except Exception:
            image_parts.append(types.Part(bytes=img_bytes))

    contents = [types.Content(role="user", parts=[text_part] + image_parts)]

    config = types.GenerateContentConfig(
        temperature=0,
        top_p=0.95,
        max_output_tokens=2000,
        response_modalities=["TEXT"],
        response_mime_type="application/json",
    )

    full_text = ""
    for chunk in genai_client.models.generate_content_stream(
        model=GEMINI_MODEL,
        contents=contents,
        config=config,
    ):
        if chunk.candidates:
            for cand in chunk.candidates:
                if getattr(cand, "content", None) and getattr(cand.content, "parts", None):
                    for part in cand.content.parts:
                        if getattr(part, "text", None):
                            full_text += part.text

    return safe_parse_json(full_text)