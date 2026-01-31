import cv2
import requests
import numpy as np
import pytesseract

# Set the path to Tesseract executable
pytesseract.pytesseract.tesseract_cmd = r'C:\\Program Files\\Tesseract-OCR\\tesseract.exe'

# URL of the image
image_url = 'https://i.stack.imgur.com/i1Abv.png'

# Download the image from the URL
response = requests.get(image_url)
image_array = np.frombuffer(response.content, dtype=np.uint8)

# Decode the image array to OpenCV format
image = cv2.imdecode(image_array, cv2.IMREAD_COLOR)

# Convert the image to grayscale
gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

# Resize the image
gray = cv2.resize(gray, None, fx=2, fy=2)

# Recognize text using OCR
text = pytesseract.image_to_string(gray)

# Print the recognized text
print(text)