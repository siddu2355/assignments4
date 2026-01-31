import cv2
import numpy as np
import time
from keras.models import load_model
from keras.preprocessing import image

# Load pre-trained model
model_path = "models/cnn_face_recognizer.h5"
model = load_model(model_path)

# Define image dimensions used for training the model
img_height, img_width = 200, 200

# Load Haar Cascade for face detection
face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')

# Class indices mapping (to map prediction output to user labels)
class_indices = {0: "User1", 1: "User2", 2: "durga"}  # Example mapping
class_labels = {v: k for k, v in class_indices.items()}  # Reverse mapping

# Initialize webcam
camera = cv2.VideoCapture(0)

# Variable to store if access decision has been made
access_decision_made = False
decision_text = ""

while True:
    ret, frame = camera.read()
    if not ret:
        print("Failed to capture image. Exiting...")
        break

    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

    # Detect faces in the frame
    faces = face_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30))

    for (x, y, w, h) in faces:
        # Extract the face region
        face = frame[y:y+h, x:x+w]

        # Save the captured face image to disk
        face_filename = "detected_face.jpg"
        cv2.imwrite(face_filename, face)  # Save the face as an image

        # Preprocess the face for prediction
        resized_face = cv2.resize(face, (img_width, img_height))  # Resize to 200x200
        img_array = image.img_to_array(resized_face) / 255.0
        img_array = np.expand_dims(img_array, axis=0)

        # Make predictions
        predictions = model.predict(img_array)
        predicted_class = np.argmax(predictions)
        confidence = np.max(predictions)

        # Determine if access is granted or denied
        if confidence > 0.9:  # Threshold for valid prediction
            name = class_indices.get(predicted_class, "Unknown")
            decision_text = f"Access Granted: {name} ({confidence:.2f})"
            print(decision_text)
            cv2.putText(frame, decision_text, (x, y-10), cv2.FONT_HERSHEY_SIMPLEX, 0.9, (0, 255, 0), 2)
        else:
            decision_text = "Access Denied: Unknown"
            print(decision_text)
            cv2.putText(frame, decision_text, (x, y-10), cv2.FONT_HERSHEY_SIMPLEX, 0.9, (0, 0, 255), 2)

        # Draw rectangle around detected face
        cv2.rectangle(frame, (x, y), (x+w, y+h), (255, 0, 0), 2)

        # Mark decision as made and exit after the first face is processed
        access_decision_made = True
        break

    # If a decision is made, stop further processing
    if access_decision_made:
        break

    # Display the frame
    cv2.imshow("Vehicle Security System", frame)

    # Exit on pressing 'q'
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

# Display decision for 7 seconds
if access_decision_made:
    while True:
        # Read the saved image
        saved_image = cv2.imread(face_filename)
        
        # Preprocess the saved image for prediction
        resized_face = cv2.resize(saved_image, (img_width, img_height))  # Resize to 200x200
        img_array = image.img_to_array(resized_face) / 255.0
        img_array = np.expand_dims(img_array, axis=0)

        # Make predictions
        predictions = model.predict(img_array)
        predicted_class = np.argmax(predictions)
        confidence = np.max(predictions)
        print(predicted_class, confidence)
        # Determine if access is granted or denied
        if confidence == 1:  # Threshold for valid prediction
            name = class_indices.get(predicted_class, "Unknown")
            decision_text = f"Access Granted: {name} ({confidence:.2f})"
            print(decision_text)
        else:
            decision_text = "Access Denied: Unknown"
            print(decision_text)

        # Display the decision text on the saved image
        cv2.putText(saved_image, decision_text, (50, 50), cv2.FONT_HERSHEY_SIMPLEX, 1,
                    (0, 255, 0) if "Granted" in decision_text else (0, 0, 255), 2)
        cv2.imshow("Vehicle Security System", saved_image)

        # Exit after displaying for 7 seconds
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
        time.sleep(7)  # Display for 7 seconds
        break

camera.release()
cv2.destroyAllWindows()
