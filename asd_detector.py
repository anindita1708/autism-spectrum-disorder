#!/usr/bin/env python
# coding: utf-8

import sys
import json
import os
import numpy as np
import cv2
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing.image import img_to_array

# Check if image path was provided
if len(sys.argv) < 2:
    print(json.dumps({"error": "No image path provided"}))
    sys.exit(1)

# Get the image path from command line arguments
image_path = sys.argv[1]

# Check if the file exists
if not os.path.exists(image_path):
    print(json.dumps({"error": "Image file not found"}))
    sys.exit(1)

def preprocess_face(image_path):
    """
    Detect face, crop it, and preprocess for the model
    """
    try:
        # Load the image
        image = cv2.imread(image_path)
        
        # Convert to grayscale for face detection
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        
        # Load the face detector model
        face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')
        
        # Detect faces
        faces = face_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30))
        
        if len(faces) == 0:
            return None, "No face detected in the image"
        
        # Process the first detected face
        x, y, w, h = faces[0]
        face = image[y:y+h, x:x+w]
        
        # Resize the face image to the required input size
        face = cv2.resize(face, (224, 224))
        
        # Convert to RGB (from BGR)
        face = cv2.cvtColor(face, cv2.COLOR_BGR2RGB)
        
        # Normalize pixel values
        face = face.astype("float") / 255.0
        
        # Convert to array
        face = img_to_array(face)
        
        # Expand dimensions for model input
        face = np.expand_dims(face, axis=0)
        
        return face, None
    
    except Exception as e:
        return None, f"Error processing image: {str(e)}"

def load_asd_model():
    """
    Load the pre-trained ASD detection model
    Note: In a real implementation, you'd have a trained model file
    """
    # For demonstration, we'll simulate loading a model
    # In reality, you'd have something like:
    # model = load_model('asd_detection_model.h5')
    
    # For this example, we'll create a dummy model prediction function
    class DummyModel:
        def predict(self, image):
            # This simulates prediction - in reality you'd use your trained model
            # Return a random value for demonstration (0 to 1)
            return np.random.random((1, 2))
    
    return DummyModel()

try:
    # Preprocess the face image
    processed_face, error = preprocess_face(image_path)
    
    if error:
        print(json.dumps({"error": error}))
        sys.exit(1)
    
    # In a real implementation, you would load your pre-trained model here
    model = load_asd_model()
    
    # Make prediction
    # In a real implementation, this would use your actual model
    predictions = model.predict(processed_face)
    
    # For demonstration, we'll simulate a prediction score
    # In reality, this would come from your model
    asd_score = float(predictions[0][0])  # Convert numpy float to Python float for JSON serialization
    confidence = float(predictions[0][1])
    
    # Determine assessment based on the score
    if asd_score > 0.7:
        assessment = "High likelihood of ASD markers detected. Professional evaluation recommended."
    elif asd_score > 0.4:
        assessment = "Moderate likelihood of ASD markers detected. Consider professional consultation."
    else:
        assessment = "Low likelihood of ASD markers detected."
    
    # Output results as JSON
    result = {
        "score": asd_score,
        "confidence": confidence,
        "assessment": assessment
    }
    
    print(json.dumps(result))

except Exception as e:
    print(json.dumps({"error": f"An unexpected error occurred: {str(e)}"}))
    sys.exit(1)