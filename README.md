# Autism Spectrum Disorder (ASD) Screening Tool

A prototype end-to-end web application for screening autism spectrum disorder indicators, combining a machine learning pipeline with a simple web interface.

## Overview

This project explores using facial image analysis to assist in early ASD screening. It includes:
- A **Jupyter notebook** (`ASD.ipynb`) with data analysis and model experimentation
- A **web interface** (`index.html`) for users to submit input
- A **PHP backend** (`process.php`) that handles form submission
- A **Python detection script** (`asd_detector.py`) that performs face detection and preprocessing, feeding into a CNN-based classifier

⚠️ **Note:** This is a work-in-progress prototype, not a diagnostic tool. It is not intended to provide medical diagnoses and should not be used as a substitute for professional clinical evaluation.

## How It Works

1. User submits an image through the web form (`index.html`)
2. `process.php` passes the image to the Python backend
3. `asd_detector.py` uses OpenCV's Haar Cascade classifier to detect and crop the face from the image
4. The cropped face is preprocessed (resized to 224x224, normalized) and passed to a CNN model for classification
5. Results are returned as a JSON response with a likelihood score and a suggested next step (e.g., "consider professional consultation")

## Tech Stack

- **Backend:** Python, PHP
- **ML/CV:** OpenCV (Haar Cascade face detection), TensorFlow/Keras (CNN classification)
- **Data Analysis:** Jupyter Notebook, pandas, NumPy
- **Frontend:** HTML

## Project Status

- [x] Web form and backend integration
- [x] Face detection and image preprocessing pipeline
- [x] Data exploration and model experimentation (see `ASD.ipynb`)
- [ ] Trained model integration (classification currently uses a placeholder in `asd_detector.py`)
- [ ] Model evaluation and validation on held-out test data

## Setup

To run the full web app, host `index.html` and `process.php` on a PHP-enabled server (e.g. XAMPP, or any Apache/PHP setup) with Python accessible from the PHP process.

## Future Improvements

- Replace the placeholder prediction logic with a trained, validated CNN model
- Add model evaluation metrics (accuracy, precision, recall, confusion matrix)
- Add input validation and error handling on the frontend
- Include a disclaimer and consent flow given the sensitive nature of the screening use case

## Disclaimer

This tool is for educational/research purposes only. It is not a certified medical device and should not be used for actual clinical diagnosis. Any real-world ASD screening should be conducted by qualified healthcare professionals.
