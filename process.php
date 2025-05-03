<?php
// Enable error reporting for debugging
error_reporting(E_ALL);
ini_set('display_errors', 1);

// Set headers to allow AJAX requests
header('Content-Type: application/json');

// Check if an image was uploaded
if (!isset($_FILES['image']) || $_FILES['image']['error'] !== UPLOAD_ERR_OK) {
    echo json_encode(['error' => 'No image uploaded or upload error occurred']);
    exit;
}

// Create an uploads directory if it doesn't exist
$upload_dir = 'uploads/';
if (!file_exists($upload_dir)) {
    mkdir($upload_dir, 0777, true);
}

// Generate a unique filename
$filename = uniqid() . '_' . basename($_FILES['image']['name']);
$upload_path = $upload_dir . $filename;

// Check file type
$allowed_types = ['image/jpeg', 'image/png', 'image/jpg'];
$file_type = $_FILES['image']['type'];

if (!in_array($file_type, $allowed_types)) {
    echo json_encode(['error' => 'Only JPG, JPEG, and PNG images are allowed']);
    exit;
}

// Move the uploaded file to the uploads directory
if (!move_uploaded_file($_FILES['image']['tmp_name'], $upload_path)) {
    echo json_encode(['error' => 'Failed to save the uploaded image']);
    exit;
}

// Prepare to call Python script
$python_script = 'asd_detector.py';
$command = "python $python_script \"$upload_path\"";

// Execute the Python script
$output = [];
$return_var = 0;
exec($command, $output, $return_var);

// Check if the Python script executed successfully
if ($return_var !== 0) {
    echo json_encode(['error' => 'Failed to process the image']);
    exit;
}

// Parse the output from the Python script
$result = json_decode(implode("\n", $output), true);

// If parsing failed, return an error
if (json_last_error() !== JSON_ERROR_NONE) {
    echo json_encode(['error' => 'Failed to parse results']);
    exit;
}

// Return the results
echo json_encode($result);

// Optional: Delete the uploaded image after processing
// unlink($upload_path);
?>