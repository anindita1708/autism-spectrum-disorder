from flask import Flask, request, jsonify
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Thread pool for concurrent processing
executor = ThreadPoolExecutor(max_workers=10)

def process_reading(reading):
    """
    Function to process a single sensor reading.
    This is where filtering and transformation can occur.
    """
    # Simulate some processing time
    time.sleep(0.1)
    processed_reading = {
        "sensor_id": reading["sensor_id"],
        "value": reading["value"] * 2  # Example transformation
    }
    return processed_reading

@app.route('/process-readings', methods=['POST'])
def process_readings():
    data = request.get_json()

    if not data or 'readings' not in data:
        return jsonify({"error": "Invalid input"}), 400

    readings = data['readings']

    logging.info(f"Received {len(readings)} readings")

    # Process readings concurrently
    future_to_reading = {executor.submit(process_reading, reading): reading for reading in readings}
    processed_readings = []

    for future in as_completed(future_to_reading):
        result = future.result()
        processed_readings.append(result)

    logging.info(f"Processed {len(processed_readings)} readings")

    return jsonify({"processed_readings": processed_readings}), 200

if __name__ == '__main__':
    app.run(debug=True, threaded=True)
