from flask import Flask, send_from_directory
import os

# Define the folder where your images are stored
IMAGE_DIR = 'fake_images'

# The path where the Flask app is running (current directory)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# The absolute path to your image directory
FULL_IMAGE_PATH = os.path.join(BASE_DIR, IMAGE_DIR)

app = Flask(__name__)

@app.route('/images/<filename>')
def serve_image(filename):
    """
    Serves a specific image file from the local directory.
    
    Example URL to access an image: 
    http://127.0.0.1:5000/images/product_12345.jpg
    """
    try:
        # Use send_from_directory for secure file serving
        return send_from_directory(FULL_IMAGE_PATH, filename)
    except FileNotFoundError:
        # Handle cases where the requested file doesn't exist
        return "Image not found", 404

if __name__ == '__main__':
    # Ensure the directory exists before starting the server
    if not os.path.isdir(FULL_IMAGE_PATH):
        print(f"Error: Directory '{IMAGE_DIR}' not found. Please run the download script first.")
    else:
        # Run the server on the default port 5000
        print(f"Flask server started. Access images at http://127.0.0.1:5000/images/<filename>")
        app.run(debug=True)
