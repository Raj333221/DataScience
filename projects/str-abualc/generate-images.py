import pandas as pd
from faker import Faker
import requests
import os

# --- 1. Setup ---
fake = Faker('en_US')
NUM_RECORDS = 50
DOWNLOAD_DIR = 'fake-images'

# Create the directory if it doesn't exist
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
print(f"Images will be saved to the '{DOWNLOAD_DIR}' directory.")
print("-" * 50)

# --- 2. Generate Data and URLs ---
def create_fake_product():
    # Use a unique ID as a seed for the image to ensure diversity
    product_id = fake.unique.random_int(min=10000, max=99999)
    
    # Faker uses 'seed' in the URL to generate a unique image based on that seed
    image_url = f"https://picsum.photos/seed/{product_id}/300/200" 
    
    return {
        'product_id': product_id,
        'product_name': fake.word().capitalize() + ' Item',
        'placeholder_image_url': image_url
    }

fake_data = [create_fake_product() for _ in range(NUM_RECORDS)]
df_products = pd.DataFrame(fake_data)

# --- 3. Function to Download and Save ---
def download_image(row):
    url = row['placeholder_image_url']
    product_id = row['product_id']
    
    # Define the local filename
    filename = os.path.join(DOWNLOAD_DIR, f"product_{product_id}.jpg")
    
    try:
        # Send a GET request to the URL
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        
        # Save the content of the response to a file
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"Downloaded: {filename}")
        return filename  # Return the local path
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading image for product {product_id}: {e}")
        return None

# --- 4. Apply the Download Function to the DataFrame ---
df_products['local_image_path'] = df_products.apply(download_image, axis=1)

# --- 5. Display Final Results ---
print("-" * 50)
print("Finished downloading images.")
print("Updated DataFrame (including local paths):")
print(df_products[['product_id', 'placeholder_image_url', 'local_image_path']])
