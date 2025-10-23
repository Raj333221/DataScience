import pandas as pd
from faker import Faker
import requests
import os
from PIL import Image
import io
import torch
from torchvision import transforms
from torchvision.models import resnet50
import torchvision.transforms.functional as TF

# --- 1. Setup ---
fake = Faker('en_US')
NUM_RECORDS = 50
DOWNLOAD_DIR = 'fake-images'
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# --- 2. Generate Fake Product Data ---
def create_fake_product():
    product_id = fake.unique.random_int(min=10000, max=99999)
    image_url = f"https://picsum.photos/seed/{product_id}/300/200"
    return {
        'product_id': product_id,
        'product_name': fake.word().capitalize() + ' Item',
        'placeholder_image_url': image_url
    }

df_products = pd.DataFrame([create_fake_product() for _ in range(NUM_RECORDS)])

# --- 3. Download Image ---
def download_image(row):
    url = row['placeholder_image_url']
    product_id = row['product_id']
    filename = os.path.join(DOWNLOAD_DIR, f"product_{product_id}.jpg")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return filename
    except requests.exceptions.RequestException as e:
        print(f"Error downloading image for product {product_id}: {e}")
        return None

df_products['local_image_path'] = df_products.apply(download_image, axis=1)

# --- 4. Load Pretrained Model ---
model = resnet50(weights=True)
model.eval()

# Optional: Load custom labels for alcohol/abuse detection
# For demo, use ImageNet labels (you can fine-tune later)
from torchvision.models import ResNet50_Weights
imagenet_labels = ResNet50_Weights.IMAGENET1K_V2.meta['categories']

# --- 5. Classify Image Content ---
def classify_image(path):
    try:
        image = Image.open(path).convert("RGB")
        preprocess = transforms.Compose([
            transforms.Resize((224, 224)),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                 std=[0.229, 0.224, 0.225])
        ])
        input_tensor = preprocess(image).unsqueeze(0)
        with torch.no_grad():
            output = model(input_tensor)
            probs = torch.nn.functional.softmax(output[0], dim=0)
            top_idx = torch.argmax(probs).item()
            label = imagenet_labels[top_idx]
            # Simple rule-based tagging
            if "beer" in label or "wine" in label or "alcohol" in label:
                return "alcoholic"
            elif "weapon" in label or "assault" in label or "violence" in label:
                return "abusive"
            else:
                return "safe"
    except Exception as e:
        print(f"Error classifying image {path}: {e}")
        return "unknown"

df_products['content_tag'] = df_products['local_image_path'].apply(classify_image)

# --- 6. Final Output ---
print("-" * 50)
print("Finished tagging images.")
print(df_products[['product_id', 'product_name', 'local_image_path', 'content_tag']])

# --- 7. Save to CSV ---
output_path = "tagged-images.csv"
df_products.to_csv(output_path, index=False)
print(f"Tagged data saved to {output_path}")
