#1. Spark Sessions and Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import requests, os, io, torch
from PIL import Image
from torchvision import transforms
from torchvision.models import resnet50, ResNet50_Weights

spark = SparkSession.builder.appName("ImageTaggingPipeline").getOrCreate()

#2. Pretrained Model & Labels (Broadcasted)
model = resnet50(weights=ResNet50_Weights.IMAGENET1K_V2)
model.eval()
imagenet_labels = ResNet50_Weights.IMAGENET1K_V2.meta['categories']
bc_model = spark.sparkContext.broadcast(model)
bc_labels = spark.sparkContext.broadcast(imagenet_labels)

#3. UDF: Download Images
def download_image_udf(url, product_id, download_dir='fake-images'):
    os.makedirs(download_dir, exist_ok=True)
    filename = os.path.join(download_dir, f"product_{product_id}.jpg")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return filename
    except Exception as e:
        return None

download_image = udf(download_image_udf, StringType())

#4. Classify Images
def classify_image_udf(path):
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
            output = bc_model.value(input_tensor)
            probs = torch.nn.functional.softmax(output[0], dim=0)
            top_idx = torch.argmax(probs).item()
            label = bc_labels.value[top_idx]
            if "beer" in label or "wine" in label or "alcohol" in label:
                return "alcoholic"
            elif "weapon" in label or "assault" in label or "violence" in label:
                return "abusive"
            else:
                return "safe"
    except Exception:
        return "unknown"

classify_image = udf(classify_image_udf, StringType())

#5. Apply UDF's in Spark Pipelines
df = spark.read.csv("products.csv", header=True)

df = df.withColumn("local_image_path", download_image("placeholder_image_url", "product_id"))
df = df.withColumn("content_tag", classify_image("local_image_path"))

df.write.csv("tagged-images-spark.csv", header=True, mode="overwrite")
