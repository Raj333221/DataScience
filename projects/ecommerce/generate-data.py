import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

# Define number of records
num_records = 5000000  # 5 million records
# Generate data
data = []
product_categories = ["Electronics", "Books", "Home Goods", "Clothing", "Groceries"]

for _ in range(num_records):
	customer_id = fake.uuid4()
	product_id = fake.uuid4()
	transaction_amount = round(random.uniform(5.0, 500.0), 2)
	purchase_date = fake.date_time_between(start_date="-2y", end_date="now")
	product_category = random.choice(product_categories)

	data.append([customer_id,product_id,transaction_amount,purchase_date,product_category])

# Create a DataFrame
columns = ["customer_id", "product_id", "transaction_amount", "purchase_date", "product_category"]
df = pd.DataFrame(data, columns=columns)
# Save to a large CSV file
df.to_csv("e-comm-trax.csv", index=False)
print("Generated 5 million records and saved to e-comm-trax.csv")
