import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
# Define number of records and a list of common URLs and status codes
num_records = 4000000 # 4 million records
urls = ["/home", "/about", "/products/1", "/products/2", "/contact", "/blog/post-1", "/blog/post-2"]
status_codes = [200, 200, 200, 200, 200, 200, 404, 500] # More 200s to simulate success
with open("web-server.log", "w") as f:
	for _ in range(num_records):
		ip_address = fake.ipv4_public()
		timestamp = fake.date_time_between(start_date="-1y", end_date="now").strftime("%d/%b/%Y:%H:%M:%S +0000")
		http_method = random.choice(["GET", "POST"])
		url = random.choice(urls)
		http_version = "HTTP/1.1"
		status_code = random.choice(status_codes)

		log_entry = f'{ip_address} - - [{timestamp}] "{http_method} {url} {http_version}" {status_code} {random.randint(100, 5000)}\n'
		f.write(log_entry)
print("Generated 4 million log entries and saved to web-server.log")
