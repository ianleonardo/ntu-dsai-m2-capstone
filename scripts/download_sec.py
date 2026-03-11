import requests
import zipfile
import io
import os

year_input = input("Enter the year to download (e.g., 2025): ").strip()
if not year_input:
    print("Invalid year. Defaulting to 2025.")
    year_input = "2025"

quarters = [f'{year_input}q1', f'{year_input}q2', f'{year_input}q3', f'{year_input}q4']
base_url = 'https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/'
output_dir = '../data/sec'
os.makedirs(output_dir, exist_ok=True)

headers = {'User-Agent': 'myemail@example.com'} # SEC requires a User-Agent

for quarter in quarters:
    filename = f"{quarter}_form345.zip"
    url = base_url + filename
    print(f"Downloading {filename}...")
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(f"{output_dir}/{quarter}")
        print(f"Extracted {quarter} to {output_dir}/{quarter}")
    else:
        print(f"Failed to download {filename}: Status {response.status_code}")