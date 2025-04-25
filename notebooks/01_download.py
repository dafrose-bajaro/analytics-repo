# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.0
#   kernelspec:
#     display_name: sandbox
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Download
#
# This notebook downloads the data from Project CCHAIN.

# %%
import os
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# %% [markdown]
# ## All CSV files from a URL

# %% [markdown]
# ### Project CCHAIN
#
# It is recommended that all files be stored in cloud storage after download. This repository assumes that these files are in the Google Cloud Storage.

# %%
# source url
url = "https://data.humdata.org/dataset/project-cchain"

# output folder
download_folder = "../data/lake"
os.makedirs(download_folder, exist_ok=True)


# function
def download_csv_files(url, download_folder):
    try:
        # get url content
        response = requests.get(url)
        response.raise_for_status()

        # parse html
        soup = BeautifulSoup(response.content, "html.parser")

        # find links with .csv extension
        csv_links = soup.find_all("a", href=True)
        csv_files = [
            link["href"] for link in csv_links if link["href"].endswith(".csv")
        ]

        # download csv
        for relative_url in csv_files:
            csv_url = urljoin(url, relative_url)
            file_name = os.path.basename(csv_url)
            file_path = os.path.join(download_folder, file_name)

            csv_response = requests.get(csv_url)
            csv_response.raise_for_status()  # Ensure the request was successful
            with open(file_path, "wb") as file:
                file.write(csv_response.content)

            print(f"Downloaded: {file_name}")

        print("All CSV files downloaded successfully!")
    except Exception as e:
        print(f"Error occurred: {e}")


# apply
download_csv_files(url, download_folder)

# %% [markdown]
# ### WAQI
#
# Note that WAQI historical data needs verification for download. You can download [historical AQI data for Chiang Mai](https://aqicn.org/historical/#city:chiang-mai) from January 2016 to present and upload the CSV file to Google Cloud Storage.

# %% [markdown]
# ## Selected files only

# %%
# list of urls
url_list = [
    "https://data.humdata.org/dataset/5b580664-365e-4d7e-b5e5-2990df8f12a5/resource/eb1341ec-296f-442c-a741-6e78fca31332/download/osm_poi_sanitation.csv",
    # add more urls here
]

# output folder
download_folder = "../data/lake"
os.makedirs(download_folder, exist_ok=True)


# function
def download_files(url_list, download_folder):
    try:
        # loop through urls and downlaod
        for url in url_list:
            file_name = os.path.basename(url)
            file_path = os.path.join(download_folder, file_name)

            print(f"Downloading: {url}")
            response = requests.get(url)
            response.raise_for_status()

            with open(file_path, "wb") as file:
                file.write(response.content)

            print(f"Downloaded: {file_name}")

        print("All files downloaded successfully!")
    except Exception as e:
        print(f"Error occurred: {e}")


# apply
download_files(url_list, download_folder)

# %% [markdown]
# You can then add these files to Google Cloud Storage.
