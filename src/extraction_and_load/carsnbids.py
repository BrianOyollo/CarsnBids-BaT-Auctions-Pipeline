import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from cars_and_bids import driver_setup, scrape_auction, scrape_auction_urls
from utils import upload, ntfy
import os
import boto3

DBT_ENV_SECRET_AWS_ACCESS_KEY_ID = os.getenv("DBT_ENV_SECRET_AWS_ACCESS_KEY_ID")
DBT_ENV_SECRET_AWS_SECRET_ACCESS_KEY = os.getenv("DBT_ENV_SECRET_AWS_SECRET_ACCESS_KEY")
DBT_ENV_SECRET_AWS_REGION = os.getenv("DBT_ENV_SECRET_AWS_REGION")
DBT_ENV_S3_BUCKET = os.getenv("DBT_ENV_S3_BUCKET")



auctions_data = []

if __name__ == '__main__':

    # 1. extract auction urls
    driver = driver_setup.setup_driver()
    auction_urls = scrape_auction_urls.extract_auction_urls(driver, max_pages=2) # scrape max of 3 pages daily for new auctions
    driver_setup.driver_teardown(driver)

    # 2. scrape each auction url
    print(">>> Scraping Auctions")
    def one_driver_per_url(url):
        driver = driver_setup.setup_driver()
        auction_data = scrape_auction.scrape_auction_data(driver,url)
        driver_setup.driver_teardown(driver)

        return auction_data
    
    auctions_data = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(one_driver_per_url, url) for url in auction_urls[:10]]
        for future in as_completed(futures):
            try:
                result = future.result()
                auctions_data.append(result)
            except Exception as e:
                print(e)

    # 3. save auction data as parquet in s3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=DBT_ENV_SECRET_AWS_ACCESS_KEY_ID,
        aws_secret_access_key=DBT_ENV_SECRET_AWS_SECRET_ACCESS_KEY,
        region_name=DBT_ENV_SECRET_AWS_REGION
    )

    print(">>> Uploading Auction Data to S3")
    upload.upload_to_s3(s3_client, auctions_data)
    




