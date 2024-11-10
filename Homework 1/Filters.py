import requests
from bs4 import BeautifulSoup
import pandas as pd
import json


def fetch_issuers():
    url = "https://www.mse.mk/mk/issuers/free-market"
    response = requests.get(url)

    if response.status_code != 200:
        print("Failed to retrieve the webpage.")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table')

    if not table:
        print("Issuer table not found.")
        return []

    issuers = []
    for row in table.find_all('tr')[1:]:
        columns = row.find_all('td')
        if len(columns) > 0:
            issuer_code = columns[0].text.strip()
            issuer_name = columns[1].text.strip()

            if issuer_code.isalpha():
                issuers.append((issuer_name, issuer_code))
    return issuers

def save_to_json(issuers, filename='issuers.json'):
    """Save issuers data to JSON file"""
    # Convert list of tuples to list of dictionaries for better JSON structure
    json_data = [{"issuer_name": name, "code": code} for name, code in issuers]

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=4)
    print(f"Data saved to {filename}")


if __name__ == "__main__":
    issuer_list = fetch_issuers()

    # Save to all formats
    save_to_json(issuer_list)

    # Print preview
    print("\nPreview of fetched data:")
    for name, code in issuer_list:
        print(f"Issuer Name: {name}, Code: {code}")




