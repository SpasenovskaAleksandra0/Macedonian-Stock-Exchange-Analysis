# A web scraper in Python used to extract real-time stock data from the Macedonian Stock Exchange website. The scraper retrieves company details and historical stock data from MSE’s pages. Collected data is stored in a PostgreSQL database.

# Prerequisites
- Python
- PostgreSQL
- Docker

## Installation
1. Clone this repository
```bash
git clone https://github.com/SpasenovskaAleksandra0/Macedonian-Stock-Exchange-Analysis.git
cd 'Macedonian-Stock-Exchange-Analysis/Homework 1'
```

2. Install dependencies
```bash
pip install -r dependencies.txt
```

# Run the project
1. Run Docker Compose
```bash
docker compose up -d
```
2. run main.py
```bash
python main.py
```
