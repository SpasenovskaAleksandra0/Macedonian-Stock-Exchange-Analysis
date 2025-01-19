import asyncpg
from asyncpg.pool import Pool
import aiohttp
import asyncio
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup, SoupStrainer
from multiprocessing import Pool

class Database:
    def __init__(self, user, password, database, host="localhost"):
        self.pool: Pool | None = None
        self.user = user
        self.password = password
        self.database = database
        self.host = host

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            user=self.user,
            password=self.password,
            database=self.database,
            host=self.host,
            min_size=1,
            max_size=15
        )

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def create_tables(self):
        queries = [
            """
            CREATE TABLE IF NOT EXISTS Company (
                id SERIAL PRIMARY KEY,
                code VARCHAR(20) NOT NULL UNIQUE,
                name VARCHAR(255) NOT NULL,
                address VARCHAR(100),
                city VARCHAR(50),
                state VARCHAR(50),
                email VARCHAR(100),
                phones VARCHAR(50)[]
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS Issuer (
                id SERIAL PRIMARY KEY,
                code VARCHAR(20) UNIQUE NOT NULL,
                company_id INTEGER REFERENCES Company(id) ON DELETE CASCADE
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS StockHistory (
                id SERIAL PRIMARY KEY,
                issuer_id INTEGER NOT NULL REFERENCES Issuer(id) ON DELETE CASCADE,
                date DATE NOT NULL,
                last_trade_price VARCHAR(255) NOT NULL,
                max_price VARCHAR(255) NOT NULL,
                min_price VARCHAR(255) NOT NULL,
                avg_price VARCHAR(255) NOT NULL,
                percent_change VARCHAR(255) NOT NULL,
                volume VARCHAR(255) NOT NULL,
                turnover_best VARCHAR(255) NOT NULL,
                total_turnover VARCHAR(255) NOT NULL,
                CONSTRAINT unique_stock_entry UNIQUE (issuer_id, date)
            );
            """
        ]

        async with self.pool.acquire() as conn:
            for query in queries:
                await conn.execute(query)

    async def add_company(self, code, name, address=None, city=None, state=None, email=None, phones=None):
        query = """
            INSERT INTO Company (code, name, address, city, state, email, phones)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id;
        """

        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, code, name, address, city, state, email, phones)

    async def add_issuer(self, code, company_id):
        query = """
            INSERT INTO Issuer (code, company_id)
            VALUES ($1, $2)
            RETURNING id;
        """

        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, code, company_id)

    async def add_stock_entry(self, issuer_id, date, last_trade_price, _max, _min, avg_price, percent_change, volume, turnover_best, total_turnover):
        query = """
            INSERT INTO StockHistory (issuer_id, date, last_trade_price, max_price, min_price, avg_price, percent_change, volume, turnover_best, total_turnover)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING id;
        """

        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, issuer_id, date, last_trade_price, _max, _min, avg_price, percent_change, volume, turnover_best, total_turnover)

    async def assign_issuer(self, issuer_code, company_data):
        company_id = await self.add_company(*company_data)
        return await self.add_issuer(issuer_code, company_id)

    async def batch_add_stock_entries(self, entries):
        query = """
            INSERT INTO StockHistory (issuer_id, date, last_trade_price, max_price, min_price, avg_price, percent_change, volume, turnover_best, total_turnover)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (issuer_id, date) DO NOTHING;
        """

        async with self.pool.acquire() as conn:
            await conn.executemany(query, entries)

    async def find_issuer_by_code(self, code):
        query = "SELECT id FROM Issuer WHERE code = $1"

        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, code)

    async def find_stock_entry(self, code, date):
        issuer_id = await self.find_issuer_by_code(code)

        query = "SELECT * FROM StockHistory WHERE issuer_id = $1 AND date = $2"

        async with self.pool.acquire() as conn:
            result = await conn.fetchrow(query, issuer_id, date)
            return list(result)

    async def get_last_available_date(self, code):
        issuer_id = await self.find_issuer_by_code(code)

        query = "SELECT MAX(date) FROM StockHistory WHERE issuer_id = $1"

        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, issuer_id)


async def fetch_company(code):
    url = f"https://www.mse.mk/en/symbol/{code}"

    company_data = {
        "Code": code,
        "Name": "",
        "Address": "",
        "City": "",
        "State": "",
        "Mail": "",
        "Phone": []
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:

            response_text = await response.text()
            strainer = SoupStrainer('div', {'class': 'panel panel-default'})
            soup = BeautifulSoup(response_text, 'lxml', parse_only=strainer)

            title = soup.select_one("div.title")

            if title is None:
                title = soup.select_one("div#titleKonf2011")

                if title:
                    return [code, title.text.split(" - ")[2]]
                else:
                    print(f"{code} title could not be found")
                    return [code, code]

            company_data["Name"] = title.text
            details = soup.select("div#izdavach .row")[2:13]

            for row in details:
                cols = row.select("div")

                if cols:
                    key_text = cols[0].text

                    if key_text in company_data:
                        if key_text == "Phone":
                            company_data[key_text].extend(cols[1].text.split("; "))
                        else:
                            company_data[key_text] = cols[1].text

    return list(company_data.values())


async def fetch_stock_history(code):
    to_time = datetime.now()
    from_time = to_time - timedelta(days=3650)

    data = []
    tasks = []

    limiter = asyncio.Semaphore(10)

    async def fetch_data(url_):
        async with limiter:
            async with aiohttp.ClientSession() as session:
                async with session.get(url_) as response:
                    if response.status != 200:
                        await asyncio.sleep(1)
                        return await fetch_data(url_)

                    response_text = await response.text()
                    soup = BeautifulSoup(response_text, 'lxml', parse_only=SoupStrainer('tbody'))

                    rows = soup.select("tbody tr")
                    fetched_data = []
                    for row in rows:
                        cols = [col.text.strip() for col in row.select("td")]
                        if any(col == "" for col in cols):
                            continue
                        fetched_data.append(cols)
                    return fetched_data

    while to_time > from_time:
        to_date = to_time.strftime("%d,%m,%Y")
        from_date = from_time.strftime("%d,%m,%Y")
        url = f"https://www.mse.mk/mk/stats/symbolhistory/{code}?FromDate={from_date}&ToDate={to_date}"
        tasks.append(fetch_data(url))
        to_time -= timedelta(days=365)

    results = await asyncio.gather(*tasks)

    for result in results:
        for row in result:
            if row not in data:
                data.append(row)

    return data


async def fetch_issuers():
    url = "https://www.mse.mk/en/stats/current-schedule"
    excluded = ['CKB', 'SNBTO', 'TTK']
    issuers = []

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response_text = await response.text()
            soup = BeautifulSoup(response_text, "lxml", parse_only=SoupStrainer("tbody"))

            for row in soup.select("tr"):
                code = row.select("td")[0].text.strip()
                if code not in excluded and not any(char.isdigit() for char in code):
                    issuers.append(code)

            return issuers


async def get_last_available_date(db, issuer_code):
    found = await db.find_issuer_by_code(issuer_code)

    if found is None:
        company_data = await fetch_company(issuer_code)
        stock_history = await fetch_stock_history(issuer_code)

        if stock_history:
            issuer_id = await db.assign_issuer(issuer_code, company_data)

            entries = [
                [issuer_id, datetime.strptime(stock_entry[0].replace(".", "/"), "%d/%m/%Y").date()] + stock_entry[1:]
                for stock_entry in reversed(stock_history)
            ]

            await db.batch_add_stock_entries(entries)

            return entries[-1][1]

    return await db.get_last_available_date(issuer_code)


async def fill_in_missing_data(db, issuer_code, last_date):
    if not last_date:
        return

    stock_entry = (await db.find_stock_entry(issuer_code, last_date))[1:]
    start_date = stock_entry[1] + timedelta(days=1)
    end_date = datetime.now().date()
    days = (end_date - start_date).days

    entries = [
        [stock_entry[0], start_date + timedelta(days=i)] + stock_entry[2:]
        for i in range(days + 1)
    ]

    await db.batch_add_stock_entries(entries)


def sync_process_issuer(db_params, issuer_code):
    db = Database(**db_params)

    async def fetch_last_date_and_fill():
        start_time = time.time()
        await db.connect()

        last_date = await get_last_available_date(db, issuer_code)
        if last_date:
            await fill_in_missing_data(db, issuer_code, last_date)

        await db.close()
        end_time = time.time()
        print(f"{issuer_code} fetching took {end_time - start_time:.2f} seconds.")

    asyncio.run(fetch_last_date_and_fill())


async def main():
    db_params = {"user": "postgres", "password": "postgres", "database": "pgdatabase"}
    db = Database(**db_params)
    await db.connect()
    await db.create_tables()
    await db.close()

    issuers = await fetch_issuers()

    with Pool(processes=12) as pool:
        pool.starmap(sync_process_issuer, [(db_params, issuer) for issuer in issuers])


if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    end_time = time.time()
    print(f"Script took {end_time - start_time:.2f} seconds.")
