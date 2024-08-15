import asyncio
import inspect
import time
import aiohttp  # Zamiast requests
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2 import sql
import json
from aiogram import Bot
from telegram.ext import Application
from dotenv import load_dotenv
import os

load_dotenv()


class Logger:
    @staticmethod
    def _send(text):
        print(f"[{datetime.now()}]{text}")

    @staticmethod
    def _format_message(level, text):
        caller_frame = inspect.currentframe().f_back
        caller_function = caller_frame.f_code.co_name

        if caller_frame.f_back:
            caller_function = caller_frame.f_back.f_code.co_name

        if caller_frame.f_back:
            caller_frame = caller_frame.f_back

        caller_class = None
        if "self" in caller_frame.f_locals:
            caller_class = caller_frame.f_locals["self"].__class__.__name__

        class_info = f"[{caller_class}]"
        return f"[{level}]{class_info}[{caller_function}]{text}"

    @staticmethod
    def info(text):
        message = Logger._format_message("INFO", text)
        Logger._send(message)

    @staticmethod
    def debug(text):
        message = Logger._format_message("DEBUG", text)
        Logger._send(message)

    @staticmethod
    def warn(text):
        message = Logger._format_message("WARN", text)
        Logger._send(message)

    @staticmethod
    def error(text):
        message = Logger._format_message("ERROR", text)
        Logger._send(message)


class DatabaseManager:
    def __init__(self, host, user, password, database):
        Logger.info("Database Manager Initialisation")
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        Logger.info("Database Manager Connection")
        self.connection = psycopg2.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
        )

    def create_table(self):
        try:
            Logger.info("Trying to create Table")
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS destination_changes (
                    id SERIAL PRIMARY KEY,
                    price FLOAT,
                    country VARCHAR(255),
                    name VARCHAR(255),
                    airports VARCHAR(255),
                    dates VARCHAR(255),
                    brand VARCHAR(255),
                    provider VARCHAR(255),
                    date TIMESTAMP,
                    UNIQUE(price, country, name, airports, brand, dates)
                )
                """
            )
            self.connection.commit()
        except Exception as e:
            Logger.error(f"{e}")
        finally:
            self.connection.close()

    def add_to_db(self, rows):
        try:
            self.connect()
            cursor = self.connection.cursor()
            Logger.info(f"Adding data to Database")
            for _, row in rows:
                cursor.execute(
                    """
                    INSERT INTO destination_changes (
                        price, country, name, airports, brand, dates, date, provider, active
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, True)
                    ON CONFLICT (price, country, name, airports, brand, dates) DO NOTHING
                    """,
                    (
                        row["DataLayer"]["price"],
                        row["Panstwo"],
                        row["Nazwa"],
                        "".join(row["DataLayer"]["name"].split(" ")[-4:-1]),
                        row["DataLayer"]["brand"],
                        row["TerminWyjazdu"].split(" ")[0],
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        row["provider"],
                    ),
                )
            self.connection.commit()
        except Exception as e:
            Logger.error(f"{e}")
        finally:
            self.connection.close()

    def check_active(self, rows):
        try:
            self.connect()
            cursor = self.connection.cursor()
            Logger.info("Checking and updating active status in Database")

            # Create a temporary table with consistent data types
            cursor.execute("""
                CREATE TEMPORARY TABLE temp_changes (
                    price DOUBLE PRECISION, 
                    country TEXT, 
                    name TEXT, 
                    airports TEXT, 
                    brand TEXT, 
                    dates TEXT
                )
            """)

            # Prepare data for insertion, handle empty or invalid dates
            def format_date(date_str):
                try:
                    # Try parsing the date string
                    return datetime.strptime(date_str, '%Y-%m-%d').date()
                except ValueError:
                    # Return None if the date format is invalid
                    return None

            insert_data = [
                (
                    row["DataLayer"]["price"],
                    row["Panstwo"],
                    row["Nazwa"],
                    "".join(row["DataLayer"]["name"].split(" ")[-4:-1]),
                    row["DataLayer"]["brand"],
                    format_date(row["TerminWyjazdu"].split(" ")[0])  # Handle date formatting
                )
                for _, row in rows
            ]

            # Insert data into the temporary table
            cursor.executemany(
                """
                INSERT INTO temp_changes (price, country, name, airports, brand, dates)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                insert_data
            )

            # Update all records to inactive that are not in the temporary table
            cursor.execute(
                """
                UPDATE destination_changes
                SET active = False
                WHERE (price, country, name, airports, brand, dates) NOT IN (
                    SELECT price, country, name, airports, brand, dates FROM temp_changes
                )
                """
            )

            # Update records in destination_changes to active
            cursor.execute(
                """
                UPDATE destination_changes
                SET active = True
                WHERE (price, country, name, airports, brand, dates) IN (
                    SELECT price, country, name, airports, brand, dates FROM temp_changes
                )
                """
            )

            # Commit changes
            self.connection.commit()

        except Exception as e:
            Logger.error(f"{e}")
        finally:
            self.connection.close()


class DataFetcher:
    def __init__(self):
        Logger.info("Data Fetcher Initialisation")
        self.used = []

    def check_data_lengths(self, data):
        lengths = {key: len(value) for key, value in data.items()}
        Logger.debug(f"Lengths of each list in data dictionary:{lengths}")
        if len(set(lengths.values())) != 1:
            raise ValueError(
                "All lists in the data dictionary must be of the same length."
            )

    async def fetch_data(self):
        Logger.info("Fetching Data")
        data = {
            "Panstwo": [],
            "Nazwa": [],
            "Klucz": [],
            "TerminWyjazdu": [],
            "Cena": [],
            "DataLayer": [],
            "provider": [],
        }

        dataAll = {
            "Panstwo": [],
            "Nazwa": [],
            "Klucz": [],
            "TerminWyjazdu": [],
            "Cena": [],
            "DataLayer": [],
            "provider": [],
        }

        await self.fetch_rainbow_data(data, dataAll)
        await self.fetch_tui_data(data, dataAll)
        # await self.fetch_itaka_data(data)

        self.check_data_lengths(data)
        self.check_data_lengths(dataAll)

        Logger.info("Fetching Data Completed")
        return pd.DataFrame(data), pd.DataFrame(dataAll)

    async def fetch_rainbow_data(self, data, dataAll):
        try:
            Logger.info("Fetching Rainbow Data")
            url = "https://biletyczarterowe.r.pl/api/wyszukiwanie/wyszukaj?oneWay=false&dataUrodzenia%5B%5D=1989-10-30&dataUrodzenia%5B%5D=1989-10-30&sortowanie=cena"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    json_data_rainbow = await response.json()

            for destynacja in json_data_rainbow["Destynacje"]:
                usedstring = json.dumps(destynacja, separators=(",", ":"))
                if (
                    usedstring not in self.used
                    and destynacja["DataLayer"]["name"].split(" ")[-4] == "WAW"
                ):
                    data["Panstwo"].append(destynacja["Panstwo"])
                    data["Nazwa"].append(destynacja["Nazwa"])
                    data["Klucz"].append(destynacja["Klucz"])
                    data["TerminWyjazdu"].append(
                        str(
                            datetime.strptime(
                                destynacja["TerminWyjazdu"], "%Y-%m-%dT%H:%M:%SZ"
                            )
                        )
                    )
                    data["Cena"].append(int(str(destynacja["Cena"]).replace(" ", "")))
                    data["DataLayer"].append(destynacja["DataLayer"])
                    data["provider"].append("Rainbow")
                    self.used.append(usedstring)

                dataAll["Panstwo"].append(destynacja["Panstwo"])
                dataAll["Nazwa"].append(destynacja["Nazwa"])
                dataAll["Klucz"].append(destynacja["Klucz"])
                dataAll["TerminWyjazdu"].append(
                    str(
                        datetime.strptime(
                            destynacja["TerminWyjazdu"], "%Y-%m-%dT%H:%M:%SZ"
                        )
                    )
                )
                dataAll["Cena"].append(int(str(destynacja["Cena"]).replace(" ", "")))
                dataAll["DataLayer"].append(destynacja["DataLayer"])
                dataAll["provider"].append("Rainbow")
        except Exception as e:
            Logger.error(f"[Error][fetch_rainbow_data]: {e}")

    async def fetch_tui_data(self, data, dataAll):
        try:
            Logger.info("Fetching TUI Data")
            url = "https://www.tui.pl/api/www/multiCharters"
            headers = {
                "Content-Type": "application/json;charset=UTF-8",
            }
            payload = '{"adultsCt":2,"arrivalAirportCodes":[],"childrenBirthDates":[],"departureAirportCodes":["WAW"],"duration":"3-14"}'
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, data=payload) as response:
                    response.raise_for_status()
                    json_data_tui = await response.json()

            for destynacja in json_data_tui:
                usedstring = json.dumps(destynacja, separators=(",", ":"))
                if usedstring not in self.used:
                    data["Panstwo"].append(destynacja["countryName"])
                    data["Nazwa"].append(destynacja["destinationName"])
                    data["Klucz"].append(destynacja["airportCode"])
                    data["TerminWyjazdu"].append("")
                    data["Cena"].append(
                        int(str(destynacja["perPersonPrice"]).replace(" ", ""))
                    )
                    data["DataLayer"].append(
                        {
                            "brand": "TUI",
                            "price": int(
                                str(destynacja["perPersonPrice"]).replace(" ", "")
                            ),
                            "name": destynacja["destinationName"]
                            + f" WAW - {destynacja['airportCode']} NA/NA/NA",
                        }
                    )
                    data["provider"].append("TUI")
                    self.used.append(usedstring)
                dataAll["Panstwo"].append(destynacja["countryName"])
                dataAll["Nazwa"].append(destynacja["destinationName"])
                dataAll["Klucz"].append(destynacja["airportCode"])
                dataAll["TerminWyjazdu"].append("")
                dataAll["Cena"].append(
                    int(str(destynacja["perPersonPrice"]).replace(" ", ""))
                )
                dataAll["DataLayer"].append(
                    {
                        "brand": "TUI",
                        "price": int(
                            str(destynacja["perPersonPrice"]).replace(" ", "")
                        ),
                        "name": destynacja["destinationName"]
                        + f" WAW - {destynacja['airportCode']} NA/NA/NA",
                    }
                )
                dataAll["provider"].append("TUI")
        except Exception as e:
            Logger.error(f"{e}")

    async def fetch_itaka_data(self, data, dataAll):
        try:
            Logger.info("Fetching ITAKA Data")
            i = 1
            while i > 0:
                payload = {
                    "operationName": "charterFlights",
                    "variables": {
                        "adultsCount": 1,
                        "childrenCount": 0,
                        "departureRegions": "warszawa",
                        "infantsCount": 0,
                        "oneWay": False,
                        "page": i,
                        "sort": "PRICE_ASC",
                    },
                    "query": "query charterFlights($adultsCount: Int!, $childrenCount: Int, $dateFrom: String, $dateTo: String, $departureRegions: [String!], $destinationRegions: [String!], $infantsCount: Int, $oneWay: Boolean, $page: Int, $limit: Int, $sort: CharterFlightSortDirection) {\n  charterFlights(\n    adultsCount: $adultsCount\n    childrenCount: $childrenCount\n    dateFrom: $dateFrom\n    dateTo: $dateTo\n    departureRegions: $departureRegions\n    destinationRegions: $destinationRegions\n    infantsCount: $infantsCount\n    oneWay: $oneWay\n    page: $page\n    limit: $limit\n    sort: $sort\n  ) {\n    items {\n      supplierObjectId\n      departureRoute {\n        airport {\n          city\n          iata\n          name\n          __typename\n        }\n        date\n        __typename\n      }\n      departureRouteId\n      returnRoute {\n        airport {\n          city\n          iata\n          name\n          __typename\n        }\n        date\n        __typename\n      }\n      returnRouteId\n      pricePerPerson {\n        amount\n        currency\n        __typename\n      }\n      pricePerGroup {\n        amount\n        currency\n        __typename\n      }\n      priceListCode\n      oneWay\n      url\n      offerId\n      participants {\n        adultsNumber\n        childrenAge\n        __typename\n      }\n      __typename\n    }\n    totalCount\n    __typename\n  }\n}\n",
                }

                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        "https://biletylotnicze.itaka.pl/api/graphql", json=payload
                    ) as response:
                        response.raise_for_status()
                        json_data = await response.json()

                if (
                    "data" not in json_data
                    or "charterFlights" not in json_data["data"]
                    or "items" not in json_data["data"]["charterFlights"]
                ):
                    break

                for el in json_data["data"]["charterFlights"]["items"]:
                    if el is None:
                        break
                    else:
                        usedstring = json.dumps(el, separators=(",", ":"))
                        if usedstring not in self.used:
                            data["Panstwo"].append("Nieznane")
                            data["Nazwa"].append(
                                el["departureRoute"]["airport"]["city"]
                            )
                            data["Klucz"].append(
                                el["departureRoute"]["airport"]["iata"]
                            )
                            data["TerminWyjazdu"].append(
                                str(
                                    datetime.strptime(
                                        el["departureRoute"]["date"],
                                        "%Y-%m-%dT%H:%M:%S",
                                    )
                                )
                            )
                            data["Cena"].append(
                                int(
                                    str(el["pricePerPerson"]["amount"]).replace(" ", "")
                                )
                            )
                            data["DataLayer"].append(
                                {
                                    "brand": "ITAKA",
                                    "price": int(
                                        str(el["pricePerPerson"]["amount"]).replace(
                                            " ", ""
                                        )
                                    ),
                                    "name": f"{el['departureRoute']['airport']['iata']} WAW",
                                }
                            )
                            data["provider"].append("ITAKA")
                            self.used.append(usedstring)
                        dataAll["Panstwo"].append("Nieznane")
                        dataAll["Nazwa"].append(el["departureRoute"]["airport"]["city"])
                        dataAll["Klucz"].append(el["departureRoute"]["airport"]["iata"])
                        dataAll["TerminWyjazdu"].append(
                            str(
                                datetime.strptime(
                                    el["departureRoute"]["date"],
                                    "%Y-%m-%dT%H:%M:%S",
                                )
                            )
                        )
                        dataAll["Cena"].append(
                            int(str(el["pricePerPerson"]["amount"]).replace(" ", ""))
                        )
                        dataAll["DataLayer"].append(
                            {
                                "brand": "ITAKA",
                                "price": int(
                                    str(el["pricePerPerson"]["amount"]).replace(" ", "")
                                ),
                                "name": f"{el['departureRoute']['airport']['iata']} WAW",
                            }
                        )
                        dataAll["provider"].append("ITAKA")
                if i > 0:
                    i += 1
                else:
                    break
        except Exception as e:
            Logger.error(f"{e}")


class TravelDealsBot:
    def __init__(self, bot_token, chat_id, db_manager, data_fetcher):
        Logger.info("Travel Deals Bot Initialisation")
        self.bot = Bot(token=bot_token)
        self.chat_id = chat_id
        self.db_manager = db_manager
        self.data_fetcher = data_fetcher

    async def send_messages(self):
        df, dfAll = await self.data_fetcher.fetch_data()
        Logger.info("Updating DB Data!")
        self.db_manager.add_to_db(dfAll.iterrows())
        self.db_manager.check_active(dfAll.iterrows())

        if not df.empty:
            Logger.info("Sending message to Telegram!")
            separator = "- - - - - - - - - - - - -"
            df = df.reset_index()
            df = df.sort_values(by="Cena")

            await self.bot.send_message(
                self.chat_id,
                f"\n{separator}\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{separator}\n",
            )

            message_string = ""
            message_len = 0
            for index, row in df.iterrows():
                message_string_temp = f'🤑 {row["DataLayer"]["price"]}zł 📅 {str(row["TerminWyjazdu"]).split(" ")[0].split("T")[0]}[{row["provider"][0]}]\
                    \n🗺️{row["Panstwo"]}\n🌴{row["Nazwa"]}\n🛬({row["DataLayer"]["name"].split(" ")[-4]} - {row["Klucz"]}) ✈️ {row["DataLayer"]["brand"]}\n{separator}\n'

                if message_len + len(message_string_temp) > 4096:
                    if message_string != "":
                        await self.bot.send_message(self.chat_id, message_string)
                    else:
                        Logger.warn("Aborted string!")
                    message_len = 0
                    message_string = ""

                message_len += len(message_string_temp)
                message_string += message_string_temp

            if message_string != "":
                await self.bot.send_message(self.chat_id, message_string)

    async def run(self, interval=5 * 60):
        Logger.info("Application started!")
        while True:
            await self.send_messages()
            await asyncio.sleep(interval)


async def main_run_bot():
    Logger.info("Bot Initialisation")
    BOT_TOKEN = os.getenv("TELEGRAM_BOT_FLIGHT_SEARCH_TOKEN")
    CHAT_ID = os.getenv("TELEGRAM_BOT_FLIGHT_SEARCH_CHAT_ID")
    DB_CONFIG = {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_DATABASE"),
    }

    db_manager = DatabaseManager(**DB_CONFIG)
    data_fetcher = DataFetcher()
    travel_bot = TravelDealsBot(BOT_TOKEN, CHAT_ID, db_manager, data_fetcher)

    db_manager.create_table()

    await travel_bot.run()
