import asyncio
import html
import requests
import sqlite3
import inspect
import json
import pandas as pd
import time
from telegram.ext import Application
from telegram.constants import ParseMode
import inspect
import traceback
import re
from typing import List
from dotenv import load_dotenv
import os

load_dotenv()

chat_id = os.getenv("TELEGRAM_BOT_OFFER_SEARCH_TOKEN")
bot_token = os.getenv("TELEGRAM_BOT_OFFER_SEARCH_CHAT_ID")


# CREATE DB
# def create_db():
#     try:
#         conn = sqlite3.connect(DB_File)
#         cursor = conn.cursor()
#         cursor.execute(
#             f"""
#             CREATE TABLE IF NOT EXISTS {Table_Name} (
#                 id INTEGER PRIMARY KEY AUTOINCREMENT,
#                 privder TEXT,
#                 hotelCode TEXT,
#                 hotelName TEXT,
#                 city TEXT,
#                 roomCode TEXT,
#                 roomName TEXT,
#                 hotelStandard TEXT,
#                 offerCode TEXT,
#                 duration TEXT,
#                 offerUrl TEXT,
#                 departureDate
#                 returnDate
#                 boardType

#                 UNIQUE(price, country, name, airports, brand, dates)
#             )
#         """
#         )
#         conn.commit()
#         conn.close()
#     except Exception as e:
#         current_function_name = inspect.currentframe().f_code.co_name
#         print(f"[Error][{current_function_name}]: {e}")
# create_db()
# def add_to_db(rows):
#     try:
#         conn = sqlite3.connect(DB_File)
#         cursor = conn.cursor()

#         for index, row in rows:
#             cursor.execute(
#                 f"""
#                 INSERT OR IGNORE INTO {Table_Name} (
#                     price, country, name, airports, brand, dates, date, provider
#                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
#                 """,
#                 (
#                     row["DataLayer"]["price"],
#                     row["Panstwo"],
#                     row["Nazwa"],
#                     row["DataLayer"]["name"].split(" ")[-4]
#                     + "-"
#                     + row["DataLayer"]["name"].split(" ")[-2],
#                     row["DataLayer"]["brand"],
#                     row["TerminWyjazdu"].split(" ")[0],
#                     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#                     row["provider"],
#                 ),
#             )

#         conn.commit()
#         conn.close()
#     except Exception as e:
#         current_function_name = inspect.currentframe().f_code.co_name
#         print(f"[Error][{current_function_name}]: {e}")


used = []

# Search Data
departure_date_from = "2024-08-14"
departure_date_to = "2024-08-18"
duration_from = 2
duration_to = 5
Adults_count = 1
max_price = 4000 * Adults_count


def log_error(e):
    tb = traceback.format_exc()
    frame = inspect.currentframe()
    current_function_name = frame.f_code.co_name
    current_line_number = frame.f_lineno

    print(f"[Error][{current_function_name} line {current_line_number}]: {e}")
    print(tb)


def escape_markdown(text):
    special_chars = r"\*_[]()~`>#+-=|{}.!"

    def escape_char(match):
        return f"\\{match.group(0)}"

    escaped_text = re.sub(f"[{re.escape(special_chars)}]", escape_char, text)

    return escaped_text


def strip_markdown_v2(text: str) -> str:
    return re.sub(r"[\_\*\[\]\(\)\~\`\>\+\-\=\|\{\}\!]", "", text)


def markdown_length(text: str) -> int:
    stripped_text = strip_markdown_v2(text)
    return len(stripped_text)


async def send_message_async(bot, chat_id, text):
    if text != " " and text is not None and text != "":
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=ParseMode.MARKDOWN_V2,
            disable_web_page_preview=True,
        )


async def get_tui_data():
    url = "https://www.tui.pl/api/services/tui-search/api/search/offers"
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
    }

    payload_json = {
        "childrenBirthdays": [],
        "departureDateFrom": departure_date_from,
        "departureDateTo": departure_date_to,
        "departuresCodes": [
            "BZG",
            "GDN",
            "KTW",
            "KRK",
            "LUZ",
            "POZ",
            "WAW",
            "WMI",
            "RDO",
            "WRO",
            "LCJ",
        ],
        "destinationsCodes": [
            "PMI"
        ],
        "durationFrom": duration_from,
        "durationTo": duration_to,
        "occupancies": [],
        "numberOfAdults": Adults_count,
        "offerType": "BY_PLANE",
        "filters": [
            {"filterId": "priceSelector", "selectedValues": []},
            {
                "filterId": "board",
                "selectedValues": [
                    "GT06-AI GT06-XX",
                    "GT06-FB GT06-FBP",
                    "GT06-HB GT06-HBP",
                ],
            },
            {"filterId": "amountRange", "selectedValues": [f"#{str(max_price)}"]},
            {
                "filterId": "minHotelCategory",
                "selectedValues": ["defaultHotelCategory"],
            },
            {
                "filterId": "tripAdvisorRating",
                "selectedValues": ["defaultTripAdvisorRating"],
            },
            {"filterId": "beach_distance", "selectedValues": ["defaultBeachDistance"]},
        ],
        "metaData": {"page": 0, "pageSize": 500, "sorting": "price"},
    }

    try:
        response = requests.post(url, headers=headers, json=payload_json)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as err:
        print(f"Błąd przy pobieraniu danych: {err}")
    except Exception as e:
        current_function_name = inspect.currentframe().f_code.co_name
        print(f"[Error][{current_function_name}]: {e}")


async def get_wakacje_pl_data():
    url = "https://www.wakacje.pl/v2/api/offers"
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
    }
    payload = json.dumps(
        [
            {
                "method": "search.tripsSearch",
                "params": {
                    "brand": "WAK",
                    "limit": 500,
                    "priceHistory": 1,
                    "imageSizes": ["570,428"],
                    "flatArray": True,
                    "multiSearch": True,
                    "withHotelRate": 1,
                    "withPromoOffer": 0,
                    "recommendationVersion": "",
                    "withPromotionsInfo": False,
                    "type": "tours",
                    "firstMinuteTui": False,
                    "countryId": [],
                    "regionId": [
                        "33006",
                    ],
                    "cityId": [],
                    "hotelId": [],
                    "roundTripId": [],
                    "cruiseId": [],
                    "searchType": "wczasy",
                    "offersAttributes": [],
                    "alternative": {
                        "countryId": ["33"],
                        "regionId": [],
                        "cityId": [],
                    },
                    "qsVersion": "cx",
                    "query": {
                        "campTypes": [],
                        "qsVersion": "cx",
                        "qsVersionLast": 0,
                        "tab": False,
                        "candy": False,
                        "pok": None,
                        "flush": False,
                        "tourOpAndCode": None,
                        "obj_type": None,
                        "catalog": None,
                        "roomType": None,
                        "test": None,
                        "year": None,
                        "month": None,
                        "rangeDate": None,
                        "withoutLast": 0,
                        "category": False,
                        "not-attribute": False,
                        "pageNumber": 1,
                        "departureDate": departure_date_from,
                        "arrivalDate": departure_date_to,
                        "departure": None,
                        "type": [1],
                        "duration": {"min": duration_from, "max": duration_to},
                        "minPrice": None,
                        "maxPrice": str(max_price),
                        "service": [1, 2, 5, 6],
                        "firstminute": None,
                        "attribute": [],
                        "promotion": [],
                        "tourId": None,
                        "search": None,
                        "minCategory": None,
                        "maxCategory": 50,
                        "sort": 1,
                        "order": 0,
                        "totalPrice": True,
                        "rank": 60,
                        "withoutTours": [],
                        "withoutCountry": [],
                        "withoutTrips": [],
                        "rooms": [
                            {"adult": Adults_count, "kid": 0, "ages": [], "inf": None}
                        ],
                        "offerCode": None,
                        "dedicatedOffer": False,
                    },
                    "durationMin": str(duration_from),
                },
            }
        ]
    )

    try:
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as err:
        print(f"Błąd przy pobieraniu danych: {err}")
    except Exception as e:
        current_function_name = inspect.currentframe().f_code.co_name
        print(f"[Error][{current_function_name}]: {e}")


async def send_messages(bot, chat_id):
    data = {
        "hotelCode": [],
        "hotelName": [],
        "City": [],
        "roomName": [],
        "hotelStandard": [],
        "boardType": [],
        "offerCode": [],
        "duration": [],
        "offerUrl": [],
        "Country": [],
        "FullPrice": [],
        "PriceCurrency": [],
        "departurePlace": [],
        "departureDate": [],
        "returnDate": [],
        "tripRating": [],
        "departureFlight_carrierName": [],
        "returnFlight_carrierName": [],
        "departureFlight_hour": [],
        "departureFlight_hour_dest": [],
        "returnFlight_hour": [],
        "returnFlight_hour_dest": [],
        "provider": [],
    }

    try:
        json_data_tui = await get_tui_data()
        for row in json_data_tui["offers"]:
            data["hotelCode"].append(row.get("hotelCode", "N/A"))
            data["hotelName"].append(row.get("hotelName", "N/A"))
            data["City"].append(row.get("city", "N/A"))
            data["roomName"].append(row.get("roomName", "N/A"))
            data["hotelStandard"].append(row.get("hotelStandard", "N/A"))
            data["boardType"].append(row.get("boardType", "N/A"))
            data["offerCode"].append(row.get("offerCode", "N/A"))
            data["duration"].append(row.get("duration", "N/A"))
            data["offerUrl"].append(row.get("offerUrl", "N/A"))

            # Sprawdź, czy 'breadcrumbs' istnieje oraz czy 'breadcrumbs' zawiera co najmniej jeden element
            breadcrumbs = row.get("breadcrumbs", [])
            data["Country"].append(
                breadcrumbs[0].get("label", "N/A") if breadcrumbs else "N/A"
            )

            data["FullPrice"].append(int(row.get("discountFullPrice", 0)))
            data["PriceCurrency"].append(row.get("currency", "N/A"))

            # Sprawdź, czy 'departureFlight' istnieje oraz czy zawiera wymagane klucze
            departureFlight = row.get("departureFlight", {})
            data["departurePlace"].append(
                departureFlight.get("departure", {}).get("airportName", "N/A")
            )
            data["departureDate"].append(row.get("departureDate", "N/A"))
            data["returnDate"].append(row.get("returnDate", "N/A"))
            data["departureFlight_carrierName"].append(
                departureFlight.get("carrierName", "N/A")
            )
            data["departureFlight_hour"].append(
                str(
                    departureFlight.get("departure", {}).get("date", "N/A")
                    + " "
                    + departureFlight.get("departure", {}).get("time", "N/A")
                )
            )
            data["departureFlight_hour_dest"].append(
                departureFlight.get("arrival", {}).get("date", "N/A")
            )

            # Sprawdź, czy 'returnFlight' istnieje oraz czy zawiera wymagane klucze
            returnFlight = row.get("returnFlight", {})
            data["returnFlight_carrierName"].append(
                returnFlight.get("carrierName", "N/A")
            )
            data["returnFlight_hour"].append(
                returnFlight.get("departure", {}).get("date", "N/A")
            )
            data["returnFlight_hour_dest"].append(
                returnFlight.get("arrival", {}).get("date", "N/A")
            )

            data["tripRating"].append(row.get("tripAdvisorRating", "N/A"))
            data["provider"].append("TUI")

        json_data_wakacje = await get_wakacje_pl_data()
        for row in json_data_wakacje["data"]["offers"]:
            data["hotelCode"].append(row.get("id", "N/A"))
            data["hotelName"].append(row.get("name", "N/A"))
            
            data["roomName"].append(row.get("roomType", "N/A"))
            data["hotelStandard"].append(row.get("category", "N/A"))
            data["boardType"].append(row.get("serviceDesc", "N/A"))
            data["offerCode"].append(row.get("offerHash", "N/A"))
            data["duration"].append(row.get("duration", "N/A"))
            data["offerUrl"].append(row.get("link", "N/A"))

            place = row.get("place", {})
            country = place.get("country", {})
            data["Country"].append(country.get("name", "N/A"))

            region = place.get("region", {})
            data["City"].append(region.get("name", "N/A"))

            data["FullPrice"].append(int(row.get("originalCurrencyPrice", 0)))
            data["PriceCurrency"].append(row.get("originalCurrency", "N/A"))

            data["departurePlace"].append(row.get("departurePlace", "N/A"))
            data["departureDate"].append(row.get("departureDate", "N/A"))
            data["returnDate"].append(row.get("returnDate", "N/A"))

            data["departureFlight_carrierName"].append("n/a")
            data["departureFlight_hour"].append("n/a")
            data["departureFlight_hour_dest"].append("n/a")

            data["returnFlight_carrierName"].append("n/a")
            data["returnFlight_hour"].append("n/a")
            data["returnFlight_hour_dest"].append("n/a")

            data["tripRating"].append(row.get("ratingValue", "N/A"))
            data["provider"].append("WakacjePL - " + row.get("tourOperatorName", "N/A"))

        df = pd.DataFrame(data)

        df = df.reset_index()
        df = df.sort_values(by="FullPrice")
        # add_to_db(rows_to_add)
        message_string = ""
        message_len = 0
        for index, row in df.iterrows():
            usedstring = row["offerCode"] + str(row["FullPrice"])
            if usedstring not in used:
                used.append(usedstring)
                if row["provider"] == "TUI":
                    link = "https://tui.pl" + row["offerUrl"]
                else:
                    link = "https://wakacje.pl" + row["offerUrl"]

                message_string_temp = f"{escape_markdown('- - '+row['provider']+' - -')} 🔗[Link]({link})\n✈️ {escape_markdown(str(row['departurePlace']))}\n🌍{escape_markdown(str(row['Country'] + ' - ' + row['City']))}\n⭐{escape_markdown(str(row['hotelStandard']) + ' ' + row['boardType'])}\n📅{escape_markdown(str(row['departureDate'] + ' - ' + row['returnDate']))}\n💵 {escape_markdown(str(row['FullPrice']) + row['PriceCurrency'])} {escape_markdown(str('('+str(int(row['FullPrice'])/Adults_count) + row['PriceCurrency']+'/os)'))} \n\n"

                temp_length = len(message_string_temp.replace(link,"").replace("\n",""))-4

                if message_len + temp_length > 4096:
                    await send_message_async(bot, chat_id, message_string)
                    message_len = 0
                    message_string = ""
                
                message_len += temp_length
                message_string += message_string_temp

        await send_message_async(bot, chat_id, message_string)
        time.sleep(5)
    except Exception as e:
        log_error(e)


async def main():
    bot = Application.builder().token(bot_token).build()
    while True:
        # bot.bot.send_message()
        await send_messages(bot.bot, chat_id)
        await asyncio.sleep(60 * 5)


asyncio.run(main())
