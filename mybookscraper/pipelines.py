# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from forex_python.converter import CurrencyRates
from configparser import ConfigParser
import psycopg2


class MybookscraperPipeline:
    c = CurrencyRates()
    EXCHANGE_RATE = c.get_rate("GBP", "PHP")

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        self.strip_all_whitespaces(adapter)
        self.to_lowercase(adapter)
        self.convert_to_float(adapter)
        self.extract_number_of_availability(adapter)

        # convert num_reviews to int
        adapter["num_reviews"] = int(adapter.get("num_reviews"))

        self.convert_string_rating_to_int(adapter)

        return item

    def convert_string_rating_to_int(self, adapter):
        # convert rating from text to number
        rating_string = adapter.get("rating")
        rating_string = rating_string.split(" ")[1]
        rating_word_to_num = {
            "one": 1,
            "two": 2,
            "three": 3,
            "four": 4,
            "five": 5,
            "zero": 0,
        }
        adapter["rating"] = rating_word_to_num[rating_string]

    def extract_number_of_availability(self, adapter):
        # extract numbers only from availability
        availability_string = adapter.get("availability")
        split_string_array = availability_string.split("(")
        if len(split_string_array) < 2:
            adapter["availability"] = 0
        else:
            availability_array = split_string_array[1].split(" ")
            adapter["availability"] = int(availability_array[0])

    def convert_to_float(self, adapter):
        # Convert price to float
        price_columns = ["price", "price_excl_tax", "price_incl_tax", "tax"]
        for column in price_columns:
            value = adapter.get(column)
            value = float(value.replace("£", ""))
            adapter[column] = round(value * self.EXCHANGE_RATE, 2)

    def to_lowercase(self, adapter):
        # Switch to lowercase
        to_lowercase_columns = ["category", "product_type", "rating"]
        for column in to_lowercase_columns:
            adapter[column] = adapter.get(column).lower()

    def strip_all_whitespaces(self, adapter):
        # strip all whitespaces from strings
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != "description":
                adapter[field_name] = adapter.get(field_name).strip()


class SaveToPostgresDatabasePipeline:
    parser = ConfigParser()
    parser.read(".\\project.cfg")

    def __init__(self):
        self.connection = psycopg2.connect(
            dbname=self.parser.get("postgres_connection", "dbname"),
            user=self.parser.get("postgres_connection", "user"),
            password=self.parser.get("postgres_connection", "password"),
            host=self.parser.get("postgres_connection", "host"),
            port=self.parser.get("postgres_connection", "port"),
        )
        self.cursor = self.connection.cursor()
        self.create_table()

    def create_table(self):
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS books (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            category VARCHAR(255),
            price NUMERIC(10,2),
            rating INTEGER,
            description TEXT,
            upc VARCHAR(255),
            product_type VARCHAR(255),
            price_excl_tax NUMERIC(10,2),
            price_incl_tax NUMERIC(10,2),
            tax NUMERIC(10,2),
            availability INTEGER,
            num_reviews INTEGER,
            url VARCHAR(255))"""
        )

    def process_item(self, item, spider):
        INSERT_QUERY = """INSERT INTO books (title, category, price, rating, description, upc, product_type, price_excl_tax, price_incl_tax, tax, availability, num_reviews, url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s, %s, %s)"""

        data = (
            item["title"],
            item["category"],
            item["price"],
            item["rating"],
            item["description"],
            item["upc"],
            item["product_type"],
            item["price_excl_tax"],
            item["price_incl_tax"],
            item["tax"],
            item["availability"],
            item["num_reviews"],
            item["url"],
        )
        try:
            self.cursor.execute(INSERT_QUERY, data)
            self.connection.commit()
            print("\n\n")
            print("*" * 30)
            print("Data inserted successfully")
            print("*" * 30)
            print("\n\n")
        except Exception as e:
            print(e)
            self.connection.rollback()

        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()
