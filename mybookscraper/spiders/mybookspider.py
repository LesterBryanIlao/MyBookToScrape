import scrapy

from mybookscraper.items import BookItem


class MybookspiderSpider(scrapy.Spider):
    BASE_URL = "https://books.toscrape.com/"
    name = "mybookspider"
    allowed_domains = ["books.toscrape.com"]
    start_urls = ["https://books.toscrape.com"]

    def parse(self, response):
        books = response.css("article.product_pod")
        for book in books:
            relative_url = book.css("div a::attr(href)").get()
            yield response.follow(url=relative_url, callback=self.parse_book_page)

        next_page = response.css("li.next a::attr(href)").get()
        if next_page:
            if "catalogue/" not in next_page:
                next_page = "catalogue/" + next_page
            next_page_url = MybookspiderSpider.BASE_URL + next_page

            yield response.follow(url=next_page_url, callback=self.parse)

    def parse_book_page(self, response):
        # gets the tr descendants of the table element
        product_information = response.css("table tr")

        book_item = BookItem()
        #  note of the dot
        # '.product_main' means response has an element with class name of product_main
        # 'product_main' means response has an element product_main
        book_item["title"] = response.css(".product_main h1::text").get()

        # gets the nth preceding sibling of the li element with class active
        # Home (n=3)/ Books (n=3)/ Poetry (n=1)/ A Light in the Attic
        book_item["category"] = response.xpath(
            "//ul[@class='breadcrumb']/li[@class='active']/preceding-sibling::li[1]/a/text()"
        ).get()

        # gets the p element with class name price_color
        book_item["price"] = response.css("p.price_color::text").get()

        # book_item["rating"] = response.css("p.star-rating").attrib["class"]
        book_item["rating"] = response.css("p.star-rating::attr(class)").get()
        book_item["description"] = response.xpath(
            "//div[@id='product_description']/following-sibling::p/text()"
        ).get()

        # get all data from table
        book_item["upc"] = product_information[0].css("td::text").get()
        book_item["product_type"] = product_information[1].css("td::text").get()
        book_item["price_excl_tax"] = product_information[2].css("td::text").get()
        book_item["price_incl_tax"] = product_information[3].css("td::text").get()
        book_item["tax"] = product_information[4].css("td::text").get()
        book_item["availability"] = product_information[5].css("td::text").get()
        book_item["num_reviews"] = product_information[6].css("td::text").get()
        book_item["url"] = response.url

        yield book_item
