import scrapy
from datetime import datetime, UTC


class TopstoriesSpider(scrapy.Spider):
    name = "topstories"
    allowed_domains = ["news.ycombinator.com"]
    start_urls = ["https://news.ycombinator.com"]

    def parse(self, response):
        rows = response.css("tr.athing") # takes the table row items with a class of 'athing'

        for row in rows: 
            story_id = row.attrib.get("id")
            title = row.css(".titleline a::text").get()
            url = row.css(".titleline a::attr(href)").get()

            subtext = row.xpath("following-sibling::tr[1]") # Goes to the following sibling, and takes the second table row child
            score = subtext.css(".score::text").get()

            if score is None: 
                continue

            author = subtext.css(".hnuser::text").get()
            age = subtext.css(".age a::text").get()

            yield {
                "story_id" : story_id, 
                "title" : title, 
                "url" : url, 
                "score" : score, 
                "author" : author, 
                "age" : age, 
                "scraped_at" : datetime.now(UTC).isoformat()
            }
