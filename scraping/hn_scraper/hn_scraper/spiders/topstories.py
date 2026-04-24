import scrapy
from datetime import datetime, UTC


class TopstoriesSpider(scrapy.Spider):
    name = "topstories"
    allowed_domains = ["ycombinator.com/", "news.ycombinator.com/"]
    start_urls = ["https://news.ycombinator.com/news"]
    page_count = 0
    max_pages = 100

    def parse(self, response):
        self.page_count += 1
    
        self.logger.info(f"SCRAPING PAGE {self.page_count} >>> {response.url}")
    
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
        next_page = response.css('a.morelink::attr(href)').get()

        if next_page:
            self.logger.info(f"MATCH FOUND: {next_page}")
        else:
            self.logger.info("MATCH FAILED: No 'More' button found in HTML.")

        if next_page and self.page_count < self.max_pages: 
            yield response.follow(next_page, callback=self.parse, dont_filter = True)

