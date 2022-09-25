import time
from scrapy import Spider, Request
from scrapy.loader import ItemLoader

from spider.hh import HH
from spider.items import HHVacancyItem


class MYSpider(Spider):
    name = "x_spider"
    context_spider = HH
    start_urls = [context_spider.get_start_url()]
    custom_settings = {
        "FEEDS": {
            "result.json": {
                "format": "json",
                "encoding": "utf8",
                "store_empty": True,
                "indent": 4,
                "item_classes": [HHVacancyItem],
                "item_export_kwargs": {
                    "export_empty_fields": True,
                },
            }
        },
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    }

    def parse(self, response):
        if response.meta.get("target_items"):
            items = response.meta["target_items"]
        else:
            items = self.context_spider.items
        for item in items:
            cur_item = item["item_class"]()
            l = ItemLoader(item=cur_item, response=response)
            for field in cur_item.fields:
                l.add_xpath(field, item[field][0], re=item[field][1])
            yield l.load_item()
            if "next_page" in cur_item.fields:
                next_page = l.get_output_value("next_page")
                if next_page:
                    time.sleep(3)
                    yield Request(
                        url=self.context_spider.BASE_URL + cur_item.next_page,
                        callback=self.parse,
                        meta={"target_items": items},
                    )
            if "related_href" in cur_item.fields:
                related_href = l.get_output_value("related_href")
                if related_href:
                    for href in related_href:
                        time.sleep(3)
                        yield Request(
                            url=href,
                            callback=self.parse,
                            meta={"target_items": item["related"]},
                        )
