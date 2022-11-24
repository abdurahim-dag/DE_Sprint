from urllib.parse import urlencode
from spider.items import HHFrontItem, HHVacancyItem


class HH:
    BASE_URL = "https://makhachkala.hh.ru/"
    param_urls = {
        "no_magic": "true",
        "L_save_area": "true",
        "text": "python разработчик",
        "currency_code": "RUR",
        "experience": "doesNotMatter",
        "order_by": "relevance",
        "search_period": "0",
        "items_on_page": "50",
        "hhtmFrom": "vacancy_search_list",
        "page": 39,
    }
    items = [
        {
            "item_class": HHFrontItem,
            "next_page": ["//span[contains(text(), 'дальше')]/parent::a/@href", None],
            "related_href": [
                "//span[contains(@data-page-analytics-event, 'vacancy_search_suitable_item')]//a/@href",
                None,
            ],
            "related": [
                {
                    "item_class": HHVacancyItem,
                    "title": [
                        "//h1[contains(@data-qa, 'vacancy-title')]//text()",
                        None,
                    ],
                    "work_experience": [
                        "//span[contains(@data-qa, 'vacancy-experience')]//text()",
                        None,
                    ],
                    "salary": [
                        "//span[contains(@data-qa, 'vacancy-salary-compensation-type-net')]//text()",
                        None,
                    ],
                    "region": [
                        "//p[contains(@data-qa, 'vacancy-view-location')]//text()",
                        None,
                    ],
                }
            ],
        }
    ]

    @classmethod
    def get_start_url(cls):
        return f"{cls.BASE_URL}/search/vacancy/?{urlencode(cls.param_urls)}"
