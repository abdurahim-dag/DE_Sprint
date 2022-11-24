from scrapy import Field, Item
from scrapy.loader.processors import MapCompose, TakeFirst, Join, Identity


class HHFrontItem(Item):
    next_page = Field(
        input_processor=TakeFirst(),
        output_processor=Join(),
    )

    related_href = Field(
        input_processor=Identity(),
        output_processor=Identity(),
    )


class HHVacancyItem(Item):
    title = Field(
        input_processor=TakeFirst(),
        output_processor=Join(),
    )
    work_experience = Field(
        input_processor=MapCompose(),
        output_processor=Join(),
    )
    salary = Field(
        input_processor=MapCompose(),
        output_processor=Join(),
    )
    region = Field(
        input_processor=MapCompose(),
        output_processor=Join(),
    )
