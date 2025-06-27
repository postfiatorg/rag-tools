import logging
from collections.abc import Callable
from typing import Any, override

import pandas as pd
from langchain_community.document_loaders.unstructured import Element, UnstructuredBaseLoader
from unstructured.partition.pdf import partition_pdf

from agti.central_banks.types import SupportedScrapers
from agti.central_banks.utils import Categories

from ..utils import remove_special_characters

logger = logging.getLogger(__name__)

class UnstructuredParsingLoader(UnstructuredBaseLoader):
    def __init__(
        self,
        file_path: str,
        main_db : pd.DataFrame,
        links_db: pd.DataFrame,
        categories_db: pd.DataFrame,
        mode: str = "single",
        post_processors: list[Callable] | None = None,
        **unstructured_kwargs: Any,
    ):
        super().__init__(mode, post_processors, **unstructured_kwargs)
        self.file_path = file_path
        self.file_id = file_path.split("/")[-1].split(".")[0]
        self.main_db = main_db
        self.links_db = links_db
        self.categories_db = categories_db


    @override
    def _get_elements(self) -> list[Element]:
        return partition_pdf(
            self.file_path,
            **self.unstructured_kwargs,
        )

    @override
    def _get_metadata(self) -> dict[str, Any]:
        # we return original_url, s3_url, date_published ( or date listed as and link)
        # Categories,
        metadata = {}
        year = self.file_path.split("/")[-2]
        assert year.isdigit(), "Year should be a digit, path should be like 'path/to/year/file_id.pdf'"
        metadata["s3_url"] = f"https://agti-central-banks.s3.us-east-1.amazonaws.com/{year}/{self.file_id}.pdf"
        metadata["file_id"] = self.file_id
        metadata["year"] = year

        # try to find it in the main_db
        main_mask = self.main_db["file_id"] == self.file_id
        # if not found try to find it in the links_db

        links_file_urls = None
        if main_mask.any():
            assert main_mask.sum() == 1, "Multiple entries found in main_db for file_id"
            file_url = self.main_db.loc[main_mask, "file_url"].values[0]
            original_url = file_url
            metadata["date_published"] = self.main_db.loc[main_mask, "date_published"].values[0]
            metadata["date_published_str"] = self.main_db.loc[main_mask, "date_published_str"].values[0]

        else:
            metadata["date_published"] = None
            metadata["date_published_str"] = None

            links_mask = self.links_db["file_id"] == self.file_id
            assert links_mask.any(), f"File ID {self.file_id} not found in main_db or links_db"
            links_file_urls = self.links_db.loc[links_mask, "file_url"].drop_duplicates().to_list()

            # find the original link
            links_urls = self.links_db.loc[links_mask, "link_url"].drop_duplicates().to_list()
            if  len(links_urls) != 1:
                logger.warning(
                    f"Multiple link URLs found for file_id {self.file_id} in links_db: {links_urls}"
                )
            original_url = links_urls[0]

            # date_published not available in links_db
            # it can have multiple date_published, because it can be a link to a page with multiple files
            all_dates = self.main_db[self.main_db['file_url'].isin(links_file_urls)].dropna(subset=['date_published'])['date_published'].to_list()
            all_dates_str = self.main_db[self.main_db['file_url'].isin(links_file_urls)].dropna(subset=['date_published_str'])['date_published_str'].to_list()
            metadata["link_date_mentioned_str"] = ", ".join(all_dates_str) if all_dates_str else None
            metadata["link_date_mentioned"] = ", ".join(all_dates) if all_dates else None

            # solve here date published multiple file_url can point to this link

        metadata["original_url"] = original_url

        all_urls = set([original_url])
        # if it is a link add all urls from the links_db
        if links_file_urls:
            all_urls.update(links_file_urls)
        url_categories = self.categories_db[self.categories_db['file_url'].isin(all_urls)]['category_name'].to_list()

        # add Categories
        for category in Categories:
            key_value = remove_special_characters(category.value)
            metadata[key_value] = 0

        for category in url_categories:
            key_value = remove_special_characters(category)
            metadata[key_value] = 1

        return metadata











if __name__ == "__main__":
    import glob
    import itertools
    import os
    import time

    import pandas as pd
    # set info level for the logger
    logging.basicConfig(level=logging.INFO)
    years = ["x"]
    country_codes = [
        country.value.COUNTRY_CODE_ALPHA_3 for country in SupportedScrapers
    ]
    main_db = pd.read_csv("data/sqldata/central_banks_g10.csv")
    links_db = pd.read_csv("data/sqldata/central_banks_g10_links.csv")
    categories_db = pd.read_csv("data/sqldata/central_banks_g10_categories.csv")
    docs = []
    for (year, country_code) in itertools.product(years, country_codes):
        logger.info(f"Processing year: {year}, country_code: {country_code}")
        all_files = glob.glob(f"data/agti-central-banks/{country_code}/**/*.pdf", recursive=True)
        count = 0

        all_files = pd.DataFrame(all_files, columns=["file_path"])
        all_files["file_id"] = all_files["file_path"].apply(lambda x: os.path.basename(x).split(".")[0])
        not_in_both = all_files[~all_files["file_id"].isin(main_db["file_id"]) & ~all_files["file_id"].isin(links_db["file_id"])]

        print(not_in_both['file_path'].values)
        continue
        """
        TODO:
        # CHE, CAN solved already need to fix AUS because this files are missing in DB nbecauses error rises.
        # then update DVC
        ❯ poetry run python -m rag_tools.parsing.unstructured_parsing
/home/lukaskiss/.cache/pypoetry/virtualenvs/rag-tools-eTHlKuSf-py3.13/lib/python3.13/site-packages/seleniumwire/thirdparty/mitmproxy/contrib/kaitaistruct/tls_client_hello.py:10: UserWarning: pkg_resources is deprecated as an API. See https://setuptools.pypa.io/en/latest/pkg_resources.html. The pkg_resources package is slated for removal as early as 2025-11-30. Refrain from using this package or pin to Setuptools<81.
  from pkg_resources import parse_version
INFO:__main__:Processing year: x, country_code: AUS
['data/agti-central-banks/AUS/1960/18d133f40651273fbe710e8926c1d1f80640c9ee.pdf'
 'data/agti-central-banks/AUS/1960/cfc3b66242d0f949c82cc8206b668aa8e85f53f1.pdf'
 'data/agti-central-banks/AUS/1960/e56006a323c5e6043f3e5670f87c494018e9023e.pdf'
 'data/agti-central-banks/AUS/1960/38d848c896446666450dcde3ea0b44933bfbe0c9.pdf'
 'data/agti-central-banks/AUS/1960/8be89126d550e8684496970597a6b3d39e9fb4a1.pdf'
 'data/agti-central-banks/AUS/1960/bf706daf7d82789cfd0628d2db43a66039cd0f67.pdf'
 'data/agti-central-banks/AUS/1960/ca7757bfcd519adce1f5408b621af5e42cf47f3b.pdf'
 'data/agti-central-banks/AUS/1960/ab5730333572a8f6dfa7bd4393f3bed2eae6597c.pdf'
 'data/agti-central-banks/AUS/1960/f2f909e5201dab3249c9ceead9c326f2a47ba34d.pdf'
 'data/agti-central-banks/AUS/1960/4dd650b061f0e4d68faebd2b838de277de3030b6.pdf'
 'data/agti-central-banks/AUS/1960/2866fd6f291568f0b69f42a3e931062e4b58bfda.pdf'
 'data/agti-central-banks/AUS/1960/6005e7768819fdcdff9ad643da6e1895b1096535.pdf'
 'data/agti-central-banks/AUS/1960/61800ea6ac07368e553b0119bf8b246903325fcb.pdf'
 'data/agti-central-banks/AUS/1960/ec617e55a0b8262e0c1b6ab2430eb74ee5d033d1.pdf']
INFO:__main__:Processing year: x, country_code: CAN
['data/agti-central-banks/CAN/2021/bd33412fec5f58051283b53b6b07a4c1ffc25a16.pdf'
 'data/agti-central-banks/CAN/2023/c49f12329004c4d963d34c7d0dbabda57495633f.pdf'
 'data/agti-central-banks/CAN/2024/69707e2f397fb62d5826a19fad13acf1eb8996bd.pdf'
 'data/agti-central-banks/CAN/2024/b08d5e9a189c2b6ae0437efbd904ac65c4bd5625.pdf']
INFO:__main__:Processing year: x, country_code: EUE
[]
INFO:__main__:Processing year: x, country_code: ENG
[]
INFO:__main__:Processing year: x, country_code: USA
[]
INFO:__main__:Processing year: x, country_code: JPN
[]
INFO:__main__:Processing year: x, country_code: NOR
[]
INFO:__main__:Processing year: x, country_code: SWE
[]
INFO:__main__:Processing year: x, country_code: CHE
['data/agti-central-banks/CHE/unknown/63f3a5d750d538840a28b00dce56a5e7f4ca6961.pdf']
INFO:__main__:Total documents parsed: 0

        
        fix this:
        central_banks_scrapper_aus  | 2025-06-27 07:02:50,277 - agti.agti.central_banks.scrapers.australia - INFO - Processing: https://www.rba.gov.au/publications/annual-reports/rba/1960/
central_banks_scrapper_aus  | 2025-06-27 07:02:52,137 - agti.agti.central_banks.base_scrapper - INFO - Saved page as PDF: /app/datadump/cfc3b66242d0f949c82cc8206b668aa8e85f53f1.pdf
central_banks_scrapper_aus  | 2025-06-27 07:02:52,245 - agti.agti.central_banks.base_scrapper - INFO - File already exists in S3: AUS/1960/cfc3b66242d0f949c82cc8206b668aa8e85f53f1.pdf
central_banks_scrapper_aus  | 2025-06-27 07:02:53,732 - agti.agti.central_banks.base_scrapper - INFO - Saved page as PDF: /app/datadump/ab5730333572a8f6dfa7bd4393f3bed2eae6597c.pdf
central_banks_scrapper_aus  | 2025-06-27 07:02:53,750 - agti.agti.central_banks.base_scrapper - INFO - File already exists in S3: AUS/1960/ab5730333572a8f6dfa7bd4393f3bed2eae6597c.pdf
central_banks_scrapper_aus  | 2025-06-27 07:02:55,187 - agti.agti.central_banks.base_scrapper - INFO - Saved page as PDF: /app/datadump/38d848c896446666450dcde3ea0b44933bfbe0c9.pdf
central_banks_scrapper_aus  | 2025-06-27 07:02:55,226 - agti.agti.central_banks.base_scrapper - INFO - File already exists in S3: AUS/1960/38d848c896446666450dcde3ea0b44933bfbe0c9.pdf
central_banks_scrapper_aus  | 2025-06-27 07:02:57,784 - agti.agti.central_banks.base_scrapper - INFO - Saved page as PDF: /app/datadump/e56006a323c5e6043f3e5670f87c494018e9023e.pdf
central_banks_scrapper_aus  | 2025-06-27 07:02:57,799 - agti.agti.central_banks.base_scrapper - INFO - File already exists in S3: AUS/1960/e56006a323c5e6043f3e5670f87c494018e9023e.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:01,403 - agti.agti.central_banks.base_scrapper - INFO - Saved page as PDF: /app/datadump/ec617e55a0b8262e0c1b6ab2430eb74ee5d033d1.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:01,453 - agti.agti.central_banks.base_scrapper - INFO - File already exists in S3: AUS/1960/ec617e55a0b8262e0c1b6ab2430eb74ee5d033d1.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:06,189 - agti.agti.central_banks.base_scrapper - INFO - Saved page as PDF: /app/datadump/61800ea6ac07368e553b0119bf8b246903325fcb.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:06,222 - agti.agti.central_banks.base_scrapper - INFO - File already exists in S3: AUS/1960/61800ea6ac07368e553b0119bf8b246903325fcb.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:09,551 - agti.agti.central_banks.base_scrapper - INFO - Saved page as PDF: /app/datadump/6005e7768819fdcdff9ad643da6e1895b1096535.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:09,583 - agti.agti.central_banks.base_scrapper - INFO - File already exists in S3: AUS/1960/6005e7768819fdcdff9ad643da6e1895b1096535.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:11,024 - agti.agti.central_banks.base_scrapper - INFO - Saved page as PDF: /app/datadump/ca7757bfcd519adce1f5408b621af5e42cf47f3b.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:11,089 - agti.agti.central_banks.base_scrapper - INFO - File already exists in S3: AUS/1960/ca7757bfcd519adce1f5408b621af5e42cf47f3b.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:14,184 - agti.agti.central_banks.base_scrapper - INFO - Saved page as PDF: /app/datadump/bf706daf7d82789cfd0628d2db43a66039cd0f67.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:14,237 - agti.agti.central_banks.base_scrapper - INFO - File already exists in S3: AUS/1960/bf706daf7d82789cfd0628d2db43a66039cd0f67.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:16,397 - agti.agti.central_banks.base_scrapper - DEBUG - Refreshing headers
central_banks_scrapper_aus  | 2025-06-27 07:03:17,179 - agti.agti.central_banks.base_scrapper - DEBUG - Cookies initialized
central_banks_scrapper_aus  | 2025-06-27 07:03:17,235 - agti.agti.central_banks.base_scrapper - INFO - Saved page as PDF: /app/datadump/f2f909e5201dab3249c9ceead9c326f2a47ba34d.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:17,249 - agti.agti.central_banks.base_scrapper - INFO - File already exists in S3: AUS/1960/f2f909e5201dab3249c9ceead9c326f2a47ba34d.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:18,675 - agti.agti.central_banks.base_scrapper - INFO - Saved page as PDF: /app/datadump/18d133f40651273fbe710e8926c1d1f80640c9ee.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:18,814 - agti.agti.central_banks.base_scrapper - INFO - File already exists in S3: AUS/1960/18d133f40651273fbe710e8926c1d1f80640c9ee.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:20,746 - agti.agti.central_banks.base_scrapper - INFO - Saved page as PDF: /app/datadump/8be89126d550e8684496970597a6b3d39e9fb4a1.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:20,795 - agti.agti.central_banks.base_scrapper - INFO - File already exists in S3: AUS/1960/8be89126d550e8684496970597a6b3d39e9fb4a1.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:22,283 - agti.agti.central_banks.base_scrapper - INFO - Saved page as PDF: /app/datadump/2866fd6f291568f0b69f42a3e931062e4b58bfda.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:22,337 - agti.agti.central_banks.base_scrapper - INFO - File already exists in S3: AUS/1960/2866fd6f291568f0b69f42a3e931062e4b58bfda.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:24,724 - agti.agti.central_banks.base_scrapper - INFO - Saved page as PDF: /app/datadump/4dd650b061f0e4d68faebd2b838de277de3030b6.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:24,757 - agti.agti.central_banks.base_scrapper - INFO - File already exists in S3: AUS/1960/4dd650b061f0e4d68faebd2b838de277de3030b6.pdf
central_banks_scrapper_aus  | 2025-06-27 07:03:24,823 - __main__ - ERROR - Error processing bank: SupportedScrapers.AUSTRALIA
central_banks_scrapper_aus  | Traceback (most recent call last):
central_banks_scrapper_aus  |   File "/app/central_bank_scraper/main.py", line 142, in run
central_banks_scrapper_aus  |     scraper.process_all_years()
central_banks_scrapper_aus  |   File "/app/agti/agti/central_banks/scrapers/australia.py", line 1736, in process_all_years
central_banks_scrapper_aus  |     self.process_publications()
central_banks_scrapper_aus  |   File "/app/agti/agti/central_banks/scrapers/australia.py", line 1523, in process_publications
central_banks_scrapper_aus  |     for a_tag in ul.find_elements(By.XPATH, ".//a"):
central_banks_scrapper_aus  |                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
central_banks_scrapper_aus  |   File "/usr/local/lib/python3.11/site-packages/selenium/webdriver/remote/webelement.py", line 628, in find_elements
central_banks_scrapper_aus  |     return self._execute(Command.FIND_CHILD_ELEMENTS, {"using": by, "value": value})["value"]
central_banks_scrapper_aus  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
central_banks_scrapper_aus  |   File "/usr/local/lib/python3.11/site-packages/selenium/webdriver/remote/webelement.py", line 570, in _execute
central_banks_scrapper_aus  |     return self._parent.execute(command, params)
central_banks_scrapper_aus  |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
central_banks_scrapper_aus  |   File "/usr/local/lib/python3.11/site-packages/selenium/webdriver/remote/webdriver.py", line 429, in execute
central_banks_scrapper_aus  |     self.error_handler.check_response(response)
central_banks_scrapper_aus  |   File "/usr/local/lib/python3.11/site-packages/selenium/webdriver/remote/errorhandler.py", line 232, in check_response
central_banks_scrapper_aus  |     raise exception_class(message, screen, stacktrace)
central_banks_scrapper_aus  | selenium.common.exceptions.StaleElementReferenceException: Message: stale element reference: stale element not found
central_banks_scrapper_aus  |   (Session info: chrome=133.0.6943.126); For documentation on this error, please visit: https://www.selenium.dev/documentation/webdriver/troubleshooting/errors#stale-element-reference-exception

        
        
        
        
        
        """


        for file_path in all_files:
            parser = UnstructuredParsingLoader(
                file_path=file_path,
                main_db=main_db,
                links_db=links_db,
                categories_db=categories_db,
            )
            logger.info(f"Parsing file: {file_path}, count: {count + 1}/{len(all_files)}")
            start_time = time.time()
            doc = parser.load()
            docs.extend(doc)
            end_time = time.time()
            logger.info(f"Parsed {file_path} in {end_time - start_time:.2f} seconds")
            count += 1

    logger.info(f"Total documents parsed: {len(docs)}")

