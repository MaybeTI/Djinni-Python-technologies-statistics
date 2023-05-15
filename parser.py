import asyncio
import csv
import re
from dataclasses import dataclass, fields, astuple
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup

URL = "https://djinni.co/jobs/"


@dataclass
class Job:
    title: str
    company: str
    salary: int
    technologies: list[str]
    location: list[str]


JOB_FIELDS = [field.name for field in fields(Job)]


def salary_to_avg_int(salary_str: str) -> int:
    """
    Converts a salary string in the format '$XX,XXX - $YY,YYY' or '$ZZZ,ZZZ' to an integer
    representing the average salary.

    :param salary_str: A salary string in the format '$XX,XXX - $YY,YYY' or '$ZZZ,ZZZ'.
    :return int: The average salary as an integer.
    :raise ValueError: If the input string is not in a valid format.
    """
    salary_str = salary_str.replace("$", "").replace(",", "").replace(" ", "")
    if "-" in salary_str:
        min_salary, max_salary = salary_str.split("-")
        min_salary = re.findall(r"\d+", min_salary)
        max_salary = re.findall(r"\d+", max_salary)
        return (int(min_salary[0]) + int(max_salary[0])) // 2

    return int(re.findall(r"\d+", salary_str)[0])


async def get_job_info(session: aiohttp.ClientSession, url: str) -> Job:
    """
    Fetches job information from a given URL using an aiohttp session.

    :param session: An aiohttp ClientSession object to use for making HTTP requests.
    :param url: The URL of the job posting to fetch.
    :return Job: A Job object representing the job information fetched from the URL.
    :raise Exception: If there is an error fetching or parsing the job information.
    """
    async with session.get(urljoin(URL, url), ssl=False) as response:
        soup = BeautifulSoup(await response.text(), "html.parser")

        title = soup.select_one(".detail--title-wrapper > h1")

        company = soup.select_one(".job-details--title")

        salary = soup.select_one(".public-salary-item")
        if salary:
            salary = salary_to_avg_int(salary_str=salary.text)

        tech_spans = soup.select("li:contains('Категорія:') + li span")
        tech_list = [span.text for span in tech_spans[1:]]

        location_spans = (
            soup.select_one("span.location-text").contents[0].split(",")
        )
        location_list = [span.strip() for span in location_spans]

        return Job(
            title=title.contents[0].strip() if title else title,
            company=company.text.strip() if company else company,
            salary=salary,
            technologies=tech_list or None,
            location=location_list,
        )


async def parser() -> list[Job]:
    """
    Parses job information from multiple pages of job postings and returns a list of Job objects.

    :return List[Job]: A list of Job objects representing the job information parsed from the job postings.
    :raise Exception: If there is an error fetching or parsing the job information.
    """
    page_url = "?primary_keyword=Python&page=1"
    job_urls = []

    async with aiohttp.ClientSession(trust_env=True) as session:
        while True:
            async with session.get(
                urljoin(URL, page_url), ssl=False
            ) as response:
                soup = BeautifulSoup(await response.text(), "html.parser")
                job_urls.extend([a["href"] for a in soup.select("a.profile")])
                try:
                    page_url = soup.select_one(".d-md-none > a.btn-lg")["href"]
                except TypeError:
                    break

        return await asyncio.gather(
            *[get_job_info(session, url) for url in job_urls]
        )


def write_to_csv(job_list: list[Job], csv_path: str) -> None:
    """
    Writes the list of job objects to a CSV file at the specified path.

    :param job_list: A list of Job objects.
    :param csv_path: The path to the output CSV file.
    :return: None
    """
    with open(csv_path, "w") as file:
        writer = csv.writer(file)
        writer.writerow(JOB_FIELDS)
        writer.writerows([astuple(job) for job in job_list])


if __name__ == "__main__":
    job_list = asyncio.run(parser())
    write_to_csv(job_list=job_list, csv_path="jobs.csv")
