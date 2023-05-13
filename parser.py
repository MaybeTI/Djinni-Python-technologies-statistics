import csv
import asyncio
import re
from dataclasses import dataclass, fields, astuple

import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin

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
    salary_str = salary_str.replace("$", "").replace(",", "").replace(" ", "")
    if "-" in salary_str:
        min_salary, max_salary = salary_str.split("-")
        min_salary = re.findall(r"\d+", min_salary)
        max_salary = re.findall(r"\d+", max_salary)
        return (int(min_salary[0]) + int(max_salary[0])) // 2

    return int(re.findall(r"\d+", salary_str)[0])


async def get_job_info(session: aiohttp.ClientSession, url: str) -> Job:
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
    with open(csv_path, "w") as file:
        writer = csv.writer(file)
        writer.writerow(JOB_FIELDS)
        writer.writerows([astuple(job) for job in job_list])


if __name__ == "__main__":
    job_list = asyncio.run(parser())
    write_to_csv(job_list=job_list, csv_path="jobs.csv")
