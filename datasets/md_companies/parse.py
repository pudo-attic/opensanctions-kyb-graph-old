import openpyxl
from lxml import html
from typing import Optional
from urllib.parse import urljoin
from zavod import init_context, Zavod


def read_ckan(context: Zavod) -> str:
    if context.dataset.url is None:
        raise RuntimeError("No dataset url")
    path = context.fetch_resource("dataset.html", context.dataset.url)
    with open(path, "r") as fh:
        doc = html.fromstring(fh.read())

    resource_url = None
    for res_anchor in doc.findall('.//li[@class="resource-item"]/a'):
        resource_url = urljoin(context.dataset.url, res_anchor.get("href"))

    if resource_url is None:
        raise RuntimeError("No resource URL on data catalog page!")

    path = context.fetch_resource("resource.html", resource_url)
    with open(path, "r") as fh:
        doc = html.fromstring(fh.read())

    for action_anchor in doc.findall('.//div[@class="actions"]//a'):
        return action_anchor.get("href")

    raise RuntimeError("No data URL on data resource page!")


def parse_companies(context: Zavod, book: openpyxl.Workbook):
    header = None
    for row in book["Company"].iter_rows():
        cells = [c.value for c in row]
        if header is None:
            if "Denumirea completă" in cells:
                header = cells
                # print(header)
            continue
        data = dict(zip(header, cells))
        print(data)


def parse(context: Zavod):
    data_url = read_ckan(context)
    data_path = context.fetch_resource("data.xlsx", data_url)
    wb = openpyxl.load_workbook(data_path, read_only=True, data_only=True)
    parse_companies(context, wb)


if __name__ == "__main__":
    with init_context("metadata.yml") as context:
        context.export_metadata("export/index.json")
        parse(context)
