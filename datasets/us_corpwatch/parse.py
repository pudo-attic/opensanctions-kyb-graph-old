import csv
from functools import lru_cache
from pathlib import Path
from typing import Callable, Optional

from fingerprints import generate as fp
from nomenklatura.entity import CE
from zavod import Zavod, init_context
from zavod.parse.addresses import format_line


@lru_cache(1_000_000)
def clean(value: Optional[str] = None) -> Optional[str]:
    if value is None:
        return None
    if value.lower().strip() == "null":
        return None
    return value


def make_proxy(context: Zavod, cw_id: str, name: Optional[str] = None) -> CE:
    proxy = context.make("Company")
    proxy.id = context.make_slug(cw_id)
    proxy.add("name", clean(name))
    return proxy


def parse_companies(context: Zavod, row: dict):
    proxy = make_proxy(context, row.pop("cw_id"), row.pop("company_name"))
    context.emit(proxy)


def parse_company_info(context: Zavod, row: dict):
    proxy = make_proxy(context, row.pop("cw_id"), row.pop("company_name"))
    proxy.add("sector", clean(row.pop("industry_name")))
    proxy.add("sector", clean(row.pop("sector_name")))
    proxy.add("registrationNumber", clean(row.pop("irs_number")))
    context.emit(proxy)


def parse_company_names(context: Zavod, row: dict):
    proxy = make_proxy(context, row.pop("cw_id"))
    proxy.add("country", clean(row.pop("country_code")))
    name_type = row.pop("source")
    name = clean(row.pop("company_name"))
    if name_type == "cik_former_name":
        proxy.add("previousName", name)
    else:
        proxy.add("name", name)
    context.emit(proxy)


def parse_company_locations(context: Zavod, row: dict):
    proxy = make_proxy(context, row.pop("cw_id"))
    country_code = clean(row.pop("country_code")) or ""
    proxy.add("country", country_code)
    street = [s for s in (row.pop("street_1"), row.pop("street_2")) if clean(s)]
    street = ", ".join(street)
    address = format_line(
        street=street,
        postal_code=clean(row.pop("postal_code")),
        city=clean(row.pop("city")),
        state=clean(row.pop("state")),
        country_code=country_code.lower(),
    )
    if fp(address):  # exclude
        proxy.add("address", address)
    context.emit(proxy)


def parse_company_relations(context: Zavod, row: dict):
    source = make_proxy(context, row.pop("source_cw_id"))
    target = make_proxy(context, row.pop("target_cw_id"))
    target.add("parent", source)
    context.emit(source)
    context.emit(target)


def parse_relationships(context: Zavod, row: dict):
    if row.pop("ignore_record") != "0":
        return
    year = clean(row.pop("year"))
    percentage = clean(row.pop("percent"))
    if percentage or year:
        parent_id, child_id = row.pop("parent_cw_id"), row.pop("cw_id")
        if parent_id and child_id:
            parent = make_proxy(context, parent_id)
            child = make_proxy(context, child_id)
            rel = context.make("Ownership")
            rel.id = context.make_slug("ownership", parent.id, child.id)
            rel.add("owner", parent)
            rel.add("asset", child)
            rel.add("percentage", percentage)
            rel.add("date", year)
            context.emit(parent)
            context.emit(child)
            context.emit(rel)


def parse_csv(context: Zavod, data_path: Path, handler: Callable):
    context.log.info(f"Parsing `{data_path}` ...")
    ix = 0
    with open(data_path) as f:
        reader = csv.DictReader(f, delimiter="\t")
        for ix, row in enumerate(reader):
            handler(context, row)
            if ix and ix % 100_000 == 0:
                context.log.info(f"Parse record {ix}...")
    context.log.info(f"Parsed {ix} rows", fp=data_path.name)


def parse(context: Zavod):
    base_path = Path("src") / "corpwatch_api_tables_csv"
    data_path = context.get_resource_path(base_path / "companies.csv")
    parse_csv(context, data_path, parse_companies)
    data_path = context.get_resource_path(base_path / "company_info.csv")
    parse_csv(context, data_path, parse_company_info)
    data_path = context.get_resource_path(base_path / "company_names.csv")
    parse_csv(context, data_path, parse_company_names)
    data_path = context.get_resource_path(base_path / "company_locations.csv")
    parse_csv(context, data_path, parse_company_locations)
    data_path = context.get_resource_path(base_path / "company_relations.csv")
    parse_csv(context, data_path, parse_company_relations)
    data_path = context.get_resource_path(base_path / "relationships.csv")
    parse_csv(context, data_path, parse_relationships)


if __name__ == "__main__":
    with init_context("metadata.yml") as context:
        context.export_metadata("export/index.json")
        parse(context)