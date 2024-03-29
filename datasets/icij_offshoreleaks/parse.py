import io
import yaml
import click
from typing import Dict
from csv import DictReader
from functools import cache, lru_cache
from zipfile import ZipFile
from datetime import datetime
from normality import stringify, slugify
from datapatch import get_lookups
from zavod import Zavod, init_context
from zavod.logs import get_logger
from zavod.audit import audit_data
from nomenklatura.entity import CompositeEntity

from followthemoney import model
from followthemoney.types import registry

log = get_logger("offshoreleaks")

ENTITIES: Dict[str, CompositeEntity] = {}
DATE_FORMATS = [
    "%d-%b-%Y",
    "%b %d, %Y",
    "%Y-%m-%d",
    "%Y",
    "%d/%m/%Y",
    "%d.%m.%Y",
    "%d/%m/%y",
]
NODE_URL = "https://offshoreleaks.icij.org/nodes/%s"


@cache
def load_lookups():
    with open("patches.yml", "r", encoding="utf-8") as fh:
        data = yaml.load(fh, Loader=yaml.SafeLoader)
        return get_lookups(data)


@lru_cache(maxsize=10000)
def lookup(section, value):
    result = load_lookups()[section].match(value)
    if result is None:
        log.error(f"[{section}] missing value: {value}")
    return result


def make_entity_id(id):
    if id is None:
        return None
    return f"icijol-{id}"


@lru_cache(maxsize=1000)
def parse_date(text):
    if text is None:
        return None
    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(text, fmt)
            return dt.date().isoformat
        except ValueError:
            pass
    res = lookup("dates", text)
    if res is not None:
        return res.values
    # log.error("Unparseable date: %s", text)


@lru_cache(maxsize=10000)
def parse_countries(text):
    if text is None:
        return None
    if ";" in text:
        return [parse_countries(t) for t in text.split(";")]
    code = registry.country.clean_text(text)
    if code is None:
        result = lookup("countries", text)
        if result is not None:
            return [parse_countries(v) for v in result.values]
    return code
    # return text.split(",")


def emit_entity(proxy: CompositeEntity):
    assert proxy.id is not None, proxy
    if proxy.id in ENTITIES:
        schemata = [proxy.schema.name, ENTITIES[proxy.id].schema.name]
        if sorted(schemata) == sorted(["Asset", "Organization"]):
            proxy.schema = model.get("Company")
        if sorted(schemata) == sorted(["Asset", "LegalEntity"]):
            proxy.schema = model.get("Company")

        try:
            proxy = ENTITIES[proxy.id].merge(proxy)
        except Exception:
            print(proxy.schema, ENTITIES[proxy.id].schema)
            raise
    ENTITIES[proxy.id] = proxy


def dump_nodes(context: Zavod):
    context.log.info("Dumping %d nodes to: %s", len(ENTITIES), context.sink)
    for idx, entity in enumerate(ENTITIES.values()):
        assert not entity.schema.abstract, entity
        if entity.schema.name == "Address":
            continue
        context.emit(entity)
        if idx > 0 and idx % 10000 == 0:
            context.log.info("Dumped %d nodes..." % idx)


def read_rows(context, zip_path, file_name):
    with ZipFile(zip_path, "r") as zip:
        with zip.open(file_name) as zfh:
            fh = io.TextIOWrapper(zfh)
            reader = DictReader(fh, delimiter=",", quotechar='"')
            for idx, row in enumerate(reader):
                yield {k: stringify(v) for (k, v) in row.items()}
                if idx > 0 and idx % 10000 == 0:
                    context.log.info("[%s] Read %d rows...", file_name, idx)


def make_row_entity(context: Zavod, row, schema):
    # node_id = row.pop("id", row.pop("_id", row.pop("node_id", None)))
    node_id = row.pop("node_id", None)
    proxy = context.make(schema)
    proxy.id = make_entity_id(node_id)
    if proxy.id is None:
        context.log.error("No ID: %r", row)
        return
    name = row.pop("name", None)
    proxy.add("name", name)
    former_name = row.pop("former_name", None)
    if name != former_name:
        proxy.add("previousName", former_name)
    original_name = row.pop("original_name", None)
    if original_name != name:
        proxy.add("previousName", original_name)

    proxy.add("icijId", node_id)
    proxy.add("sourceUrl", NODE_URL % node_id)
    proxy.add("legalForm", row.pop("company_type", None))
    proxy.add("legalForm", row.pop("type", None))
    date = parse_date(row.pop("incorporation_date", None))
    proxy.add("incorporationDate", date)
    date = parse_date(row.pop("inactivation_date", None))
    proxy.add("dissolutionDate", date)
    date = parse_date(row.pop("struck_off_date", None))
    proxy.add("dissolutionDate", date)

    if proxy.schema.is_a("Organization"):
        proxy.add("topics", "corp.offshore")

    closed_date = parse_date(row.pop("closed_date", None))
    if proxy.has("dissolutionDate"):
        log.warning("Company has both dissolution date and closed date: %r", proxy)
    else:
        proxy.add("dissolutionDate", closed_date)

    dorm_date = parse_date(row.pop("dorm_date", None))
    if proxy.has("dissolutionDate"):
        log.warning("Company has both dissolution date and dorm date: %r", proxy)
    else:
        proxy.add("dissolutionDate", dorm_date)

    proxy.add("status", row.pop("status", None))
    proxy.add("publisher", row.pop("sourceID", None))
    proxy.add("notes", row.pop("valid_until", None))
    proxy.add("notes", row.pop("note", None))

    row.pop("jurisdiction", None)
    # countries = parse_countries()
    # proxy.add("jurisdiction", countries)
    countries = parse_countries(row.pop("jurisdiction_description", None))
    proxy.add("jurisdiction", countries)
    proxy.add("address", row.pop("address", None))

    countries = parse_countries(row.pop("country_codes", None))
    proxy.add("country", countries)

    countries = parse_countries(row.pop("countries", None))
    proxy.add("country", countries)
    proxy.add("program", row.pop("service_provider", None))

    proxy.add("registrationNumber", row.pop("ibcRUC", None), quiet=True)

    row.pop("internal_id", None)
    audit_data(row)
    emit_entity(proxy)


def make_row_address(context: Zavod, row):
    node_id = row.pop("node_id", None)
    proxy = context.make("Address")
    proxy.id = make_entity_id(node_id)
    proxy.add("full", row.pop("address", None))

    name = row.pop("name", None)
    proxy.add("full", name)
    # if name is not None:
    #     log.info("Name [%s] => [%s]", proxy.first("full"), name)

    row.pop("country_codes", None)
    countries = parse_countries(row.pop("countries"))
    proxy.add("country", countries)
    proxy.add("summary", row.pop("valid_until", None))
    proxy.add("remarks", row.pop("note", None))
    proxy.add("publisher", row.pop("sourceID", None))

    audit_data(row)
    emit_entity(proxy)


LINK_SEEN = set()


def make_row_relationship(context: Zavod, row):
    # print(row)
    # return
    _type = row.pop("rel_type")
    _start = row.pop("node_id_start")
    _end = row.pop("node_id_end")
    start = make_entity_id(_start)
    start_ent = ENTITIES.get(start)
    end = make_entity_id(_end)
    end_ent = ENTITIES.get(end)
    link = row.pop("link", None)
    source_id = row.pop("sourceID", None)
    start_date = parse_date(row.pop("start_date"))
    end_date = parse_date(row.pop("end_date"))

    try:
        res = lookup("relationships", link)
    except Exception as exc:
        context.log.exception("Unknown link: %s" % link)
        return

    if start_ent is None or end_ent is None:
        return

    if res is None:
        if link not in LINK_SEEN:
            # log.warning("Unknown link type: %s (%s, %s)", link, _type, row)
            LINK_SEEN.add(link)
        return

    if start_ent.schema.name == "Address":
        return

    if end_ent.schema.name == "Address" and start_ent.schema.is_a("Thing"):
        start_ent.add("address", end_ent.get("full"))
        start_ent.add("country", end_ent.get("country"))
        return

    if res.address:
        context.log.warn(
            "Address is not an address",
            start=start_ent,
            end=end_ent,
            link=link,
            type=_type,
        )
        return

    if end_ent is not None and end_ent.schema.name == "Address":
        context.log.warn("End is addr", link=link, end=end_ent)

    if res.schema is not None:
        rel = context.make(res.schema)
        rel_id = slugify(f"{_start}-{_end}-{link}")
        rel.id = make_entity_id(rel_id)
        rel.add("startDate", start_date)
        rel.add("endDate", end_date)
        rel.add(res.status, row.pop("status"))
        rel.add(res.link, link)
        rel.add("publisher", source_id)
        rel.add(res.start, start)
        rel.add(res.end, end)
        # emit_entity(rel)
        context.emit(rel)

        # this turns legalentity into organization in some cases
        start_ent = context.make(rel.schema.get(res.start).range)
        start_ent.id = start
        emit_entity(start_ent)

        end_ent = context.make(rel.schema.get(res.end).range)
        end_ent.id = end
        emit_entity(end_ent)

    audit_data(row)


@click.command()
@click.argument("zip_file", type=click.File(mode="rb"))
def make_db(zip_file):
    with init_context("metadata.yml") as context:
        context.log.info("Loading: nodes-entities.csv...")
        for row in read_rows(context, zip_file, "nodes-entities.csv"):
            make_row_entity(context, row, "Company")

        context.log.info("Loading: nodes-officers.csv...")
        for row in read_rows(context, zip_file, "nodes-officers.csv"):
            make_row_entity(context, row, "LegalEntity")

        context.log.info("Loading: nodes-intermediaries.csv...")
        for row in read_rows(context, zip_file, "nodes-intermediaries.csv"):
            make_row_entity(context, row, "LegalEntity")

        context.log.info("Loading: nodes-others.csv...")
        for row in read_rows(context, zip_file, "nodes-others.csv"):
            make_row_entity(context, row, "LegalEntity")

        context.log.info("Loading: nodes-addresses.csv...")
        for row in read_rows(context, zip_file, "nodes-addresses.csv"):
            make_row_address(context, row)

        context.log.info("Loading: relationships.csv...")
        for row in read_rows(context, zip_file, "relationships.csv"):
            make_row_relationship(context, row)

        context.export_metadata("export/index.json")
        dump_nodes(context)


if __name__ == "__main__":
    make_db()
