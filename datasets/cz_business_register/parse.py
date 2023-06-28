import tarfile
from normality import slugify
from io import BufferedReader
from typing import Optional

from followthemoney.util import make_entity_id
from lxml import etree
from nomenklatura.entity import CE
from zavod import Zavod, init_context
from zavod.parse import format_address
from zavod.parse.xml import ElementOrTree, remove_namespace

URL = "http://wwwinfo.mfcr.cz/ares/ares_vreo_all.tar.gz"


def company_id(
    context: Zavod, reg_nr: str, name: Optional[str] = None
) -> Optional[str]:
    if reg_nr:
        return f"oc-companies-cz-{reg_nr}"
    return context.make_slug("company", name)


def person_id(context: Zavod, name: str, address: str, company_id: str) -> str:
    if slugify(address) is not None:
        return context.make_slug("person", name, make_entity_id(address))
    return context.make_slug("person", name, make_entity_id(company_id))


def make_address(tree: Optional[ElementOrTree] = None) -> Optional[str]:
    if tree is None:
        return None

    components = {
        "stat": "state",
        "psc": "postal_code",
        "okres": "district",
        "obec": "city",
        "ulice": "street",
        "cisloTxt": "street_nr",
    }
    data = {}
    for path, key in components.items():
        data[key] = tree.findtext(path)

    summary_parts = []
    street = " ".join((data.pop("street") or "", data.pop("street_nr") or "")).strip()
    if street:
        summary_parts.append(street)
    district = data.pop("district")
    if district:
        summary_parts.append(district)
    data["summary"] = ", ".join(summary_parts).strip(", ")
    data["country_code"] = "cz"
    return format_address(**data)


def make_company(context: Zavod, tree: ElementOrTree) -> CE:
    tree = remove_namespace(tree)
    name = tree.findtext(".//ObchodniFirma")
    proxy = context.make("Company")
    reg_nr = tree.findtext(".//ICO")
    proxy.id = company_id(context, reg_nr, name)
    if proxy.id is not None:
        proxy.add("name", name)
        proxy.add("registrationNumber", reg_nr)
        proxy.add("address", make_address(tree.find(".//Sidlo")))
        proxy.add("incorporationDate", tree.findtext(".//DatumZapisu"))
        proxy.add("dissolutionDate", tree.findtext(".//DatumVymazu"))
        return proxy


def parse_xml(context: Zavod, reader: BufferedReader):
    tree = etree.parse(reader)
    company = make_company(context, tree)
    if company is not None:
        context.emit(company)
        for member in tree.findall(".//Clen"):
            proxy = context.make("Person")
            first_name = member.findtext("fosoba/jmeno")
            last_name = member.findtext("fosoba/prijmeni")
            proxy.add("firstName", first_name)
            proxy.add("lastName", last_name)
            if first_name and last_name:
                proxy.add("name", " ".join((first_name, last_name)))
            address = make_address(member.find(".//adresa"))
            proxy.add("address", address)
            proxy.id = person_id(context, proxy.caption, address, company.id)
            if proxy.id is not None:
                context.emit(proxy)

                role = member.findtext("funkce/nazev")
                if role is not None:
                    rel = context.make("Directorship")
                    rel.id = context.make_slug("directorship", company.id, proxy.id)
                    rel.add("role", role)
                    rel.add("director", proxy)
                    rel.add("organization", company)
                    context.emit(rel)


def parse(context: Zavod):
    data_path = context.fetch_resource("data.tar.gz", URL)
    ix = 0
    with tarfile.open(data_path, "r:gz") as f:
        archive_member = f.next()
        while archive_member is not None:
            ix += 1
            res = f.extractfile(archive_member)
            parse_xml(context, res)
            archive_member = f.next()
            if ix and ix % 10_000 == 0:
                context.log.info("Parse item %d ..." % ix)
    if ix:
        context.log.info("Parsed %d items." % (ix + 1), fp=data_path.name)


if __name__ == "__main__":
    with init_context("metadata.yml") as context:
        context.export_metadata("export/index.json")
        parse(context)
