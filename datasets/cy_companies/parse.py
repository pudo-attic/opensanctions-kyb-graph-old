import csv
from datetime import datetime
from io import TextIOWrapper
from zipfile import ZipFile
from zavod import Zavod, init_context

from followthemoney.util import join_text

NAME = "cy_companies"
URL = "https://www.data.gov.cy/node/4016/dataset/download"
TYPES = {"C": "HE", "P": "S", "O": "AE", "N": "BN", "B": "B"}


def parse_date(text):
    if text is None or not len(text.strip()):
        return None
    return datetime.strptime(text, "%d/%m/%Y").date()


def company_id(org_type, reg_nr):
    org_type_oc = TYPES[org_type]
    return f"oc-companies-cy-{org_type_oc}{reg_nr}".lower()


def address_id(seq_nr):
    seq_nr = seq_nr.strip()
    if len(seq_nr):
        return f"cy-address-seq-{seq_nr}"


def iter_rows(zip: ZipFile, name: str):
    with zip.open(name, "r") as fh:
        wrapper = TextIOWrapper(fh, encoding="utf-8-sig")
        for row in csv.DictReader(wrapper):
            yield row


def parse_organisations(context: Zavod, rows):
    for row in rows:
        org_type = row.pop("ORGANISATION_TYPE_CODE")
        reg_nr = row.pop("REGISTRATION_NO")
        if org_type in ("", "Εμπορική Επωνυμία"):
            continue
        entity = context.make("Company")
        entity.id = company_id(org_type, reg_nr)
        entity.add("name", row.pop("ORGANISATION_NAME"))
        entity.add("status", row.pop("ORGANISATION_STATUS"))
        if org_type == "O":
            entity.add("country", "cy")
        else:
            entity.add("jurisdiction", "cy")
        org_type_oc = TYPES[org_type]
        oc_id = f"{org_type_oc}{reg_nr}"
        oc_url = f"https://opencorporates.com/companies/cy/{oc_id}"
        entity.add("opencorporatesUrl", oc_url)
        entity.add("registrationNumber", oc_id)
        entity.add("registrationNumber", f"{org_type}{reg_nr}")
        org_type_text = row.pop("ORGANISATION_TYPE")
        org_subtype = row.pop("ORGANISATION_SUB_TYPE")
        if len(org_subtype.strip()):
            org_type_text = f"{org_type_text} - {org_subtype}"
        entity.add("legalForm", org_type_text)
        reg_date = parse_date(row.pop("REGISTRATION_DATE"))
        entity.add("incorporationDate", reg_date)
        status_date = parse_date(row.pop("ORGANISATION_STATUS_DATE"))
        entity.add("modifiedAt", status_date)

        addr_id = address_id(row.pop("ADDRESS_SEQ_NO"))
        entity.add("addressEntity", addr_id)
        context.emit(entity)
        # print(entity.to_dict())


def parse_officials(context: Zavod, rows):
    org_types = list(TYPES.keys())
    for row in rows:
        org_type = row.pop("ORGANISATION_TYPE_CODE")
        if org_type not in org_types:
            continue
        reg_nr = row.pop("REGISTRATION_NO")
        name = row.pop("PERSON_OR_ORGANISATION_NAME")
        position = row.pop("OFFICIAL_POSITION")
        entity = context.make("LegalEntity")
        entity.id = context.make_id(org_type, reg_nr, name)
        entity.add("name", name)
        context.emit(entity)

        link = context.make("Directorship")
        link.id = context.make_id("Directorship", org_type, reg_nr, name, position)
        link.add("organization", company_id(org_type, reg_nr))
        link.add("director", entity.id)
        link.add("role", position)
        context.emit(link)


def parse_address(context: Zavod, rows):
    # org_types = list(TYPES.keys())
    for row in rows:
        entity = context.make("Address")
        entity.id = address_id(row.pop("ADDRESS_SEQ_NO"))
        if entity.id is None:
            continue
        entity.add("country", "cy")
        street = row.pop("STREET")
        entity.add("street", street)
        building = row.pop("BUILDING")
        entity.add("remarks", building)
        territory = row.pop("TERRITORY")
        entity.add("full", join_text(building, street, territory, sep=", "))
        context.emit(entity)
        # print(row)


def parse(context: Zavod):
    data_path = context.fetch_resource("data.zip", URL)
    with ZipFile(data_path, "r") as zip:
        for name in zip.namelist():
            context.log.info("Reading: %s in %s" % (name, data_path))
            if name.startswith("organisations_"):
                rows = iter_rows(zip, name)
                parse_organisations(context, rows)
            if name.startswith("organisation_officials_"):
                rows = iter_rows(zip, name)
                parse_officials(context, rows)
            if name.startswith("registered_office_"):
                rows = iter_rows(zip, name)
                parse_address(context, rows)


if __name__ == "__main__":
    with init_context("metadata.yml") as context:
        context.export_metadata("export/index.json")
        parse(context)
