from datetime import datetime
from typing import Any, Callable, Optional, Tuple

import ijson
from followthemoney.util import make_entity_id
from nomenklatura.entity import CE
from zavod import Zavod, init_context

# https://avaandmed.ariregister.rik.ee/en/downloading-open-data
SOURCES = {
    "general": "ettevotja_rekvisiidid__yldandmed.json",
    "officers1": "ettevotja_rekvisiidid__kaardile_kantud_isikud.json",
    "officers2": "ettevotja_rekvisiidid__kandevalised_isikud.json",
    "bfo": "ettevotja_rekvisiidid__kasusaajad.json",
}

TYPES = {
    "Füüsilisest isikust ettevõtja": "Person",  # Self-employed person
    "Kohaliku omavalitsuse asutus": "PublicBody",  # Local government body
    "Mittetulundusühing": "Organization",  # Non-profit association
    "Täidesaatva riigivõimu asutus või riigi muu institutsioon": "PublicBody",  # Executive or other public institution
    "Sihtasutus": "Organization",  # Foundation
}


def get_value(
    data: dict, keys: Tuple[str], default: Optional[Any] = None
) -> Optional[str]:
    for key in keys:
        val = data.pop(key, None)
        if val is not None:
            return val
    return default


def parse_date(value: Optional[str] = None) -> Optional[str]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%d.%m.%Y").date().isoformat()
    except ValueError:
        return None


def get_address(data: dict) -> Optional[str]:
    value = data.pop("aadress_ads__ads_normaliseeritud_taisaadress", None)
    if value is not None:
        return value
    parts = []
    for key in ("aadress_ehak_tekstina", "aadress_tanav_maja_korter"):
        value = data.pop(key, None)
        if value is not None:
            parts.append(value)
    if parts:
        return ", ".join(parts).strip(", ")


def make_proxy(context: Zavod, row: dict, schema: Optional[str] = "LegalEntity") -> CE:
    proxy = context.make(schema)
    ident = row.pop("ariregistri_kood")
    proxy.id = context.make_slug(ident)
    proxy.add("registrationNumber", ident)
    proxy.add("name", row.pop("nimi"))
    proxy.add("sourceUrl", f"https://ariregister.rik.ee/eng/company/{ident}")
    proxy.add("jurisdiction", "ee")
    return proxy


def make_officer(context: Zavod, data: dict, company_id: str) -> CE:
    legal_form = data.pop("isiku_tyyp", None)
    id_number = get_value(data, ("isikukood_registrikood", "isikukood"))
    first_name, last_name = data.pop("eesnimi", None), get_value(
        data, ("nimi_arinimi", "nimi")
    )
    if legal_form == "F":
        proxy = context.make("Person")
        proxy.add("idNumber", id_number)
        proxy.add("firstName", first_name)
        proxy.add("lastName", last_name)
        if first_name and last_name:
            proxy.add("name", " ".join((first_name, last_name)))
    else:
        proxy = context.make("LegalEntity")
        proxy.add("name", " ".join((first_name or "", last_name or "")).strip())

    address = get_address(data)
    proxy.id = context.make_slug(id_number)
    if proxy.id is None:
        ident_id = make_entity_id(address)
        if ident_id is None:
            ident_id = id_number or company_id
        proxy.id = context.make_slug("officer", proxy.caption, ident_id)

    proxy.add("registrationNumber", id_number)
    proxy.add("country", data.pop("aadress_riik"))
    proxy.add("email", data.pop("email", None))
    proxy.add("address", address)
    return proxy


def make_rel(
    context, company: CE, officer: CE, schema: str, data, role: Optional[str] = None
) -> CE:
    rel = context.make(schema)
    rel.id = context.make_slug(
        rel.schema.name, company.id, officer.id, role, strict=False
    )
    rel.add("startDate", parse_date(data.pop("algus_kpv")))
    rel.add("endDate", parse_date(data.pop("lopp_kpv")))
    rel.add("role", role)
    if schema == "Ownership":
        rel.add("owner", officer)
        rel.add("asset", company)
    elif schema == "Directorship":
        rel.add("director", officer)
        rel.add("organization", company)
    return rel


def parse_general(context: Zavod, row: dict):
    data = row.pop("yldandmed")
    legal_form = data.pop("oiguslik_vorm_tekstina")
    proxy = make_proxy(context, row, TYPES.get(legal_form, "Company"))
    proxy.add("legalForm", legal_form)
    for item in data.pop("staatused"):
        if item["staatus"] == "R":
            proxy.add("incorporationDate", parse_date(item["algus_kpv"]))
    for item in get_value(data, ("aadressid", "kontaktisiku_aadressid"), []):
        proxy.add("address", get_address(item))
    proxy.add(
        "status", get_value(data, ("staatus_tekstina", "ettevotja_staatus_tekstina"))
    )

    for contact in data.pop("sidevahendid", []):
        if contact["liik"] == "EMAIL":
            proxy.add("email", contact["sisu"])
        elif contact["liik"] == "WWW":
            proxy.add("website", contact["sisu"])
        elif contact["liik"] in ("MOB", "TEL"):
            proxy.add("phone", contact["sisu"])

    context.emit(proxy)


def parse_officer(context: Zavod, row: dict):
    company = make_proxy(context, row)
    context.emit(company)

    for data in get_value(row, ("kaardile_kantud_isikud", "kaardivalised_isikud")):
        officer = make_officer(context, data, company.id)
        rel_type = data.pop("isiku_roll")
        role = data.pop("isiku_roll_tekstina")
        if rel_type == "O":
            rel = make_rel(context, company, officer, "Ownership", data, role)
            rel.add("percentage", data.pop("osaluse_protsent"))
            rel.add("sharesValue", data.pop("osaluse_suurus"))
            rel.add("sharesCurrency", data.pop("osaluse_valuuta"))
        else:
            rel = make_rel(context, company, officer, "Directorship", data, role)
        context.emit(officer)
        context.emit(rel)


def parse_bfo(context: Zavod, row: dict):
    company = make_proxy(context, row)
    context.emit(company)

    for data in row.pop("kasusaajad"):
        officer = make_officer(context, data, company.id)
        rel = make_rel(
            context,
            company,
            officer,
            "Ownership",
            data,
            data.pop("kontrolli_teostamise_viis_tekstina"),
        )
        context.emit(officer)
        context.emit(rel)


def parse_json(context: Zavod, source: str, handler: Callable):
    data_path = context.get_resource_path(source)
    ix = 0
    with open(data_path, "r") as f:
        items = ijson.items(f, "item")
        for ix, item in enumerate(items):
            handler(context, item)
            if ix and ix % 10_000 == 0:
                context.log.info("Parse ijson item %d ..." % ix)
    if ix:
        context.log.info("Parsed %d ijson items." % (ix + 1), fp=data_path.name)


def parse(context: Zavod):
    # general data
    parse_json(context, SOURCES["general"], parse_general)

    # officers
    parse_json(context, SOURCES["officers1"], parse_officer)
    parse_json(context, SOURCES["officers2"], parse_officer)

    # bfo data
    parse_json(context, SOURCES["bfo"], parse_bfo)


if __name__ == "__main__":
    with init_context("metadata.yml") as context:
        context.export_metadata("export/index.json")
        parse(context)
