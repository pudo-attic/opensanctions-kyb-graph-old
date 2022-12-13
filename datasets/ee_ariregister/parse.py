"""
columns:

  1: nimi
  2: ariregistri_kood
  3: ettevotja_oiguslik_vorm
  4: ettevotja_oigusliku_vormi_alaliik
  5: kmkr_nr
  6: ettevotja_staatus
  7: ettevotja_staatus_tekstina
  8: ettevotja_esmakande_kpv
  9: ettevotja_aadress
 10: asukoht_ettevotja_aadressis
 11: asukoha_ehak_kood
 12: asukoha_ehak_tekstina
 13: indeks_ettevotja_aadressis
 14: ads_adr_id
 15: ads_ads_oid
 16: ads_normaliseeritud_taisaadress
 17: teabesysteemi_link

translated:

  1: name
  2: registration_code
  3: holder_legal_form
  4: type_of_business_form_type
  5: kmkr_nr
  6: holder_status
  7: holder_status_as_Text
  8: date of first establishment
  9: holder_address
 10: location_at_address_of_business_holder
 11: location_address_code
 12: place_of_establishment_as_text
 13: index_at_address_of_holder
 14: ads_adr_id
 15: ads_ads_oid
 16: ads_normalised_address
 17: information_system_link

"""

import csv
from datetime import datetime
from io import TextIOWrapper
from typing import Optional
from zipfile import ZipFile

from zavod import Zavod, init_context

URL = "https://avaandmed.rik.ee/andmed/ARIREGISTER/ariregister_csv.zip"
TYPES = {
    "Füüsilisest isikust ettevõtja": "Person",  # Self-employed person
    "Kohaliku omavalitsuse asutus": "PublicBody",  # Local government body
    "Mittetulundusühing": "Organization",  # Non-profit association
    "Täidesaatva riigivõimu asutus või riigi muu institutsioon": "PublicBody",  # Executive or other public institution
    "Sihtasutus": "Organization",  # Foundation
}


def iter_rows(zip: ZipFile, name: str):
    with zip.open(name, "r") as fh:
        wrapper = TextIOWrapper(fh, encoding="utf-8-sig")
        for row in csv.DictReader(wrapper, delimiter=";"):
            yield row


def parse_date(value: str) -> Optional[str]:
    try:
        return datetime.strptime(value, "%d.%m.%Y").date().isoformat()
    except ValueError:
        return None


def proxy_id(context: Zavod, row: dict) -> Optional[str]:
    if row["kmkr_nr"]:
        return context.make_slug("vat", row["kmkr_nr"])
    if row["ariregistri_kood"]:
        return context.make_slug(row["ariregistri_kood"])
    context.log.warn("No id for proxy")


def parse_row(context: Zavod, row: dict):
    proxy = context.make(TYPES.get(row["ettevotja_oiguslik_vorm"], "Company"))
    proxy.id = proxy_id(context, row)
    proxy.add("name", row["nimi"])
    proxy.add("legalForm", row["ettevotja_oiguslik_vorm"])
    proxy.add("incorporationDate", parse_date(row["ettevotja_esmakande_kpv"]))
    proxy.add("address", row["ads_normaliseeritud_taisaadress"])
    proxy.add("sourceUrl", row["teabesysteemi_link"])
    proxy.add("status", row["ettevotja_staatus_tekstina"])
    proxy.add("jurisdiction", "ee")
    context.emit(proxy)


def parse(context: Zavod):
    data_path = context.fetch_resource("data.zip", URL)
    with ZipFile(data_path, "r") as zip:
        for name in zip.namelist():
            if name.startswith("ettevotja_rekvisiidid"):
                rows = iter_rows(zip, name)
                for row in rows:
                    parse_row(context, row)


if __name__ == "__main__":
    with init_context("metadata.yml") as context:
        context.export_metadata("export/index.json")
        parse(context)
