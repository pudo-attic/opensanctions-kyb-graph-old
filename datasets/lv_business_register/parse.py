import csv

from zavod import Zavod, init_context

TYPES = {
    "FOREIGN_ENTITY": "LegalEntity",
    "LEGAL_ENTITY": "LegalEntity",
    "NATURAL_PERSON": "Person",
    "OWNER": "Ownership",
    "CO_OWNER": "Ownership",
}


def company_id(reg_nr):
    return f"oc-companies-lv-{reg_nr}".lower()


def oc_url(reg_nr):
    return f"https://opencorporates.com/companies/lv/{reg_nr}"


def make_bank_account(context: Zavod, row: dict):
    account = context.make("BankAccount")
    account.id = context.make_slug("iban", row["sepa"])
    account.add("iban", row["sepa"])
    return account


def parse_register(context: Zavod, row: dict):
    company = context.make("Company")
    company.id = company_id(row["regcode"])
    company.add("name", row["name"])
    company.add("legalForm", row["type_text"])
    company.add("incorporationDate", row["registered"])
    company.add("address", row["address"])
    company.add("opencorporatesUrl", oc_url(row["regcode"]))

    if row["terminated"]:
        company.add("dissolutionDate", row["terminated"])
        company.add("status", row["closed"])

    if row["sepa"]:
        bankAccount = make_bank_account(context, row)
        ownership = context.make("Ownership")
        ownership.id = context.make_slug(
            "bankaccountholder", company.id, bankAccount.id
        )
        ownership.add("owner", company)
        ownership.add("asset", bankAccount)
        context.emit(bankAccount)
        context.emit(ownership)

    context.emit(company)


def parse_old_names(context: Zavod, row: dict):
    company = context.make("Company")
    company.id = company_id(row["regcode"])
    company.add("previousName", row["name"])
    context.emit(company)


def make_officer(context: Zavod, row: dict):
    officer_type = TYPES.get(row.get("entity_type"), "Person")
    is_person = officer_type == "Person"
    officer = context.make(officer_type)
    if is_person:
        ident = row["latvian_identity_number_masked"]
        officer.add("birthDate", row["birth_date"])
        if "forename" in row and "surname" in row:
            first_name, last_name = row["forename"], row["surname"]
            officer.add("firstName", first_name)
            officer.add("lastName", last_name)
            officer.add("name", " ".join((first_name, last_name)))
        elif "name" in row:
            officer.add("name", row["name"])

        if ident:
            officer.add("idNumber", ident)
            officer.id = context.make_slug("officer", ident, officer.caption)
        else:
            officer.id = context.make_slug("officer", row["id"], officer.caption)
    else:
        officer.id = company_id(row["legal_entity_registration_number"])
        officer.add("name", row["name"])
    return officer


def parse_officers(context: Zavod, row: dict):
    rel_type = TYPES.get(row["position"], "Directorship")
    is_ownership = rel_type == "Ownership"
    officer = make_officer(context, row)
    context.emit(officer)

    cid = company_id(row["at_legal_entity_registration_number"])
    rel = context.make(rel_type)
    rel.id = context.make_slug(rel_type, officer.id, cid)
    rel.add("role", row["position"])
    rel.add("role", row["governing_body"])
    rel.add("startDate", row["registered_on"])
    if is_ownership:
        rel.add("owner", officer)
        rel.add("asset", cid)
    else:
        rel.add("director", officer)
        rel.add("organization", cid)
    context.emit(rel)


def parse_beneficial_owners(context: Zavod, row: dict):
    officer = make_officer(context, row)
    officer.add("nationality", row["nationality"])
    officer.add("country", row["residence"])
    cid = company_id(row["legal_entity_registration_number"])
    rel = context.make("Ownership")
    rel.id = context.make_slug("OWNER", officer.id, cid)
    rel.add("role", "OWNER")
    rel.add("startDate", row["registered_on"])
    rel.add("owner", officer)
    rel.add("asset", cid)
    context.emit(officer)
    context.emit(rel)


def parse_members(context: Zavod, row: dict):
    cid = company_id(row["at_legal_entity_registration_number"])
    rel = context.make("Ownership")
    rel.add("role", "OWNER")
    rel.add("asset", cid)
    rel.add("sharesCount", row["number_of_shares"])
    rel.add("sharesValue", row["share_nominal_value"])
    rel.add("sharesCurrency", row["share_currency"])
    rel.add("startDate", row["date_from"])
    if row["entity_type"] == "JOINT_OWNERS":
        # owners will be added by `parse_joint_members` based on relation id:
        rel.id = context.make_slug("OWNER", row["id"])
    else:
        officer = make_officer(context, row)
        rel.add("owner", officer)
        rel.id = context.make_slug("OWNER", officer.id, cid)
        context.emit(officer)
    context.emit(rel)


def parse_joint_members(context: Zavod, row: dict):
    officer = make_officer(context, row)
    rel = context.make("Ownership")
    rel.id = context.make_slug("OWNER", row["member_id"])
    rel.add("owner", officer)
    context.emit(officer)
    context.emit(rel)


def parse_csv(context: Zavod, data_path: str, parser):
    with open(data_path) as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            parser(context, row)


def parse(context: Zavod):
    data_path = context.get_resource_path("src/register.csv")
    parse_csv(context, data_path, parse_register)
    data_path = context.get_resource_path("src/register_name_history.csv")
    parse_csv(context, data_path, parse_old_names)
    data_path = context.get_resource_path("src/beneficial_owners.csv")
    parse_csv(context, data_path, parse_beneficial_owners)
    data_path = context.get_resource_path("src/members.csv")
    parse_csv(context, data_path, parse_members)
    data_path = context.get_resource_path("src/members_joint_owners.csv")
    parse_csv(context, data_path, parse_joint_members)


if __name__ == "__main__":
    with init_context("metadata.yml") as context:
        context.export_metadata("export/index.json")
        parse(context)
