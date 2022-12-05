import countrynames
from zavod import Zavod


def make_oc_company_id(context: Zavod, jurisdiction: str, company_nr: str) -> str:
    """
    generate company id in open corporates format
    """
    # ensure country
    cc = countrynames.to_code(jurisdiction)
    nr = company_nr.lower()
    if cc is None:
        context.log.warn("Not a valid jurisdiction", jurisdiction=jurisdiction)
        return f"oc-companies-{nr}"  # FIXME
    cc = cc.lower()
    return f"oc-companies-{cc}-{nr}"
