import json
import requests
from datetime import datetime
from nomenklatura.dataset import DataCatalog
from nomenklatura.util import PathLike, datetime_iso
from zavod.dataset import ZavodDataset


def build_catalog(catalog_in: PathLike):
    catalog = DataCatalog(ZavodDataset, {})
    catalog.updated_at = datetime_iso(datetime.utcnow())
    with open(catalog_in, "r") as fh:
        while url := fh.readline():
            try:
                resp = requests.get(url)
                data = resp.json()
                catalog.make_dataset(data)
            except Exception as exc:
                print("ERROR [%s]: %s" % (url, exc))

    with open("catalog.json", "w") as fh:
        json.dump(catalog.to_dict(), fh)


if __name__ == "__main__":
    build_catalog("catalog.txt")
