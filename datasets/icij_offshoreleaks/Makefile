
data/full-oldb.zip:
	mkdir -p data/
	wget -q -c -O data/full-oldb.zip https://offshoreleaks-data.icij.org/offshoreleaks/csv/full-oldb.20220708.zip

data/fragments.json: data/full-oldb.zip
	python parse.py data/full-oldb.zip

data/sorted.json: data/fragments.json
	sort -o data/sorted.json data/fragments.json

data/export/entities.ftm.json: data/sorted.json
	mkdir -p data/export
	ftm sorted-aggregate -i data/sorted.json -o data/export/entities.ftm.json

publish:
	aws s3 sync --no-progress --cache-control "public, max-age=64600" --metadata-directive REPLACE --acl public-read data/export s3://data.opensanctions.org/contrib/icij_offshoreleaks

process: data/export/entities.ftm.json

clean:
	rm -rf data/