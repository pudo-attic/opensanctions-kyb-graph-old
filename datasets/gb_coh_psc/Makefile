all: clean process publish

data/fragments.json: parse.py
	python parse.py

data/combined.json: data/fragments.json
	sort -o data/combined.json data/fragments.json
          
data/export/entities.ftm.json: data/combined.json
	mkdir -p data/export
	nk sorted-aggregate -o data/export/psc.json -i data/combined.json

publish:
	aws s3 sync --no-progress --cache-control "public, max-age=64600" --metadata-directive REPLACE --acl public-read data/export s3://data.opensanctions.org/contrib/gb_coh_psc

process: data/export/entities.ftm.json

clean:
	rm -rf data/