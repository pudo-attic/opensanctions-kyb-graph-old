all: clean fetch process publish

data/export/entities.ftm.json: data/sorted.json
	mkdir -p data/export
	nk sorted-aggregate -i data/sorted.json -o data/export/entities.ftm.json

data/sorted.json: data/fragments.json
	sort -o data/sorted.json data/fragments.json

data/fragments.json:
	python parse.py

process: data/export/entities.ftm.json

publish:
	aws s3 sync --no-progress --cache-control "public, max-age=64600" --metadata-directive REPLACE --acl public-read data/export s3://data.opensanctions.org/graph/ru_egrul

clean:
	rm -rf data/export data/fragments.json data/sorted.json
