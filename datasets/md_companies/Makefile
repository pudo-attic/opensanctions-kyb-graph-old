all: clean process

data/fragments.json:
	python parse.py

data/sorted.json: data/fragments.json
	sort -o data/sorted.json data/fragments.json

data/export/entities.ftm.json: data/sorted.json
	mkdir -p data/export
	nk sorted-aggregate -i data/sorted.json -o data/export/entities.ftm.json

publish:
	aws s3 sync --no-progress --metadata-directive REPLACE --acl public-read data/export s3://data.opensanctions.org/graph/md_companies

process: data/export/entities.ftm.json

clean:
	rm -rf data
