all: clean process publish

data/fragments.json: parse.py
	python parse.py

data/combined.json: data/fragments.json
	sort -u -o data/combined.json data/fragments.json

data/export/entities.ftm.json: data/combined.json
	mkdir -p data/export
	nk sorted-aggregate -o data/export/entities.ftm.json -i data/combined.json

publish:
	bash ../../upload.sh gb_coh_psc data/export

process: data/export/entities.ftm.json

clean:
	rm -rf data/
