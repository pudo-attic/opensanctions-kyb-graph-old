all: clean process publish

data/src:
	mkdir -p data/src
	wget --no-check-certificate -i ./sources.txt -P data/src

data/fragments.json: data/src
	python parse.py

data/sorted.json: data/fragments.json
	sort -o data/sorted.json data/fragments.json

data/export/entities.ftm.json: data/sorted.json
	mkdir -p data/export
	nk sorted-aggregate -i data/sorted.json -o data/export/entities.ftm.json

publish:
	bash ../../upload.sh lv_business_register data/export

process: data/export/entities.ftm.json

clean:
	rm -rf data/
