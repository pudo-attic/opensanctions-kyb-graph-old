all: clean process publish

data/src:
	mkdir -p data/src
	wget -q -O data/src/corpwatch.tar.gz https://archive.org/download/corpwatch_api_data_dumps/corpwatch_api_tables_csv.tar.gz
	tar -C data/src/ -xvf data/src/corpwatch.tar.gz
	rm data/src/corpwatch.tar.gz

data/fragments.json: data/src
	python parse.py

data/sorted.json: data/fragments.json
	sort -o data/sorted.json data/fragments.json

data/export/entities.ftm.json: data/sorted.json
	mkdir -p data/export
	nk sorted-aggregate -i data/sorted.json -o data/export/entities.ftm.json

publish:
	bash ../../upload.sh us_corpwatch data/export

process: data/export/entities.ftm.json

clean:
	rm -rf data/
