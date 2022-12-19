all: clean process

data/source.json:
	mkdir -p data
	curl -o data/source.zip https://s3.eu-west-1.amazonaws.com/oo-bodsdata/data/denmark/json.zip
	unzip -j data/source.zip denmark.json -d data/
	mv data/denmark.json data/source.json

data/fragments.json: data/source.json
	python parse.py

data/sorted.json: data/fragments.json
	sort -o data/sorted.json data/fragments.json

data/export/entities.ftm.json: data/sorted.json
	mkdir -p data/export
	nk sorted-aggregate -i data/sorted.json -o data/export/entities.ftm.json

publish:
	bash ../../upload.sh dk_cvr data/export

process: data/export/entities.ftm.json

clean:
	rm -rf data
