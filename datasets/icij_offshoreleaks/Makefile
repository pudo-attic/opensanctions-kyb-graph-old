all: clean process publish

data/full-oldb.zip:
	mkdir -p data/
	wget -q -c -O data/full-oldb.zip https://offshoreleaks-data.icij.org/offshoreleaks/csv/full-oldb.20220708.zip

data/fragments.json: data/full-oldb.zip parse.py
	python parse.py data/full-oldb.zip

data/sorted.json: data/fragments.json
	sort -o data/sorted.json data/fragments.json

data/export/entities.ftm.json: data/sorted.json
	mkdir -p data/export
	nk sorted-aggregate -i data/sorted.json -o data/export/entities.ftm.json

publish:
	bash ../../upload.sh icij_offshoreleaks data/export

process: data/export/entities.ftm.json

clean:
	rm -rf data/
