general = data/ettevotja_rekvisiidid__yldandmed.ijson
officers1 = data/ettevotja_rekvisiidid__kaardile_kantud_isikud.ijson
officers2 = data/ettevotja_rekvisiidid__kandevalised_isikud.ijson
bfo =  data/ettevotja_rekvisiidid__kasusaajad.ijson
sources = $(general) $(officers1) $(officers2) $(bfo)

all: clean process publish

download: $(sources)

$(sources): data/%.ijson: data/%.zip
	unzip -j data/$*.zip $*.json -d data
	jq -c ".[]" data/$*.json > data/$*.ijson
	rm data/$*.json

data/%.zip:
	mkdir -p data
	curl -o data/$*.zip https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/$*.json.zip

data/fragments.json: download
	python parse.py

data/sorted.json: data/fragments.json
	sort -o data/sorted.json data/fragments.json

data/export/entities.ftm.json: data/sorted.json
	mkdir -p data/export
	nk sorted-aggregate -i data/sorted.json -o data/export/entities.ftm.json

publish:
	aws s3 sync --no-progress --cache-control "public, max-age=64600" --metadata-directive REPLACE --acl public-read data/export s3://data.opensanctions.org/graph/ee_ariregister

process: data/export/entities.ftm.json

clean:
	rm -rf data/