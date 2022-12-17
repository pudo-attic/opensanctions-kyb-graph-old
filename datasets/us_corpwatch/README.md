# corpwatch

http://api.corpwatch.org/documentation/#data

## data quality / depth

No directors, only companies and their (international) subsidiaries / parents.

Parent companies (as parsed from the Exhibit 21 of the 10-K where possible) are
mapped via the `parent` property, no extra `Ownership` interval is created,
unless there is detailed share information (percentage and/or date).
