# corpwatch

http://api.corpwatch.org/documentation/#data

## data quality / depth

No directors, only companies and their (international) subsidiaries / parents.

Parent companies (as parsed from the Exhibit 21 of the 10-K where possible) are
mapped via the `parent` property, no extra `Ownership` interval is created,
unless there is detailed share information (percentage and/or date).

*From Corpwatch source data README:*

NOTE: Some groups of companies may form circular loops due to peculiarities of
their filings (Same companies listed as parents and children), or because
multiple CIKs refer to the same filing.
