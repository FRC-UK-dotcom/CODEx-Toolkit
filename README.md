# FRC Parse and Load Tools

This repository contains Python scripts for parsing UK ixbrl files and loading
them into sql database. The scripts use the Arelle parser to breakdown the iXBRL.

## Dependencies

The tools depend on Arelle, and the Python `lxml` module.  `lxml` can
be installed with:

```
pip3 install lxml
```

Arelle can be obtained from [GitHub](https://github.com/Arelle/Arelle).

The code will look for the Arelle repository in a sibiling directory of this
repository.  For example, if both this repository and the Arelle repository are
cloned into a `work` directory:

```
  work
    + frc-parse-load
    + Arelle
```

Alternatively, the location of the Arelle repository can be set using the
`ARELLE` environment variable.

```
frc-load.py --cache-dir FCA cache out-dir
```

## Running the parser

The parser can be run in one of two modes. Either stripping data into csv files for
immediate analysis (and/or later database load) or directly into a SQL Server database.

Database credentials need to be specified in the config.json file.

It can be run from the command line with various flag settings. The scripts have also
been organised to make it posible to interact with components of the API.

A minimum of three flags need to be set for it to parse filings:

```
frc-load.py --cache-dir cache out-dir index
```

"cache" is the name of a directory where files download from filings.xbrl.org will be stored.

The "out-dir" is a directory where output files will be created.

The final compulsory argument is the index the parser will use to find iXBRL filings


## The filings repositories

iXBRL files can be downloaded from one of three repositories:

1. FCA: FCA
2. CH: Companies House
3. FO: Filings.org

```
frc-load.py --cache-dir cache out-dir FCA
```

The Companies House API requires a key. You need to register at <https://developer.company-information.service.gov.uk>

The parser works off a list of filing identifiers (currently LEI or Company Registration Number)

These can be specified in one of three ways:

1. comma separated list in the command line --LEI 123456,78901234
2. csv file --List alist.csv (future version!)
3. sql server table and column --list Table.Column



## Outputs

The script can create a set of core pre-defined csv files matchiing the Tables that can alternatively be
updated automatically from these scripts:

1. Filings
2. FilingsExtraDetail
3. Facts
4. Dims
5. Concepts
6. Anchors
7. Namespaces

The columns required for each table are defined in the dbtables directory.

The db flag must be set for tables to be updated from the parser

```
frc-load.py --cache-dir --list Table.Column --db cache out-dir FCA
```

## Force

If the script is modified to report new data points, re-processing can be
forced by specifying the `-f` option, or by deleting the contents of the output
directory.

## Filtering

The script has various options for filtering which filings are processed:

* `-l LEI` will limit to the specified LEI.  The option can be repeated to
  specify multiple LEIs.
* `--country COUNTRY` will limit to the specified country code.  The option can be repeated to specify multiple countries.
* `--from YYYY-MM-DD` will limit to filings with an end date of or after the specified date.
* `--limit N` will limit the script to processing at most N filings for each Filer ie. per LEI 



