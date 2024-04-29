### FRC Parse and Load Tools

This repository contains Python scripts for performing initial analysis of ESEF
filings.  The scripts work on a local copy of the filings.xbrl.org filing
repository and produce a JSON file containing various datapoints for each
filing.  This JSON file is intended to be used as an input for further
analysis.

## Dependencies

The analysis tools depends on Arelle, and the Python `lxml` module.  `lxml` can
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

## The filings repository



## Running the analysis



## From filings.xbrl.org



```
frc-analyse --cache-dir cache  https://filings.xbrl.org/index.json out-dir
```

"cache" is the name of a directory where files download from filings.xbrl.org will be stored.

The last argument is a directory where output files will be created.

## Outputs

The script will create a JSON file for each filing in the index, and when the
run is completed, it will create a combined, summary `all_filings.json` file.

If the script is re-run with the same output directory, any existing per-filing
JSON files will be re-used, rather than re-processing the filing.  This allows
an interrupted process to be resumed, or for the output to be quickly updated
to include any new filings.

If the script is modified to report new data points, re-processing can be
forced by specifying the `-f` option, or by deleting the contents of the output
directory.

## Filtering

The script has various options for filtering which filings are processed:

* `-l LEI` will limit to the specified LEI.  The option can be repeated to
  specify multiple LEIs.
* `--country COUNTRY` will limit to the specified country code.  The option can be repeated to specify multiple countries.
* `--from YYYY-MM-DD` will limit to filings with an end date of or after the specified date.
* `--limit N` will limit the script to processing at most N new filings.  

Note that in all cases, the results will be combined with any filings for which
processed outputs already exist in `out-dir`, regardless of whether they meet
the specified filters.

