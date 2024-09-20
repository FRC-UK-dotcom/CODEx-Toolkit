# This is a simple command line tool that bundles up the requisite api calls to frc-loader.
# It therefore also shows how they could be called from your own custom module or from a jupyter notebook for testing.
# Takes args which are then passed to a settings function. This function can of course be called as an api call to replicate this.

import sys
import argparse

import frcLoader
from frcLoader import setupLoading, loadFOFilings, loadDb, getLEIsFromList

# -c cachedir --lei 02226110  --limit 3 --from 2022-01-25  CH output
# -c cachedir --lei 2138007UTBN8X9K1A235 --limit 4 --from 2023-03-25 --db https://filings.xbrl.org/api/entities output
# -c cachedir --lei 2138001JXGCFKBXYB828 --list Filers_FTSE.LEGAL_ENTITY_ID  --limit 3 --from 2023-01-25 --db FCA output
# -c cachedir --lei 10681178 --list Filers_CH.entity_CRN --limit 2 --from 2022-01-25 --db CH output
parser = argparse.ArgumentParser(description = "ESEF XBRL Loader")
# Watch out - package-dir translates as an argument as package_dir!! Converts - to _ ?
parser.add_argument('-p', '--package-dir', action="store", help = "Directory containing base taxonomy packages")
parser.add_argument('-f', '--force', action='store_true', help = "Regenerate even if output already exists")
parser.add_argument('-c', '--cache-dir', action='store', help = "Directory to download and cache files to if 'index_file' is a URL")
# Append allows a list to be built up from repeated same argument in the command line. So type is always a list.
parser.add_argument('--lei', action='append', help = "Filter to LEI (can be repeated)")
parser.add_argument('--list', action='store', help = "Either file with csv extension or database table with identifier column as extension if db set eg. Table1.LEI")
parser.add_argument('--limit', type=int, action='store', help = "Process at most this many new filings")
parser.add_argument('--country', action='append', help = "Filter to specified countries (can be repeated)")
parser.add_argument('--from', dest="after", action='store', help = "Filter to filings with filing date on or after specified date (YYYY-MM-DD)")
parser.add_argument('--db', action='store_true', help = "Store to db tables otherwise store to files")
# If no flag then these are compulsory arguments
#parser.add_argument('index_file', action="store", help = "Repository index file")
parser.add_argument('index', action="store", help = "Index")
parser.add_argument('output_dir', action="store", help = "Output directory")
args = parser.parse_args()

params = {}

# Converts args to a dict
for key, value in vars(args).items():
    params[key] = value
    #print(key, value)
# If you want to load args outside command line eg. in a notebook, then load up psuedo args as params in setup cell then pass them as below:

params['index'] = params['index'].lower()

if setupLoading(params):
    import svrOps
    # qry = "SELECT ftse.LEGAL_ENTITY_ID FROM [dbo].[Filings] as fgs RIGHT JOIN Filers_FTSE as ftse ON fgs.LEI = ftse.LEGAL_ENTITY_ID GROUP by ftse.LEGAL_ENTITY_ID,ftse.NAME HAVING Max(Filing_EndDate) is NULL"
    # LEIs = svrOps.getTable(qry, top=20, start = 1)
    if params['list'] is not None:
        LEISource = params['list'].split('.')
        if LEISource[1].lower() == 'csv':
            print('csv file source - not implemented')
        else:
            LEIs = getLEIsFromList(LEISource[1], dbTbl=LEISource[0], top=3, start = 58)
            # LEIs = getLEIsFromList('LEGAL_ENTITY_ID', dbTbl='Filers_FTSE', top=30, start = 79)
            # LEIs = getLEIsFromList('entity_CRN', dbTbl='Filers_CH', top=5, start = 1)
    else:
        LEIs = [params['lei']]
    for lei in LEIs:
        if params['index'] == 'fo':
            filerData = loadFOFilings(lei) # Possibly multiple filings if limit is set > 1 Change to use arg version of lei if running with args.
        elif params['index'] == 'fca':
            filerData = frcLoader.loadFCAFilings(lei)
        elif params['index'] == 'ch':
            filerData = frcLoader.loadCHFilings(lei)
        if params['db'] and len(filerData) != 0:
            # Could dump fileData to file and/or instead.
            loadDb(filerData)