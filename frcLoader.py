#!/usr/bin/env python3

import json
import sys
import os
import re
from collections import defaultdict
from urllib.parse import urlparse
from pathlib import Path
import requests
import tempfile
import datetime
import numbers


sys.path.append(os.environ.get("ARELLE", "../Arelle"))

import xbrlanalyse
from xbrlanalyse import url_path_join, load_json, local_file

# Added JT - 22/03/24
import svrOps

WIDER_NARROWER_ARCROLE = 'http://www.esma.europa.eu/xbrl/esef/arcrole/wider-narrower'
DOM_MEM_ARCROLE = 'http://xbrl.org/int/dim/arcrole/domain-member'
DIM_DOM_ARCROLE = 'http://xbrl.org/int/dim/arcrole/dimension-domain'
HC_DIM_ARCROLE = 'http://xbrl.org/int/dim/arcrole/hypercube-dimension'
PARENT_CHILD_ARCROLE = 'http://www.xbrl.org/2003/arcrole/parent-child'
DIMENSION_ARCROLES = (DOM_MEM_ARCROLE, DIM_DOM_ARCROLE, HC_DIM_ARCROLE)
BASE_TAXONOMY_HOSTS = { 'xbrl.ifrs.org', 'www.esma.europa.eu', 'xbrl.frc.org.uk' }

APIS = {"fca":'https://api.data.fca.org.uk/search?index=fca-nsm-searchdata', "fo":'https://filings.xbrl.org/api/entities', "ch":'https://api.companieshouse.gov.uk/company/'}

MANDATORY_CONCEPT_LOCALNAMES = {
    "AddressOfRegisteredOfficeOfEntity",
    "CountryOfIncorporation",
    "DescriptionOfNatureOfEntitysOperationsAndPrincipalActivities",
    "DomicileOfEntity",
    "ExplanationOfChangeInNameOfReportingEntityOrOtherMeansOfIdentificationFromEndOfPrecedingReportingPeriod",
    "LegalFormOfEntity",
    "NameOfParentEntity",
    "NameOfReportingEntityOrOtherMeansOfIdentification",
    "NameOfUltimateParentOfGroup",
    "PrincipalPlaceOfBusiness",
    }

KEY_VALUES = {
    "revenue": (
        "InterestRevenueExpense", 
        "Revenue",
        "RevenueFromContractsWithCustomers",
        "RevenueFromInsuranceContractsIssuedWithoutReductionForReinsuranceHeld",
        "RevenueFromInterest",
    ),
    "profit": (
        "ProfitLoss",
    ),
    "country": (
        "CountryOfIncorporation",
    ),
    "name": (
        "NameOfReportingEntityOrOtherMeansOfIdentification",
    ),
    "goodwill": (
        "Goodwill",
    ),
    "intangibleassetsandgoodwill": (
        "IntangibleAssetsAndGoodwill",
    ),
}

# Global Variables used in loading db - Added JT - 22/03/24
locs = {} # File/directory locations for processing.
secrets = {}
register = None
#filing should be unique descriptor of the filing eg. FilerID, Report End, Version Number, Country if applicable. May or may not correspond the name of package stored in register.
filing = None
filingID = None
filingLEI = None
filingEndDate = None

namespaceURI = None
stdTaxyDate = None
# Dict to allow filing context ids to be standardised.
ctxtMap = {}

# Allow access to core elements eg. current arelle model from outside loader.
xbrl = None
params = {}
arelle = None

def getXBRL():
    return arelle

def startArelle(pDir):
    global arelle
    arelle = xbrlanalyse.ArelleLoader()

    package_dir = pDir if pDir is not None else os.path.join(os.path.dirname(__file__), "packages")
    print("Loading packages from %s" % package_dir)
    arelle.loadPackagesFromDir(package_dir)

def setupLoading(p):
    global params
    global arelle
    global register
    global locs
    global secrets

    params = p
    secrets = load_json('config.json')

    # Neither essential params are present:
    if params['lei'] is None and params['list'] is None:
        print('No identifier(s) provided')
        return False
    
    # Check that both params are present:
    if params['output_dir'] is None and params['db'] is None:
        print('No output provided')
        return False
    
    arelle = xbrlanalyse.ArelleLoader()
    package_dir = params['package_dir'] if params['package_dir'] is not None else os.path.join(os.path.dirname(__file__), "packages")
    print("Loading packages from %s" % package_dir)
    arelle.loadPackagesFromDir(package_dir)

    #Could allow this to work with db index table (as per xbrlxl.com) in future or even file
    if params['index'] == 'fca':
        register = 'ESEF_FCA' # Will work it out from URL sent eventually. eg. Index file
        locs['index_file'] = APIS[params['index']]
        locs['base_uri'] = locs['index_file'].rsplit('/',1)[0]
        locs['base_filings_uri'] = 'https://data.fca.org.uk/artefacts/NSM/Portal'
    elif params['index'] == 'fo':
        register = 'ESEF_FO'
        locs['index_file'] = APIS[params['index']]
        locs['base_uri'] = locs['index_file'].rsplit('/',1)[0]
        locs['base_filings_uri'] = os.path.dirname(locs['base_uri'])
    elif params['index'] == 'ch':
        register = 'CH'
        locs['index_file'] = APIS[params['index']]
        locs['base_uri'] = locs['index_file'].rsplit('/',1)[0]
        locs['base_filings_uri'] = "https://find-and-update.company-information.service.gov.uk/company"
    else:
        print('Unrecognised Index')
        return False

    return True

def getFCAFilingMetaData():
    from requests.structures import CaseInsensitiveDict

    # Possibly overkill - used as header types are case sensitive in literal http requests so prevents errors from wrong case.
    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"

    queryDict = {"from":0,"size":250,"sort":"publication_date","keyword":None,"sortorder":"desc","criteriaObj":{"criteria":[{"name": "lei", "value": [None]},{"name":"tag_esef","value":["Tagged"]}],"dateCriteria":[{"name":"publication_date","value":{"from":None,"to":"2022-04-08T11:56:06Z"}}]}}
    queryDict["criteriaObj"]["dateCriteria"][0]["value"]["to"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    queryDict["criteriaObj"]["criteria"][0]["value"] = str(filingLEI)
    
    data = str(queryDict).replace("None", "null")

    resp = requests.post(APIS['fca'], headers=headers, data=data)

    #print(resp.status_code) # The HTTP 200 OK success status response code indicates that the request has succeeded.

    json = resp.json() 

    filings_info = []
    for c,v in enumerate(json['hits']['hits'], 1):
        meta_info = {}
        meta_info["disclosure_level"] = None
        meta_info["name"] = v["_source"]["company"]
        meta_info["LEI"] = v["_source"]["lei"]
        meta_info["period_end"] = v["_source"]["document_date"]
        meta_info["publication_date"] = v["_source"]["publication_date"]
        meta_info["submitted_date"] = v["_source"]["submitted_date"]
        unique_package_url_parts = v["_source"]["download_link"].split('/')[2:]
        meta_info["package_url"] = f'/{"/".join(unique_package_url_parts)}'
        # meta_info["package_url"] = f'{locs["base_filings_uri"]}{v["_source"]["download_link"]}'
        # meta_info["report_url"] = f'{locs["base_filings_uri"]}{v["_source"]["html_link"]}'
        unique_report_url_parts = v["_source"]["html_link"].split('/')[2:]
        meta_info["report_url"] = f'/{"/".join(unique_report_url_parts)}'
        filings_info.append(meta_info)

    return filings_info

def getCHFilingMetaData():
    from requests.auth import HTTPBasicAuth
    headers = {}
    headers["Accept"] = "application/json"
    url = f"{APIS['ch']}{filingLEI}/filing-history?category=accounts"
    APIKey = secrets.get('api_key_ch')
    # No password required so set to empty string. APIKey is basically the username.
    # Deals with Base64 conversion issue.
    auth = HTTPBasicAuth(APIKey,'')
    resp = requests.get(url, headers=headers, auth=auth)
    # Surprising - no json property available despite requesting json as content and accepting only json, just text and text being json.
    # So have to do this.
    history = json.loads(resp.text)

    filings_info = []
    for item in history["items"]:
        meta_info = {}
        meta_info["disclosure_level"] = item.get("description")
        meta_info["period_end"] = item.get("description_values").get("made_up_date")
        meta_info["publication_date"] = None
        meta_info["submitted_date"] = item.get("date")
        meta_info["transaction_id"] = item.get("transaction_id")

        if meta_info["period_end"] is None:
            continue # Will be missing if not accounts filed but other 'account' filings such as change of year end.
        # \\08008979\\2023-12-18\\08008979_2023-12-18.xhtml
        storage_parts = (filingLEI,meta_info["period_end"])
        # Using this to store what would be the unpacked path (if it were ever packaged up!) which is where we will store the instance.
        meta_info["package_url"] = "\\" + "\\".join(storage_parts) + "\\" + "_".join(storage_parts) + ".xhtml"
        report_url_parts = (filingLEI, "filing-history", meta_info["transaction_id"], "document")
        # '/08008979/filing-history/MzI1MjUzMTIzNGFkaXF6a2N4/document?format=xhtml&download=1'
        meta_info["report_url"] = "/" + "/".join(report_url_parts) + "?format=xhtml&download=1"
        
        filings_info.append(meta_info)
    return filings_info
    
# Maybe overkill - packages up meta_info on a consistent basis with other registries
def getFOFilingMetaData(lei):
    filings_info = []

    
    index = load_json(locs['index_file'] + '/' + lei + '/filings' )

    if index is None:
        print("Processing Aborted %s - Filer Index missing" % lei)
        return []
     
    # Iterates through each filing in json array found at index/lei/filings url
    # Building a generic list of filing info dicts for each filing replicable across different sources.
    for filing_meta in index.get('data'): 
        filing_info = filing_meta.get('attributes') # 'attributes' sort of equivalent to old metadata.json file.
        # Just shuffling these dates about so better named and consistent with FCA.
        # They are of course very approximately similar!
        filing_info["disclosure_level"] = None
        filing_info['publication_date'] = filing_info['date_added']
        filing_info['submission_date'] = filing_info['processed']
        filings_info.append(filing_info) 
    return filings_info

def getLEIsFromList(field, fName = None, dbTbl = None, top = 0, start = 0):
    if dbTbl is not None:
        return svrOps.getFieldFromTable(secrets, field, dbTbl, top, start)
    
#
# Find the ultimate parent-child parents of the given concept across all ELRs
#
def get_presentation_roots(xbrl, concept):
    roots = []
    c = concept
    for baseSetKey, baseSetModelLinks  in xbrl.baseSets.items():
        arcrole, ELR, linkqname, arcqname = baseSetKey
        if arcrole == PARENT_CHILD_ARCROLE and ELR is not None and linkqname is not None and arcqname is not None:
            relSet = xbrl.relationshipSet(arcrole, ELR)
            while True:
                inbound = relSet.toModelObject(c) 
                if len(inbound) == 0:
                    if c != concept and c not in roots:
                        roots.append(c)
                    break
                c = inbound[0].fromModelObject
    return roots

#
# Find all wider-narrower relationships, and return a list of dicts, each
# containing "wider" and "narrower" keys with Clark-notation QName values
#
def get_anchoring_relationships(xbrl):
    anchors = set()
    for baseSetKey, baseSetModelLinks  in xbrl.baseSets.items():
        arcrole, ELR, linkqname, arcqname = baseSetKey
        if arcrole in (WIDER_NARROWER_ARCROLE, ) and ELR is not None:
            relSet = xbrl.relationshipSet(arcrole, ELR)
            for r in relSet.modelRelationships:
                if r.fromModelObject is not None and r.toModelObject is not None:
                    roots = tuple( root.qname.clarkNotation for root in get_presentation_roots(xbrl, r.toModelObject if not is_base_qname(r.toModelObject.qname) else r.fromModelObject) )
                    anchors.add((r.fromModelObject.qname.clarkNotation, r.toModelObject.qname.clarkNotation, roots))

    # Convert sets to list to make them JSON serialisable
    return list({ 'wider': w, 'narrower': n, 'roots': r } for (w, n, r) in anchors)

#
# Get details of any extension concepts that are "anchored" using dimensional
# relationships.
#
# This means:
#    domain-member where the target is an extension and the base is not.
#    dimension-domain where the target is an extension 
#    hypercube-dimension where the target is an extension 
#
def get_dimension_anchors(xbrl):
    extensionMembers = set()
    extensionDomains = set()
    extensionDimensions = set()
    for baseSetKey, baseSetModelLinks  in xbrl.baseSets.items():
        arcrole, ELR, linkqname, arcqname = baseSetKey
        domainMemberRels = xbrl.relationshipSet(DOM_MEM_ARCROLE, ELR)
        dimensionDomainRels = xbrl.relationshipSet(DIM_DOM_ARCROLE, ELR)
        hypercubeDimensionRels = xbrl.relationshipSet(HC_DIM_ARCROLE, ELR)
        for r in domainMemberRels.modelRelationships:
            f = r.fromModelObject
            t = r.toModelObject

            if is_base_qname(f.qname) and not is_base_qname(t.qname):
                node = f
                # Traverse to top of domain-member tree
                while True:
                    parentRels = domainMemberRels.toModelObject(node)
                    if len(parentRels) != 1:
                        break 
                    node = parentRels[0].fromModelObject

                # Now find the dimension(s) using this domain
                parentRels = dimensionDomainRels.toModelObject(node)
                if len(parentRels) >= 1:
                    # Record (dimension, member, member's parent)
                    for p in parentRels:
                        extensionMembers.add((p.fromModelObject.qname.clarkNotation, t.qname.clarkNotation, f.qname.clarkNotation))

        for r in dimensionDomainRels.modelRelationships:
            f = r.fromModelObject
            t = r.toModelObject

            if not is_base_qname(t.qname):
                extensionDomains.add((f.qname.clarkNotation, t.qname.clarkNotation))

        for r in hypercubeDimensionRels.modelRelationships:
            f = r.fromModelObject
            t = r.toModelObject

            if not is_base_qname(t.qname):
                extensionDimensions.add((f.qname.clarkNotation, t.qname.clarkNotation))

    return {
            "members": list({ 'dimension': d, 'member': m, 'parent': p } for (d, m, p) in extensionMembers),
            "domains": list({"dimension": dim, "domain": dom} for (dim, dom) in extensionDomains),
            "dimensions": list({"hypercube": h, "dimension": d} for (h, d) in extensionDimensions)
            }

#
# Return true if the supplied QName is in a base taxonomy namespace
#
def is_base_qname(qname):
    hostname = urlparse(qname.namespaceURI).hostname
    return hostname in BASE_TAXONOMY_HOSTS


def extract_facts(xbrl):
    fact_list = [[]]
    for fact in xbrl.facts:
        currency = None
        decimal = None
        numVal = None
        txtVal = None
        if fact.concept.isNumeric:
            numVal = fact.textValue
            if fact.concept.isMonetary:
                currency = fact.unit.measures[0][0].localName
            # print(fact.qname.localName,type(numVal))
            #if isinstance(numVal,float):
            #    numVal = str(numVal)
        else:
            if fact.textValue is not None:
                if len(fact.textValue) <= 2000:
                    txtVal = fact.textValue
                else:
                    txtVal = '_tooBIG'
        # Catches 'INF' as decimals
        decimals = fact.decimals if fact.decimals is not None and fact.decimals[-1].isnumeric() else None   
        endDate = None if fact.context.endDate == None else fact.context.endDate.strftime('%Y-%m-%d')
        startDate = None if fact.context.startDatetime == None else fact.context.startDatetime.strftime('%Y-%m-%d')
        # print(fact.qname.prefix, fact.qname.localName, fact.contextID, fact.textValue, fact.decimals, currency, fact.context.endDate, fact.context.startDatetime)          
        # Use dict.get so can return '' for entity contexts. Cam't use None (null) as part of Primary Key.
        fact_list.append([filingID, fact.qname.prefix, fact.qname.localName, txtVal, numVal, decimals, currency, endDate, startDate, ctxtMap.get(fact.contextID,'')])

    return fact_list

def extract_uniqueDims(xbrl):
    global ctxtMap
    global dims

    # Assumes no context has more than 1 dimension - not very true!
    # We will need to relax this when fs notes are tagged as per SEC (See why below)
    # and build further 2 dicts: 1. DimNo: AxisMemberTagPair; 2. (DimNo Set) (multiple dims): newCtxtCode
    # Sets with same numbers but in different order are equal.
    dims = {} # AxisMemberTagPair: [filingID, newCurrCtxtCode, dimQname.prefix, dimQname.localName, dim.memberQname.prefix, dim.memberQname.localName]
    # New db contexts are different and simpler - do not contain dates so there are less of them. Also entity only contexts are discarded.
    # This map is for translating for Facts.
    ctxtMap = {} # ctxt.id (reported context): newCurrCtxtCode - One to Many (some ctxt.ids will map to same newCurrCtxtCodes - different years)
    
    ctxtTot = 0
    for ctxt in xbrl.contexts.values():        
        #print(ctxt.qnameDims.items())
        if len(ctxt.qnameDims.items()) > 0:
            if len(ctxt.qnameDims.items()) > 1:
                # If an AxisMbr pair appears in more than one (dateless) context then it will only be recorded for the first context encountered.
                # This could only occur if there were contexts that contained more than one dimension.
                # In this scenario (currently), this context would be ignored if both AxisMbr pairs exist separately in single dim contexts
                # Or if any one of the AxisMbr pairs are unique to this multi-dim context, they will be recorded as a new single dim context
                # If a context has more than one dimension which is unique to this multi-dim context then they will each be recorded erroneously in different contexts.
                # And the ctxtMap will be overwritten with this new context for the very last dimension so any uniquely new dimension to this context
                # that is not the last will not be recorded in any context. Obviously really not expecting this fs xbrl only.
                # If it is the last (which is most likely but not guaranteed) then it will in effect be recorded as the distinct dim for this context which is not a bad thing!
                print("More than 1 dimension in a Context")    
        for dimQname, dim in ctxt.qnameDims.items():
            #print(f'{ctxt.id}: {dim.contextElement}: {dimQname}: {dim.memberQname}')
            AxisMbrKey = dimQname.localName+dim.memberQname.localName
            existingDim = dims.get(AxisMbrKey)
            if existingDim is None:
                ctxtTot += 1 # Assumes each context has no more than one dimension.
                newCtxtCode = 'C-' + str(ctxtTot)
                dims[AxisMbrKey] = [filingID, newCtxtCode, dimQname.prefix, dimQname.localName, dim.memberQname.prefix, dim.memberQname.localName]          
                ctxtMap[ctxt.id] = newCtxtCode
                ctxtMap.update({ctxt.id: newCtxtCode})
            else:
                ctxtMap.update({ctxt.id: existingDim[1]}) # Dict saved newCtxtCode        
                ctxtMap[ctxt.id] = existingDim[1]    
    return list(dims.values())

def extract_dims(xbrl):
    global ctxtMap
    global dims

    dim_list = [[]]
    
    # Dims will be duplicated for different periods but can use external group query functions to see a unique version (in sql, power query etc)
    # Entity contexts with no dimensions are effectively discarded.
    # The dates that are in the contexts are held in the fact_list along with the dateless newCtxtCode (which we create here)
    # So you can retrieve the date of a context that way.
    # You can have many dims for any context although only one is usual (but not exclusive) where only fs are being tagged. 
    ctxtTot = 0
    for ctxt in xbrl.contexts.values():        
        #print(ctxt.qnameDims.items())
        # Excludes entity contexts that have no dimensions
        if len(ctxt.qnameDims.items()) > 0:
            ctxtTot += 1
            newCtxtCode = 'c-' + str(ctxtTot)
            ctxtMap[ctxt.id] = newCtxtCode
            for dimQname, dim in ctxt.qnameDims.items():
                # Possible for domains? No member - CH companies - 04730984
                if dim.memberQname is None:
                    # Can't be None (NULL) as part of primary key.
                    mbrPrefix = ''
                    mbrName = ''
                else:
                    mbrPrefix = dim.memberQname.prefix
                    mbrName = dim.memberQname.localName
                dim_list.append([filingID, newCtxtCode, dimQname.prefix, dimQname.localName, mbrPrefix, mbrName])        
                     
    return dim_list


#Based on get_anchoring_relationships
def extract_anchors(xbrl):
    Anchr_list = [[]]

    for baseSetKey, baseSetModelLinks  in xbrl.baseSets.items():
        arcrole, ELR, linkqname, arcqname = baseSetKey # This turns the baseSetKey into 4 separate variables.
        if arcrole in (WIDER_NARROWER_ARCROLE, ) and ELR is not None:
            relSet = xbrl.relationshipSet(arcrole, ELR)
            for r in relSet.modelRelationships:
                if r.fromModelObject is not None and r.toModelObject is not None:
                    testw = r.fromModelObject.qname.localName
                    testn = r.toModelObject.qname.localName
                    roots = tuple(root.qname for root in get_presentation_roots(xbrl, r.toModelObject if not is_base_qname(r.toModelObject.qname) else r.fromModelObject) )
                    root = roots[0].localName
                    #from wider, to narrower
                    Anchr_list.append([filingID, r.toModelObject.qname.prefix, r.toModelObject.qname.localName, r.fromModelObject.qname.prefix, r.fromModelObject.qname.localName, roots[0].localName])        
    
    return Anchr_list

def extract_concepts(xbrl):
    cept_list = [[]]
    taxyDate = None
    extNamespaceURI = next(iter(xbrl.namespaceDocs)) # Assumes extension namespace is always first!? 
    if params['index'] != 'ch':
        # This create a list of all the keys and gets the item (prefix) in it at the index corresponding to namespaceURI.
        extPrefix = list(xbrl.prefixedNamespaces.keys())[list(xbrl.prefixedNamespaces.values()).index(extNamespaceURI)]
    else:
        extPrefix = None

    for cept in xbrl.qnameConcepts.values():  # Can you do the test of preFix type in the for loop iteration def?
        label = None
        documentation = None
        references = None
        if cept.qname.prefix in (extPrefix, 'ifrs-full', 'IFRS', 'FRS-101', 'FRS-102', 'core', 'common', 'bus', 'dpl' 'ref','accrep', 'aurep', 'direp'): # Could be expandable as option or based on filing source selected eg. CH or SEC etc.            
            
            # if params['index'] != 'ch':
            # Check size is 2 before proceeeding!!
            if len(cept.propertyView[0]) >= 3:
                labels = cept.propertyView[0][2] # List of tuples
                for lbl in labels:
                    # You might get welsh versions hence (en). Whether we'd miss any labelled just 'label'?
                    if lbl[0].startswith('label (en)'):
                        label = lbl[1]
                    elif lbl[0].startswith('documentation (en)'):
                        documentation = lbl[1]
                refs = cept.propertyView[11]
            # else:
                #labels = cept.propertyView[0]
                #for lbl in labels:
                #    if lbl.startswith('label'):
                #        label = lbl[1]
                #    elif lbl[0].startswith('documentation'):
                #        documentation = lbl[1]
                #refs = cept.propertyView[11]
            # Reference is also broken down into its components either as example ref or disclosure ref in refs[2]
            if len(refs) > 0 and refs[0].startswith('references'):
                # Is there ever more than one reference - are they then split into a tuple?
                references = refs[1]
            if cept.qname.prefix != extPrefix:
                taxyDate = stdTaxyDate
            else:
                taxyDate = filingEndDate
            cept_list.append([cept.qname.prefix, cept.qname.localName, cept.niceType, cept.periodType, cept.balance, cept.abstract, cept.isTextBlock, cept.isNumeric, label, documentation, references, cept.qname.namespaceURI, taxyDate])
    #print(cept_list[-1])   
    return cept_list

def extract_namespaces(xbrl):
    ns_list = [[]]
    for prefix, ns in xbrl.prefixedNamespaces.items():
        ns_list.append([filingID, prefix, ns])
    return ns_list



def getStdTaxyDate(xbrl):
    # eg. http://xbrl.frc.org.uk/FRS-102/2023-01-01
    for nsDoc in xbrl.prefixedNamespaces.values():
        nsBits = nsDoc.split('/')
        if nsBits[0] in ('http://xbrl.frc.org.uk'):
            for nsBit in nsBits:
                if re.match(r'\d{4}-\d{2}-\d{2}', nsBit):                   
                    return nsBit 
    return None

#
# Return a dict of booleans indicating if each concept in
# MANDATORY_CONCEPT_LOCALNAMES is reported at least once in a base taxonomy
# namespace
#
def extract_mandatory_facts(xbrl):
    data = { concept: False for concept in MANDATORY_CONCEPT_LOCALNAMES }
    for fact in xbrl.facts:
        if is_base_qname(fact.qname) and fact.qname.localName in MANDATORY_CONCEPT_LOCALNAMES:
            data[fact.qname.localName] = True
    return data


#
# Return a dict containing "primary" and "all_used".  "all_used" is a list of
# all currencies used in the filing, "primary" is a single value with the most
# used currency.
#
def extract_currencies(xbrl):
    currencies = dict()
    for fact in xbrl.facts:
        if fact.concept.isMonetary:
            name = fact.unit.measures[0][0].localName
            currencies[name] = currencies.get(name, 0) + 1
    return {
        "primary": max(currencies, key=currencies.get),
        "all_used": currencies
    }


#
# Returns counts of the number of unique concepts, dimensions and members,
# split into base and extension counts.
#
def extract_concept_counts(xbrl):
    concepts = defaultdict(int)
    dimensions = defaultdict(int)
    members = defaultdict(int)
    for fact in xbrl.facts:
        concepts[fact.qname] += 1
        for dimension in fact.context.qnameDims.values():
            dimensions[dimension.dimensionQname] += 1
            if dimension.memberQname is not None:
                members[dimension.memberQname] += 1

    result = dict()
    for name, data in (("concept", concepts), ("dimension", dimensions), ("member", members)):
        result["base-%ss" % name] = sum(is_base_qname(n) for n in data.keys())
        result["extension-%ss" % name] = sum(not is_base_qname(n) for n in data.keys())
        result["base-%s-facts" % name] = sum(c for n, c in data.items() if is_base_qname(n))
        result["extension-%s-facts" % name] = sum(c for n, c in data.items() if not is_base_qname(n))

    result["facts"] = len(xbrl.facts)

    return result


#
# Return a list of URLs that are on "base taxonomy hosts" and imported from
# files that are not on base taxonomy hosts.  This should correspond to the
# effective entry point of the base taxonomy.
#
def get_base_taxonomy_urls(xbrl):
    files = set()
    for uri, doc in xbrl.urlDocs.items():
        if urlparse(uri).hostname not in BASE_TAXONOMY_HOSTS:
            for refDoc, ref in doc.referencesDocument.items():
                if urlparse(refDoc.uri).hostname in BASE_TAXONOMY_HOSTS:
                    files.add(refDoc.uri)
    return list(files)


#
# Find the most recent duration used in the report that is approximately one
# year in length.  Returns a tuple of (start, end) as datetime objects.
#
def latest_annual_period(xbrl):
    periods = set()
    all_periods = set()
    for fact in xbrl.facts:
        if fact.context.isStartEndPeriod:
            period_length = fact.context.endDatetime - fact.context.startDatetime
            all_periods.add((fact.context.startDatetime, fact.context.endDatetime))
            if period_length.days > 350 and period_length.days < 380:
                periods.add((fact.context.startDatetime, fact.context.endDatetime))
    if len(periods) == 0:
        print("Could not find any periods of approximately 1yr.  Periods found: ")
        print(all_periods)
        return None
    return max(periods, key = lambda x: x[1])

def extract_key_values(xbrl, period):
    # This doesn't currently check the units of the concepts found, and only
    # looks for non-dimensional facts
    values = {}
    for fact in xbrl.facts:
        if (is_base_qname(fact.concept.qname) 
            and (not fact.context.isStartEndPeriod or fact.context.startDatetime == period[0])
            and fact.context.endDatetime == period[1]
            and len(fact.context.qnameDims) == 0):
            for key, concepts in KEY_VALUES.items():
                if fact.concept.qname.localName in concepts:
                    values.setdefault(key, {})[fact.concept.qname.localName] = fact.value
    return values

def get_separate_member_count(xbrl):
    count = 0
    for fact in xbrl.facts:
        if any(d.memberQname is not None and is_base_qname(d.memberQname) and d.memberQname.localName == 'SeparateMember' for d in fact.context.qnameDims.values()):
            count += 1
    return count

def loadDb(fsd):    
    import pandas as pd
    from svrOps import loadTbl

    # Key point 6
    
    loadTblNames = ('Facts3', 'Dims3', 'Anchors3', 'Concepts3', 'Namespaces3') # May want to create multiple versions of tables for testing etc hence numbers. These should correspond with db table names
    loadTblDefs = {}
    # Loads up table defs for db loading first.
    for nm in loadTblNames:
        
        hdrdf = pd.read_csv('dbtables\\' + nm  + '.csv', nrows=0) # nrows= 0 means we just load the headers - overkill using pd really        
        hdrdf.columns.name = nm
        dataListName = ''.join((x for x in nm if not x.isdigit())).lower() # Removes digits to give raw name used to load data from arelle parser.
        loadTblDefs[nm] = (dataListName, hdrdf.columns) # str, list of str's        

    print("Db Loading for %s" % filingLEI)
    #Loads the datasets for each filing into db as specified by loadTblNames - number of fields and order must match in defs and filing_data (fd) currently.
    loaded = 0    
    for fd in fsd.values():        
        if len(fd) != 0: # Probably! already added so empty filing data dict was added to fsd (or db fail) so don't attempt load - nothing will be loaded anyway as primary keys will be tripped.
            tables = 0
            for nm, tblDef in loadTblDefs.items():
                dataList = fd[tblDef[0]]
                if len(dataList) > 1: # A list maybe empty eg. Anchors
                    ltd = dataList[1:]            
                    svrOps.loadTbl(secrets, nm, tblDef[1], ltd)
                    tables += 1 
            loaded += 1
            print("%s Main Tables loaded. Loading finished %s" % (tables, fd['filing']))
            # Loads extra filing details
            svrOps.loadRecord(secrets, 'FilingsExtraDetail',['Filing_ID', 'Vendor', 'ZipSize','ConcealedFacts','HiddenFacts'],[fd['filingID'], fd['vendor'], fd['zip-size'], fd['concealed-facts'], len(fd['hidden-facts'])])
            print("Extra Table loaded. Loading finished %s" % (fd['filing']))
    
    print("Loaded to Db %s filings for %s" % (loaded, filingLEI))
    return True


# Modified version of analyse_ixbrl - JT - 19/03/24 to pick up more data ready for adding to db
def load_ixbrl(package_path, report_path):
    print("Loading %s" % report_path)
    data = {}

    global xbrl
    global filingID
    global stdTaxyDate

    
    # None for ch filings
    if package_path is not None: 
        pi = arelle.addPackage(package_path)
    # Key Point 3
    print("Arelle loading %s" % filing)
    (xbrl, errors) = arelle.loadReport(report_path) 


    if any(f.concept is None for f in xbrl.facts):
        print("Report missing concept definitions")
    else:
        # Key Point 5
        print("Extracting data %s" % filing)
        period = latest_annual_period(xbrl)

        # Added - JT - 21/03/24 
        # filingEndDate = period[1].strftime('%Y-%m-%d') # Needs to be in string form for db add.
        #filing = os.path.basename(xbrl.uri).split('.')[0] # or we just use value already picked up in loading filing?
        #filingID = SvrOps.loadRecord('Filings',['Register', 'Entity_ID','Filing','Filing_EndDate'],[register, filingLEI, filing, filingEndDate])
        stdTaxyDate = getStdTaxyDate(xbrl)

        # Added - JT - 21/03/24
        data['filingID'] = filingID # Required for db.
        data["filing"] = filing 
        data["dims"] = extract_dims(xbrl)
        data["facts"] = extract_facts(xbrl)
        data["anchors"] = extract_anchors(xbrl)
        data["concepts"] = extract_concepts(xbrl)
        data["namespaces"] = extract_namespaces(xbrl)

        #data["extensions"] = extract_extensionConcepts(xbrl)
        #data["anchors"] = get_anchoring_relationships(xbrl)
        data["currencies"] = extract_currencies(xbrl)
        data["dimension-anchors"] = get_dimension_anchors(xbrl)
        data["dts-files"] = get_base_taxonomy_urls(xbrl)
        data["mandatory-facts"] = extract_mandatory_facts(xbrl)
        data["counts"] = extract_concept_counts(xbrl)
        data["separate-member-fact-count"] = get_separate_member_count(xbrl)
        data["validation"] = errors

        if period is not None:
            data["key-metrics"] = extract_key_values(xbrl, period)

    xbrl.close()
    # None for ch filings
    if package_path is not None: 
        arelle.removePackage(pi)

    print("Finished core processing %s" % filing)
    return data

#
# Return the namespaces declared in the XBRL-JSON file
#
def analyse_xbrl_json(path):
    xj = load_json(path)
    return  {"namespaces": list(xj.get("documentInfo",{}).get("namespaces", {}).values())}


def loadFiling(meta_info):
    global filingEndDate
    global filingID
    global filing

    filingID = None
    
    print("Processing %s" % filing) 
    
    locs['report_file'] = meta_info.get('report_url')

    if locs['report_file'] is None: # Cases of it missing - eg. 213800WFVZQMHOZP2W17 1st filing
        print("Processing Aborted %s - report file url missing" % filing)
        return {}

    # Files downloaded in following lines.
    if params['index'] != 'ch':
        locs['report_package_path'] = local_file(meta_info.get('package_url'),locs['base_filings_uri'], cache_dir = params['cache_dir'])
        locs['report_path'] = local_file(locs['report_file'], locs['base_filings_uri'], cache_dir = params['cache_dir'])
    else:
        locs['report_package_path'] = None
        locs['report_path'] = local_file(locs['report_file'], locs['base_filings_uri'], cache_dir = params['cache_dir'], savePath = meta_info.get('package_url'))
    
    filingEndDate = meta_info.get('period_end')    
    
    # Would happen if CH download failed.
    if locs['report_path'] is None: 
        print("Processing Aborted %s - report file missing" % filing)
        return {}
    
        
    if params['db']:
        # New filng id is returned aas filing meta data is added to db. Filing Id used when storing to db tables.
        # If filing already exists in db then None is returned. Note register will be added to Primary Key so the same filing (different filingID) can be added to db.
        # Could add new col with a pointer to unique filingID so data is only stored once? At moment it would be repeatedly stored (be the same data?) with different filngs id's
        filingID = svrOps.loadRecord(secrets, 'Filings',['Register', 'LEI', 'Entity_ID','Filing','Filing_EndDate','Filing_PubDate','Filing_SubDate','DisclosureLevel'],[register, filingLEI, filingLEI, filing, filingEndDate, meta_info['publication_date'],meta_info['submitted_date'],meta_info['disclosure_level']])
        if filingID is None:
            print("Processing Aborted %s - already in db" % filing)
            return {}         
    else:
        filingID = register + filing # Should make it unique! Can wrtie to files for viewing or connecting together in Excel oor running into db at a later stage.

    return load_ixbrl(locs['report_package_path'], locs['report_path'])
    
def loadExtraDetails():
    xDetails = {}
    if locs['report_package_path'] is not None and Path(locs['report_package_path']).suffix == '.zip':
        xDetails['zip-size'] = os.path.getsize(locs['report_package_path'])
    else:
        xDetails['zip-size'] = None
    xDetails['concealed-facts'] = xbrlanalyse.concealed_fact_count(locs['report_path'])
    xDetails['hidden-facts'] = xbrlanalyse.hidden_facts(locs['report_path'])
    xDetails['vendor'] = xbrlanalyse.identify_software(locs['report_path'])
    print("Finished extra processing %s" % filing)

    return xDetails

def loadCHFilings(lei):
    global params
    global filings_data
    global filingLEI
    global filing

    params['lei'] = lei[0]
    filingLEI = lei[0]

    #Do we need to hold it for all filings as loading it into db - release each time - no as just holding it for filings for each Filer?
    #Might hold them as latest filings at some point (multiple filers)
    # filings_data = { "filings": {} }
    filings_data = {}   
    #ns_count = {}

    # filingLEI = params['lei'][0] # Should clean this up when clean up arguments
    # index = load_json(locs['index_file'] + '/' + filingLEI + '/filings' )
    filings_info = getCHFilingMetaData()

    if filings_info is None:
        print("Processing Aborted %s - Filer Index missing" % filingLEI)
        return filings_data

    if params['limit'] is not None:
        limit = params['limit']
    else:
        limit = 5
    
    if params['after'] is not None:
        after_date = datetime.date.fromisoformat(params['after'])

    filing_count = 0
    # Iterates through each filing in json array found at index/lei/filings url

    for filing_info in filings_info:
        if params['country'] is not None and filing_info.get("country", None) in  params['country']:
            continue
        if after_date is not None and datetime.date.fromisoformat(filing_info.get("period_end", None)) < after_date:
            continue
        package_uri= filing_info.get('package_url')
        filing = Path(package_uri).stem
        # filing = os.path.basename(package_uri) # Use this when we store to db
        # Hammering stuff so it fits into frc-analyse existing boxes
        # base_filings_uri = os.path.dirname(locs['base_uri'])
        #base_uri = filings.org;
        # Key point 1
        # filings_data[filing] = process_load_filing(base_filings_uri, params['output_dir'], filing_metadata, force = params['force'])
        filings_data[filing] = loadFiling(filing_info)
        if not params['db'] or filingID is not None:
            filings_data[filing].update(loadExtraDetails())
        #print("Finished processing %s" % filing)      
        filing_count += 1
        if filing_count >= limit:
            break
        
    return filings_data

def loadFCAFilings(lei):
    global params
    global filings_data
    global filingLEI
    global filing

    params['lei'] = lei[0]
    filingLEI = lei[0]

    #Do we need to hold it for all filings as loading it into db - release each time - no as just holding it for filings for each Filer?
    #Might hold them as latest filings at some point (multiple filers)
    # filings_data = { "filings": {} }
    filings_data = {}   
    #ns_count = {}

    # filingLEI = params['lei'][0] # Should clean this up when clean up arguments
    # index = load_json(locs['index_file'] + '/' + filingLEI + '/filings' )
    filings_info = getFCAFilingMetaData()

    if filings_info is None:
        print("Processing Aborted %s - Filer Index missing" % filingLEI)
        return filings_data

    if params['limit'] is not None:
        limit = params['limit']
    else:
        limit = 5
    
    if params['after'] is not None:
        after_date = datetime.date.fromisoformat(params['after'])

    filing_count = 0
    # Iterates through each filing in json array found at index/lei/filings url

    for filing_info in filings_info:
        if params['country'] is not None and filing_info.get("country", None) in  params['country']:
            continue
        if after_date is not None and datetime.date.fromisoformat(filing_info.get("period_end", None)) < after_date:
            continue
        package_uri= filing_info.get('package_url')
        filing = Path(package_uri).stem
        # filing = os.path.basename(package_uri) # Use this when we store to db
        # Hammering stuff so it fits into frc-analyse existing boxes
        # base_filings_uri = os.path.dirname(locs['base_uri'])
        #base_uri = filings.org;
        # Key point 1
        # filings_data[filing] = process_load_filing(base_filings_uri, params['output_dir'], filing_metadata, force = params['force'])
        filings_data[filing] = loadFiling(filing_info)
        if not params['db'] or filingID is not None:
            filings_data[filing].update(loadExtraDetails())
        #print("Finished processing %s" % filing)      
        filing_count += 1
        if filing_count >= limit:
            break
        
    return filings_data

def loadFOFilings(lei):
    global params
    global filings_data
    global filingLEI
    global filing

    params['lei'] = lei[0]
    filingLEI = lei[0]

    #Do we need to hold it for all filings as loading it into db - release each time - no as just holding it for filings for each Filer?
    #Might hold them as latest filings at some point (multiple filers)
    # filings_data = { "filings": {} }
    filings_data = {}   
    #ns_count = {}
  
    if params['limit'] is not None:
        limit = params['limit']
    else:
        limit = 5
    
    if params['after'] is not None:
        after_date = datetime.date.fromisoformat(params['after'])

    # filingLEI = params['lei'][0] # Should clean this up when clean up arguments
    # Could limit by number and date when we assemble meta data if wanted to
    filings_info = getFOFilingMetaData(filingLEI)
    if filings_info is None:    
        return filings_data

    filing_count = 0    
    # Iterates through each filing in json array found at index/lei/filings url
    for filing_info in filings_info:
        # dict get but returns None rather than failing if key is missing.
        if params['country'] is not None and filing_info.get("country", None) in  params['country']:
            continue
        if after_date is not None and datetime.date.fromisoformat(filing_info.get("period_end", None)) < after_date:
            continue
        package_uri= filing_info.get('package_url')
        filing = os.path.dirname(package_uri) # Use this when we store to db
        # Hammering stuff so it fits into frc-analyse existing boxes
        # base_filings_uri = os.path.dirname(locs['base_uri'])
        print("Processing %s" % filing)
        #base_uri = filings.org;
        # Key point 1
        # filings_data[filing] = process_load_filing(base_filings_uri, params['output_dir'], filing_metadata, force = params['force'])
        loadFiling(filing_info)
        print("Finished processing %s" % filing)      
        filing_count += 1
        if filing_count >= limit:
            break
        
    return filings_data