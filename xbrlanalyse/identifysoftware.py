from lxml import etree
import re

FACT_ELEMENTS = {'{http://www.xbrl.org/2013/inlineXBRL}nonFraction', '{http://www.xbrl.org/2013/inlineXBRL}nonNumeric'}

def identify_software(path):
    classes = set()
    ids = set() 
    fact_count = 0
    id_count = 0
    comments = []
    for event, element in etree.iterparse(path, events = ("start", "end","comment", "pi"), huge_tree=True, remove_comments=False):
        if event == 'comment':
            comments.append(element.text)
        elif event == 'pi':
            comments.append('%s: %s' % (element.target, element.text))
        elif event == 'start':
            cc = element.get('class', "")
            for c in cc.split(' '):
                classes.add(c.strip())

            if element.tag in FACT_ELEMENTS:
                i = element.get('id', None)
                if i is not None:
                    id_count += 1
                    ids.add(i)
                fact_count += 1

    vendor_strings = []
    for c in comments:
        m = re.match('(?i)\s*((?:document\s+)?created (?:by|with|using)|merged by|generated by|i?xbrl document created with|generated using|Generado por):?\s+(.*)', c)
        if m is not None:
            vendor_strings.append(m.group(2))
        else:
            m = re.match('(?i)\s*(datatracks|integix|clausion)',c)
            if m is not None:
                vendor_strings.append(c)

    if len(vendor_strings) == 0:
        vendor = None
    else:
        vendor = " / ".join(vendor_strings)

    return vendor
