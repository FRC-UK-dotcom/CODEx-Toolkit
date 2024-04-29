from lxml import etree
import re

FACT_ELEMENTS = {'{http://www.xbrl.org/2013/inlineXBRL}nonFraction', '{http://www.xbrl.org/2013/inlineXBRL}nonNumeric'}

def concealed_fact_count(path):
    concealed_count = 0
    none_stack = []
    header_stack = []
    for event, element in etree.iterparse(path, events = ("start", "end"), huge_tree=True):
        if event == 'start':
            style = element.get('style', None)
            in_none = style is not None and re.match(r'\bdisplay\s*:\s*none', element.get('style','')) is not None
            in_ix_header = element.tag == '{http://www.xbrl.org/2013/inlineXBRL}header'
            none_stack.append(in_none)
            header_stack.append(in_ix_header)
            if element.tag in FACT_ELEMENTS and any(none_stack) and not any(header_stack):
                concealed_count += 1
        elif event == 'end':
            none_stack.pop()
            header_stack.pop()

    return concealed_count
