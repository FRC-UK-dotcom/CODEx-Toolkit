from lxml import etree
import re

FACT_ELEMENTS = {'{http://www.xbrl.org/2013/inlineXBRL}nonFraction', '{http://www.xbrl.org/2013/inlineXBRL}nonNumeric'}
IX_HIDDEN = '{http://www.xbrl.org/2013/inlineXBRL}hidden'


def toClark(nsmap, qname):

    if ':' in qname:
        pfx, localname = qname.split(':', 1)
    else:       
        pfx = None
        localname = qname
    
    ns = nsmap.get(pfx)
    if ns is None:
        raise ValueError("Unknown namespace prefix '{0}'!".format(pfx))
    return "{%s}%s" % (ns, localname)


def hidden_facts(path):
    in_hidden = False
    hidden_facts = {}
    for event, element in etree.iterparse(path, events = ("start", "end"), huge_tree=True):
        if event == 'start':
            if element.tag == IX_HIDDEN:
                in_hidden = True
        elif event == 'end':
            if element.tag == IX_HIDDEN:
                in_hidden = False
            if in_hidden and element.get("target", None) is None and element.get("name") is not None:
                name = toClark(element.nsmap, element.get("name"))
                hidden_facts.setdefault(name, [])
                hidden_facts[name].append(element.text)

    return hidden_facts

