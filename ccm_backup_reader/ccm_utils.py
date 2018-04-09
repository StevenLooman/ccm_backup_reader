# -*- coding: utf-8 -*-

import re
from datetime import datetime


def parse_fpn(four_part_name, delim):
    fpn = {}
    fpn['name'], version_type_instance = four_part_name.split(delim)
    fpn['version'], fpn['type'], fpn['instance'] = version_type_instance.split(':')
    return fpn


UNESCAPE_TEXT_OL_TABLE = {
    r"'(.)": lambda m: chr(ord(m.group(1)) - 0x20),
    r'`(.)`(.)': lambda m: bytes([ord(m.group(1)) + 0x80, ord(m.group(2)) + 0x80]).decode('utf-8'),
    r'`b"`"(.)': lambda m: bytes([0xe2, 0x80, ord(m.group(1)) + 0x20]).decode('utf-8'),
    r'`b"``(.)': lambda m: bytes([0xe2, 0x80, ord(m.group(1)) + 0x80]).decode('utf-8'),
}

UNESCAPE_TEXT_TABLE = {
    r'\\([ \*])': lambda m: chr(ord(m.group(1)) - 0x20),
}

UNESCAPE_TEXT_OL_RE = re.compile('|'.join([r for r in UNESCAPE_TEXT_OL_TABLE.keys()]))
UNESCAPE_TEXT_RE = re.compile('|'.join([r for r in UNESCAPE_TEXT_TABLE.keys()]))


def unescape_text_ol(text):
    def unescape_replace(match):
        s = match.group(0)
        for expr, func in UNESCAPE_TEXT_OL_TABLE.items():
            m = re.match(expr, s)
            if m:
                return func(m)

    return UNESCAPE_TEXT_OL_RE.sub(unescape_replace, text)

def unescape_text(text):
    def unescape_replace(match):
        s = match.group(0)
        for expr, func in UNESCAPE_TEXT_TABLE.items():
            m = re.match(expr, s)
            if m:
                return func(m)

    return UNESCAPE_TEXT_RE.sub(unescape_replace, text)


def deserialize_ol(text):
    m = re.match('ol(\d)+,', text)
    g = m.group(0)
    return text[len(g):]


TYPE_IDS = {
    'oa': ('int', lambda text: int(text[2:])),  # XXX TODO: verify this type
    'ob': ('boolean', lambda text: str(text[2:] == '1').upper()),
    'oj': ('time', lambda text: datetime.fromtimestamp(int(text[2:])).strftime("%d-%m-%y %H:%M")),
    'ol': ('uptext', deserialize_ol),
}

def type_from_text(text):
    type_id = text[:2]
    if type_id not in TYPE_IDS:
        return 'string'
    return TYPE_IDS[type_id][0]


def deserialize_textval(text):
    type_id = text[:2]
    if type_id not in TYPE_IDS:
        return text
    caster = TYPE_IDS[type_id][1]
    return caster(text)
