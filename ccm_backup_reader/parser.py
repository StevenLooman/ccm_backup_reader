#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import fileinput
import os
import re
import sys


# version
# platform
# schemaversion
# Section START/Section END
# table/tblend    table/table end
# rs/re           record start/record end
# tx/te           text start/text end
# s:              string
# i:              int
# f:              float
# bn              binary null
# fn              float null
# in              int null
# tn              text null
# sn              string null


class ParserError(RuntimeError):
    """
    """

    def __init__(self, reader, string):
        super(RuntimeError, self).__init__(str(reader.lineno) + ": " + string)


class IntegrityError(RuntimeError):
    """
    """

    def __init__(self, reader, string):
        super(RuntimeError, self).__init__(str(reader.lineno) + ": " + string)


def hook_compressed_encoded(encoding):
    def hook(filename, mode):
        ext = os.path.splitext(filename)[1]
        if ext == '.z' or ext == '.Z':
            from io import TextIOWrapper
            from zipfile import ZipFile

            zip_file = ZipFile(filename, 'r')
            zip_fd = zip_file.open('-')
            return TextIOWrapper(zip_fd, encoding, None, None)
        else:
            return open(filename, 'rt', encoding=encoding)
    return hook


class FileInputLineReader(object):
    """
    """

    def __init__(self):
        self._file_input = fileinput.input(openhook=hook_compressed_encoded("latin-1"))

    def readline(self):
        try:
            line = self._file_input.__next__()
        except StopIteration:
            raise EOFError
        if line.endswith('\n'):
            line = line[:-1]
        if line.endswith('\r'):
            line = line[:-1]
        return line

    def close(self):
        self._file_input.close()

    @property
    def lineno(self):
        return self._file_input.filelineno()

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self.lineno)

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        if tb:
            print('Encountered error at lineno: {}'.format(self.lineno))
        self.close()


class CcmBackupParser(object):
    """
    """

    UNESCAPE_TEXT_OL_TABLE = {
        r"'(.)": lambda m: chr(ord(m.group(1)) - 0x20),
        r'`(.)`(.)': lambda m: bytes([ord(m.group(1)) + 0x80, ord(m.group(2)) + 0x80]).decode('utf-8'),
        r'`b"`"(.)': lambda m: bytes([0xe2, 0x80, ord(m.group(1)) + 0x20]).decode('utf-8'),
        r'`b"``(.)': lambda m: bytes([0xe2, 0x80, ord(m.group(1)) + 0x80]).decode('utf-8'),
    }

    UNESCAPE_TEXT_TABLE = {
        r'\\([ \*])': lambda m: chr(ord(m.group(1)) - 0x20),
    }

    def __init__(self, reader):
        self._reader = reader
        self._callbacks = {}

        exprs = [r for r in CcmBackupParser.UNESCAPE_TEXT_OL_TABLE.keys()]
        self._unescape_text_ol_re = re.compile('|'.join(exprs))

        exprs = [r for r in CcmBackupParser.UNESCAPE_TEXT_TABLE.keys()]
        self._unescape_text_re = re.compile('|'.join(exprs))

    def _unescape_text_ol(self, text):
        def unescape_replace(match):
            s = match.group(0)
            for expr, func in CcmBackupParser.UNESCAPE_TEXT_OL_TABLE.items():
                m = re.match(expr, s)
                if m:
                    return func(m)

        return self._unescape_text_ol_re.sub(unescape_replace, text)

    def _unescape_text(self, text):
        def unescape_replace(match):
            s = match.group(0)
            for expr, func in CcmBackupParser.UNESCAPE_TEXT_TABLE.items():
                m = re.match(expr, s)
                if m:
                    return func(m)

        return self._unescape_text_re.sub(unescape_replace, text)

    def set_callback(self, event, callback):
        self._callbacks[event] = callback

    def get_callback(self, event):
        return self._callbacks.get(event)

    def _callback(self, event, data):
        if event in self._callbacks:
            self._callbacks[event](event, data)

    def _parse_object(self, line):
        if line.startswith('s:'):
            return line[2:]
        elif line.startswith('i:'):
            return int(line[2:])
        elif line.startswith('f:'):
            # XXX: unverified
            return float(line[2:])
        elif line.startswith('tx'):
            count = int(line[2:])
            text = ''

            # read text
            while True:
                data = self._reader.readline()
                data = data.replace('\\\\', '\\')
                text += data
                if len(text.encode('latin-1')) >= count:
                    break

            # escaping, if needed
            if text.startswith('oa') or text.startswith('ob') or text.startswith('oj'):
                text = text[2:]
            elif text.startswith('ol'):
                ol_match = re.match('^ol(\d+),', text)
                text = text[2 + len(ol_match.group(1)) + 1:]
                text = self._unescape_text_ol(text)
            else:
                text = self._unescape_text(text)

            # ensure ok, read te
            line = self._reader.readline()
            if line != 'te':
                raise ParserError(self._reader, "Expected 'te' but found: '" + line + "'")

            return text
        elif line == 'sn':
            return None
        elif line == 'in':
            return None
        elif line == 'tn':
            return None
        elif line == 'bn':
            return None
        elif line == 'fn':
            return None

        raise ParserError(self._reader, "Unknown type: '" + line + '"')

    def _parse_version(self, line):
        line_items = line.split(' ')
        version = line_items[1]
        self._callback('version', version)

    def _parse_platform(self, line):
        line_items = line.split(' ')
        platform = line_items[1]
        self._callback('platform', platform)

    def _parse_schemaversion(self, line):
        line_items = line.split(' ')
        schemaversion = line_items[1]
        self._callback('schemaversion', schemaversion)

    def _parse_section(self, line):
        line_items = line.split(' ')
        section = {
            'name': ' '.join(line_items[2:4]),
            'items': [],
        }

        while True:
            line = self._reader.readline()
            if line == 'Section END':
                break

            obj = self._parse_object(line)
            section['items'].append(obj)

        self._callback('section', section)

    def _parse_record(self, line, table):
        record = []
        while True:
            line = self._reader.readline()
            if line == 're':
                break
            obj = self._parse_object(line)
            record.append(obj)

        self._callback('table_record', {'table': table, 'record': record})

    def _parse_table(self, line):
        line_items = line.split(' ')
        table = {
            'name': line_items[1],
            'record_count': 0,
        }

        self._callback('table_start', table)
        record_count = 0

        while True:
            line = self._reader.readline()
            if line == 'rs':
                record_count += 1
                table['record_count'] = record_count

                record = self._parse_record(line, table)
            elif line.startswith('tblend '):
                break

        line_items = line.split(' ')
        end_table_name = line_items[1]
        if end_table_name != table['name']:
            raise IntegrityError(self._reader, "Table end name differs, expected: '{}', got: '{}'".format(table['name'], end_table_name))
        end_record_count = int(line_items[2][1:-1])
        if end_record_count != table['record_count']:
            raise IntegrityError(self._reader, "Record count differs, expected: '{}', got: '{}'".format(end_record_count, table['record_count']))

        self._callback('table_end', table)

    def _parse_next(self):
        line = self._reader.readline()
        instruction = line.split(' ')[0]
        if instruction == 'version':
            self._parse_version(line)
        elif instruction == 'platform':
            self._parse_platform(line)
        elif instruction == 'schemaversion':
            self._parse_schemaversion(line)
        elif instruction == 'Section':
            self._parse_section(line)
        elif instruction == 'table':
            self._parse_table(line)
        else:
            raise ParserError(self._reader, "Unknown instruction: '" + instruction + "'")

    def parse(self):
        try:
            while True:
                self._parse_next()
        except EOFError:
            pass
