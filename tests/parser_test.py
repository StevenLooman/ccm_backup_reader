#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os.path
import fileinput
import pytest

from ccm_backup_reader import CcmBackupParser


class FixtureReader(object):
    """
    """

    def __init__(self, name):
        filename = os.path.join('tests', 'fixtures', name)
        self._file_input = fileinput.input(files=(filename, ), openhook=fileinput.hook_encoded("latin-1"))

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


class TestPreambles:

    def test_read_version(self):
        with FixtureReader('test_preambles') as reader:
            did_read_version = False
            def read_version(event, version):
                assert version == '1.2.3'
                nonlocal did_read_version
                did_read_version = True

            parser = CcmBackupParser(reader)
            parser.set_callback('version', read_version)

            parser.parse()
            assert did_read_version

    def test_platform(self):
        with FixtureReader('test_preambles') as reader:
            did_read_platform = False
            def read_platform(event, platform):
                assert platform == 'WINDOWS_311'
                nonlocal did_read_platform
                did_read_platform = True

            parser = CcmBackupParser(reader)
            parser.set_callback('platform', read_platform)

            parser.parse()
            assert did_read_platform

    def test_schemaversion(self):
        with FixtureReader('test_preambles') as reader:
            did_read_schemaversion = False
            def read_schemaversion(event, schemaversion):
                assert schemaversion == '1234'
                nonlocal did_read_schemaversion
                did_read_schemaversion = True

            parser = CcmBackupParser(reader)
            parser.set_callback('schemaversion', read_schemaversion)

            parser.parse()
            assert did_read_schemaversion

    def test_section(self):
        with FixtureReader('test_preambles') as reader:
            did_read_section = False
            def read_section(event, section):
                assert section == {'items': ['entry'], 'name': 'DEFAULT TEST_SECTION'}
                nonlocal did_read_section
                did_read_section = True

            parser = CcmBackupParser(reader)
            parser.set_callback('section', read_section)

            parser.parse()
            assert did_read_section

    def test_table_start(self):
        with FixtureReader('test_preambles') as reader:
            did_read_table_start = False
            def read_table_start(event, table_start):
                assert table_start == {'name': 'table_1', 'record_count': 0}
                nonlocal did_read_table_start
                did_read_table_start = True

            parser = CcmBackupParser(reader)
            parser.set_callback('table_start', read_table_start)

            parser.parse()
            assert did_read_table_start

    def test_table_end(self):
        with FixtureReader('test_preambles') as reader:
            did_read_table_end = False
            def read_table_end(event, table_end):
                assert table_end == {'name': 'table_1', 'record_count': 0}
                nonlocal did_read_table_end
                did_read_table_end = True

            parser = CcmBackupParser(reader)
            parser.set_callback('table_end', read_table_end)

            parser.parse()
            assert did_read_table_end


class TestTable:

    def test_read_record_1(self):
        with FixtureReader('test_table_record_1') as reader:
            did_read_table_record = False
            def read_table_record(event, record):
                assert record == {'record': [1, 'string', 'Text'], 'table': {'name': 'table_1', 'record_count': 1}}
                nonlocal did_read_table_record
                did_read_table_record = True

            parser = CcmBackupParser(reader)
            parser.set_callback('table_record', read_table_record)

            parser.parse()
            assert did_read_table_record

    def test_read_record_2(self):
        with FixtureReader('test_table_record_2') as reader:
            did_read_table_record = False
            def read_table_record(event, record):
                assert record == {'record': [None, None, None, None, None], 'table': {'name': 'table_1', 'record_count': 1}}
                nonlocal did_read_table_record
                did_read_table_record = True

            parser = CcmBackupParser(reader)
            parser.set_callback('table_record', read_table_record)

            parser.parse()
            assert did_read_table_record


class TestText:

    def test_empty_text(self):
        with FixtureReader('test_empty_text') as reader:
            def read_table_record(event, record):
                assert record == {'record': [''], 'table': {'name': 'table_1', 'record_count': 1}}

            parser = CcmBackupParser(reader)
            parser.set_callback('table_record', read_table_record)

    def test_oa(self):
        with FixtureReader('test_text_oa') as reader:
            def read_table_record(event, record):
                assert record == {'record': ["oa1"], 'table': {'name': 'table_1', 'record_count': 1}}

            parser = CcmBackupParser(reader)
            parser.set_callback('table_record', read_table_record)

            parser.parse()

    def test_ob(self):
        with FixtureReader('test_text_ob') as reader:
            def read_table_record(event, record):
                assert record == {'record': ["ob2"], 'table': {'name': 'table_1', 'record_count': 1}}

            parser = CcmBackupParser(reader)
            parser.set_callback('table_record', read_table_record)

            parser.parse()
            parser.parse()

    def test_oj(self):
        with FixtureReader('test_text_oj') as reader:
            def read_table_record(event, record):
                assert record == {'record': ["oj1234"], 'table': {'name': 'table_1', 'record_count': 1}}

            parser = CcmBackupParser(reader)
            parser.set_callback('table_record', read_table_record)

            parser.parse()

    def test_ol(self):
        with FixtureReader('test_text_ol') as reader:
            def read_table_record(event, record):
                assert record == {'record': ["ol23,0123456789abc0123456789"], 'table': {'name': 'table_1', 'record_count': 1}}

            parser = CcmBackupParser(reader)
            parser.set_callback('table_record', read_table_record)

            parser.parse()

    def test_escape_1(self):
        with FixtureReader('test_text_escape_1') as reader:
            def read_table_record(event, record):
                assert record == {'record': ["ol20,\ttab5678901234567890"], 'table': {'name': 'table_1', 'record_count': 1}}

            parser = CcmBackupParser(reader)
            parser.set_callback('table_record', read_table_record)

            parser.parse()

    def test_escape_2(self):
        with FixtureReader('test_text_escape_2') as reader:
            def read_table_record(event, record):
                assert record == {'record': ["ol1,…"], 'table': {'name': 'table_1', 'record_count': 1}}

            parser = CcmBackupParser(reader)
            parser.set_callback('table_record', read_table_record)

            parser.parse()

    def test_escape_3(self):
        with FixtureReader('test_text_escape_3') as reader:
            def read_table_record(event, record):
                assert record == {'record': ["ol1,“"], 'table': {'name': 'table_1', 'record_count': 1}}

            parser = CcmBackupParser(reader)
            parser.set_callback('table_record', read_table_record)

            parser.parse()

    def test_escape_4(self):
        with FixtureReader('test_text_escape_4') as reader:
            def read_table_record(event, record):
                assert record == {'record': ["ol1,ë"], 'table': {'name': 'table_1', 'record_count': 1}}

            parser = CcmBackupParser(reader)
            parser.set_callback('table_record', read_table_record)

            parser.parse()

    def test_escape_5(self):
        with FixtureReader('test_text_escape_5') as reader:
            def read_table_record(event, record):
                assert record == {'record': ["a\nb"], 'table': {'name': 'table_1', 'record_count': 1}}

            parser = CcmBackupParser(reader)
            parser.set_callback('table_record', read_table_record)

            parser.parse()

    def test_escape_6(self):
        with FixtureReader('test_text_escape_6') as reader:
            def read_table_record(event, record):
                assert record == {'record': ["a\*b"], 'table': {'name': 'table_1', 'record_count': 1}}

            parser = CcmBackupParser(reader)
            parser.set_callback('table_record', read_table_record)

            parser.parse()

    def test_escape_6(self):
        with FixtureReader('test_text_escape_7') as reader:
            def read_table_record(event, record):
                assert record == {'record': ["a\\nb"], 'table': {'name': 'table_1', 'record_count': 1}}

            parser = CcmBackupParser(reader)
            parser.set_callback('table_record', read_table_record)

            parser.parse()


class TestString:

    def test_empty_string(self):
        with FixtureReader('test_empty_string') as reader:
            def read_table_record(event, record):
                assert record == {'record': [''], 'table': {'name': 'table_1', 'record_count': 1}}

            parser = CcmBackupParser(reader)
            parser.set_callback('table_record', read_table_record)

            parser.parse()
