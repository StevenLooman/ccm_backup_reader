#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys

from ccm_backup_reader import CcmBackupParser
from ccm_backup_reader import FileInputLineReader


def version(event, data):
    print('-- ccm: Version: {}'.format(data))


def platform(event, data):
    print('-- ccm: Platform: {}'.format(data))


def schemaversion(event, data):
    print('-- ccm: Schema version: {}'.format(data))


def section(event, data):
    print('-- ccm: Section: {}'.format(data))


def table_start(event, data):
    print('-- ccm: Table start: {}'.format(data))


def table_end(event, data):
    print('-- ccm: Table end: {}'.format(data))


def table_record_print(event, data):
    print('-- ccm: Record: {}'.format(data))


def main():
    if len(sys.argv) < 2:
        print('Usage: ' + sys.argv[0] + ' <DBDump file>')
        sys.exit(1)

    with FileInputLineReader() as reader:
        parser = CcmBackupParser(reader)
        parser.set_callback('version', version)
        parser.set_callback('platform', platform)
        parser.set_callback('schemaversion', schemaversion)
        parser.set_callback('section', section)
        parser.set_callback('table_start', table_start)
        parser.set_callback('table_end', table_end)
        parser.set_callback('table_record', table_record_print)

        parser.parse()


if __name__ == '__main__':
    main()
