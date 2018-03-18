#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os.path
import sqlite3
import sys

from ccm_backup_reader import CcmBackupParser
from ccm_backup_reader import FileInputLineReader


schema_creators = {
    '0114': ["CREATE TABLE attrib (id INTEGER PRIMARY KEY NOT NULL, name TEXT, modify_time INTEGER, textval TEXT, binval TEXT, strval TEXT, intval INTEGER, floatval TEXT, is_attr_of INTEGER, has_attype INTEGER);",
             "CREATE TABLE bind (has_asm INTEGER, has_bound_bs INTEGER, has_child INTEGER, has_parent INTEGER, create_time INTEGER, sync_time INTEGER, wa_time INTEGER);",
             "CREATE TABLE bsite (id INTEGER PRIMARY KEY NOT NULL, name TEXT, info TEXT, ui_info TEXT, is_bsite_of INTEGER, has_bstype INTEGER, has_next_bs INTEGER);",
             "CREATE TABLE compver (id INTEGER PRIMARY KEY NOT NULL, status TEXT, create_time INTEGER, modify_time INTEGER, owner TEXT, is_asm INTEGER, is_model INTEGER, subsystem TEXT, cvtype TEXT, name TEXT, version TEXT, is_product INTEGER, ui_info INTEGER, release INTEGER, has_cvtype INTEGER, has_model INTEGER, has_super_type INTEGER, acc_key_0 INTEGER, acc_key_1 INTEGER, acc_key_2 INTEGER, acc_key_3 INTEGER, acc_key_4 INTEGER, acc_key_5 INTEGER, acc_key_6 INTEGER, acc_key_7 INTEGER, acc_key_8 INTEGER, acc_key_9 INTEGER, acc_key_10 INTEGER, acc_key_11 INTEGER, acc_key_12 INTEGER, acc_key_13 INTEGER, acc_key_14 INTEGER, acc_key_15 INTEGER, acc_key_16 INTEGER, acc_key_17 INTEGER, acc_key_18 INTEGER, acc_key_19 INTEGER);",
             "CREATE TABLE control (id INTERGER PRIMARY KEY NOT NULL, nextid INTEGER, info TEXT);",
             "CREATE TABLE relate (name TEXT, from_cv INTEGER, to_cv INTEGER, create_time INTEGER);",
             "CREATE TABLE release (id INTEGER PRIMARY KEY NOT NULL, name TEXT);",
             "CREATE TABLE acckeys (id INTEGER PRIMARY KEY NOT NULL, attr_name TEXT, attr_value TEXT);",
             ],
}


def main():
    if len(sys.argv) < 2:
        print('Usage: ' + sys.argv[0] + ' <path to DBDump.Z>')
        sys.exit(1)

    if os.path.exists('DBdump.sqlite3'):
        print("DBdump.sqlite3 already exists, aborting")
        sys.exit(1)

    def schemaversion(event, data):
        c = conn.cursor()
        for statement in schema_creators[data]:
            c.execute(statement)
        conn.commit()

    def table_end(event, data):
        conn.commit()

    def table_record(event, data):
        statement = 'INSERT INTO {} VALUES ({});'.format(data['table']['name'], ','.join(['?' for d in data['record']]))
        cursor.execute(statement, data['record'])

    conn = sqlite3.connect('DBdump.sqlite3')
    with FileInputLineReader() as reader:
        cursor = conn.cursor()

        parser = CcmBackupParser(reader)
        parser.set_callback('schemaversion', schemaversion)
        parser.set_callback('table_end', table_end)
        parser.set_callback('table_record', table_record)

        parser.parse()


if __name__ == '__main__':
    main()
