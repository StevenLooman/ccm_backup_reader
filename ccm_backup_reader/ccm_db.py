# -*- coding: utf-8 -*-

import os.path
import re
import sqlite3

from collections import OrderedDict

from ccm_backup_reader.ccm_query_parser import SqlQueryBuilder
import ccm_backup_reader.ccm_utils as ccm_utils


sqlite3.enable_callback_tracebacks(True)


_COMPVER_ATTR_NAMES = OrderedDict([
    ('create_time', 'time'),
    ('cvtype', 'string'),
    ('is_asm', 'boolean'),
    ('is_model', 'boolean'),
    ('modify_time', 'time'),
    ('name', 'string'),
    ('owner', 'string'),
    ('release', 'string'),
    ('status', 'string'),
    ('subsystem', 'string'),
    ('version', 'string'),
])


def ccm_status(status_log):
    """ Extract last status change """
    if not status_log:
        return status_log
    matches = re.findall("Status set to '(\w+)' by", status_log)
    if not matches:
        return ''
    return matches[-1]


class CcmDb(object):

    def __init__(self, backup_path, dbdump_filename='DBdump.sqlite3'):
        self._backup_path = backup_path

        db_path = os.path.join(backup_path, dbdump_filename)
        db_uri = 'file:' + db_path + '?mode=ro'
        connection = sqlite3.connect(db_uri, uri=True)
        connection.create_function("ccm_status", 1, ccm_status)
        self._db_connection = connection

    @property
    def backup_path(self):
        return self._backup_path

    def delim(self):
        """ """
        query = \
"""
select attrib.strval
from   compver inner join attrib on (compver.id = attrib.is_attr_of)
where  compver.name = 'base' and
       compver.version = '1' and
       compver.cvtype = 'model' and
       compver.subsystem = 'base' and
       attrib.name = 'delimiter';
"""
        cursor = self._db_connection.cursor()
        cursor.execute(query)
        delim = cursor.fetchone()[0]
        return delim

    def attrs(self, four_part_name):
        """ """
        fpn = ccm_utils.parse_fpn(four_part_name, self.delim())

        # attributes from attrib-table
        query = \
"""
SELECT attrib.name, attrib.textval
FROM   compver INNER JOIN attrib ON (compver.id = attrib.is_attr_of)
WHERE  compver.name = ? AND
       compver.version = ? AND
       compver.cvtype = ? AND
       compver.subsystem = ?;
"""
        args = (fpn['name'], fpn['version'], fpn['type'], fpn['instance'])
        cursor = self._db_connection.cursor()
        cursor.execute(query, args)
        attrib_attrs = {row[0]: ccm_utils.type_from_text(row[1]) for row in cursor.fetchall()}

        # attributes from compver-table
        compver_attrs = _COMPVER_ATTR_NAMES.copy()

        attrs = {}
        attrs.update(attrib_attrs)
        attrs.update(compver_attrs)
        return attrs

    def attr(self, four_part_name, attr_name):
        """ """
        # attributes from attrib-table
        fpn = ccm_utils.parse_fpn(four_part_name, self.delim())
        query = \
"""
SELECT attrib.textval
FROM   compver INNER JOIN attrib ON (compver.id = attrib.is_attr_of)
WHERE  compver.name = ? AND
       compver.version = ? AND
       compver.cvtype = ? AND
       compver.subsystem = ? AND
       attrib.name = ?;
"""
        args = (fpn['name'], fpn['version'], fpn['type'], fpn['instance'], attr_name)
        cursor = self._db_connection.cursor()
        cursor.execute(query, args)
        result = cursor.fetchone()
        if result:
            return ccm_utils.deserialize_textval(result[0])

        # attributes from compver-table
        query = "SELECT " + ", ".join(_COMPVER_ATTR_NAMES.keys()) + \
"""
FROM   compver
WHERE  compver.name = ? AND
       compver.version = ? AND
       compver.cvtype = ? AND
       compver.subsystem = ?;
"""
        args = (fpn['name'], fpn['version'], fpn['type'], fpn['instance'])
        cursor = self._db_connection.cursor()
        cursor.execute(query, args)
        result = cursor.fetchone()
        if result:
            compver_attrs = dict(zip(_COMPVER_ATTR_NAMES.keys(), result))
            return compver_attrs.get(attr_name)

        return None

    def query(self, ccm_query):
        """ Parse a CCM query and return resulting compvers """
        delim = self.delim()
        query_builder = SqlQueryBuilder(delim)
        sql_query = query_builder.build(ccm_query)

        cursor = self._db_connection.cursor()
        cursor.execute(sql_query)
        return [dict(zip(query_builder.COMPVER_COLUMNS, row)) for row in cursor.fetchall()]

    def finduse_task(self, four_part_name):
        fpn = ccm_utils.parse_fpn(four_part_name, self.delim())
        names = ['objectname', 'status']

        # find all task_in_baseline ?
        # find all task_in_folder ?  XXX TODO: is this correct?
        sql_query = \
            ("SELECT cv2.name || '" + self.delim() + "' || cv2.version || ':' || cv2.cvtype || ':' || cv2.subsystem AS objectname, ccm_status(a1.textval) AS status " + \
             "FROM compver cv1 INNER JOIN relate r1 ON (cv1.id = r1.to_cv) INNER JOIN relate r2 ON (r1.from_cv = r2.to_cv) INNER JOIN compver cv2 ON (r2.from_cv = cv2.id) LEFT JOIN attrib a1 ON (cv2.id = a1.is_attr_of) " + \
             "WHERE cv1.name = '{name}' AND cv1.version = '{version}' AND cv1.cvtype = '{type}' AND cv1.subsystem = '{instance}' AND " + \
                  "r1.name = 'task_in_folder' AND " + \
                  "r2.name = 'folder_in_rp' AND "+ \
                  "a1.name = 'status_log'").format(**fpn)
        # find all dirty_task_in_baseline ?

        cursor = self._db_connection.cursor()
        cursor.execute(sql_query)
        return [dict(zip(names, row)) for row in cursor.fetchall()]

    def contents_dir(self, four_part_name):
        fpn = ccm_utils.parse_fpn(four_part_name, self.delim())
        if fpn['type'] != 'dir':
            raise CcmError('Object is not a directory')

        sql_query = \
            ("SELECT bsite.info " + \
             "FROM bsite " + \
             "WHERE bsite.is_bsite_of = (" + \
             "  SELECT compver.id " + \
             "  FROM compver " + \
             "  WHERE compver.name = '{name}' AND compver.version = '{version}' AND compver.cvtype = '{type}' AND compver.subsystem = '{instance}' " + \
             ") AND bsite.info NOT LIKE '%/dir/%' " + \
             "ORDER BY bsite.info").format(**fpn)

        cursor = self._db_connection.cursor()
        cursor.execute(sql_query)
        return [row[0] for row in cursor.fetchall()]

    def query_sql(self, sql_query, *args):
        """ Execute a SQL query on table compver and return resulting compvers """
        cursor = self._db_connection.cursor()
        cursor.execute(sql_query, args)
        return cursor.fetchall()
