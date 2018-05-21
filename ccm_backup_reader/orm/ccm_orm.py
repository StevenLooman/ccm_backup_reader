# -*- coding: utf-8 -*-

from collections import defaultdict

from ccm_backup_reader import ccm_utils
from ccm_backup_reader.ccm_db import _COMPVER_ATTR_NAMES
from ccm_backup_reader.orm.ccm_objects import CcmBaseline
from ccm_backup_reader.orm.ccm_objects import CcmDirectory
from ccm_backup_reader.orm.ccm_objects import CcmFile
from ccm_backup_reader.orm.ccm_objects import CcmFolder
from ccm_backup_reader.orm.ccm_objects import CcmFolderTemplate
from ccm_backup_reader.orm.ccm_objects import CcmProcessRule
from ccm_backup_reader.orm.ccm_objects import CcmProblem
from ccm_backup_reader.orm.ccm_objects import CcmProject
from ccm_backup_reader.orm.ccm_objects import CcmProjectGrouping
from ccm_backup_reader.orm.ccm_objects import CcmRelease
from ccm_backup_reader.orm.ccm_objects import CcmReleaseDef
from ccm_backup_reader.orm.ccm_objects import CcmTask


CCM_ORM_OBJECT_TYPE_MAP = {
    'baseline': CcmBaseline,
    'project': CcmProject,
    'folder': CcmFolder,
    'problem': CcmProblem,
    'task': CcmTask,
    'dir': CcmDirectory,
    'project_grouping': CcmProjectGrouping,
    'releasedef': CcmReleaseDef,
    'process_rule': CcmProcessRule,
    'folder_temp': CcmFolderTemplate,
}


class CcmOrm:

    def __init__(self, ccm_db):
        self._db = ccm_db

    @property
    def delim(self):
        return self._db.delim()

    def _construct_object(self, cv_id, cv_type):
        type_ = CCM_ORM_OBJECT_TYPE_MAP.get(cv_type, CcmFile)
        return type_(self, cv_id)

    def _construct_release(self, release_id):
        return CcmRelease(self, release_id)

    def release_from_object(self, ccm_object):
        compver_id = ccm_object.id
        sql_query = \
            "SELECT r.id " + \
            "FROM compver cv INNER JOIN release r ON (cv.is_product = r.id) " + \
            "WHERE cv.id = ?"
        rows = self._db.query_sql(sql_query, compver_id)
        if not rows:
            return None
        row = rows[0]
        return self._construct_release(row[0])

    def release_name(self, ccm_release):
        sql_query = \
            "SELECT name " + \
            "FROM release " + \
            "WHERE id = ?"
        rows = self._db.query_sql(sql_query, ccm_release.id)
        if not rows:
            return None
        return rows[0][0]

    def object_by_id(self, compver_id):
        sql_query = \
            "SELECT cv.id AS cvid, cv.cvtype " + \
            "FROM compver cv " + \
            "WHERE cv.id = ?"
        rows = self._db.query_sql(sql_query, compver_id)
        if not rows:
            return None
        row = rows[0]
        cv_id = row[0]
        cv_type = row[1]
        return self._construct_object(cv_id, cv_type)

    def object_by_fpn(self, four_part_name):
        fpn = four_part_name if isinstance(four_part_name, dict) else ccm_utils.parse_fpn(four_part_name, self.delim) 
        sql_query = \
            "SELECT cv.id, cv.cvtype " + \
            "FROM compver cv " + \
            "WHERE cv.name = ? AND cv.version = ? AND cv.cvtype = ? and cv.subsystem = ?"
        rows = self._db.query_sql(sql_query, fpn['name'], fpn['version'], fpn['type'], fpn['instance'])
        if not rows:
            return None
        row = rows[0]
        cv_id = row[0]
        cv_type = row[1]
        return self._construct_object(cv_id, cv_type)

    def object_by_full_name(self, full_name):
        fpn = ccm_utils.parse_full_name(full_name)
        return self.object_by_fpn(fpn)

    def objects_by_partial_name(self, partial_name):
        subsystem, cvtype, name = partial_name.split('/')
        sql_query = \
            "SELECT cv.id, cv.cvtype " + \
            "FROM compver cv " + \
            "WHERE cv.name = ? AND cv.cvtype = ? and cv.subsystem = ?"
        rows = self._db.query_sql(sql_query, name, cvtype, subsystem)
        return [self._construct_object(row[0], row[1]) for row in rows]

    def objects_by_release(self, release):
        is_product = release.id
        sql_query = \
            "SELECT cv.id, cv.cvtype " + \
            "FROM compver cv " + \
            "WHERE cv.is_product = ?"
        rows = self._db.query_sql(sql_query, is_product)
        return [self._construct_object(row[0], row[1]) for row in rows]

    def object_fpn(self, ccm_object):
        compver_id = ccm_object.id
        sql_query = \
            "SELECT cv.name, cv.version, cv.cvtype AS type, cv.subsystem AS instance " + \
            "FROM compver cv " + \
            "WHERE cv.id = ?"
        rows = self._db.query_sql(sql_query, compver_id)
        if not rows:
            return None
        row = rows[0]
        return {
            'name': row[0],
            'version': row[1],
            'type': row[2],
            'instance': row[3],
        }

    def object_attributes(self, ccm_object):
        # from attr table
        cv_id = ccm_object.id
        sql_query = \
            "SELECT attr.name, attr.textval " + \
            "FROM attrib AS attr " + \
            "WHERE attr.is_attr_of = ?"
        rows = self._db.query_sql(sql_query, cv_id)
        attrib_attrs = {row[0]: row[1] for row in rows}

        # from compver table
        sql_query = \
            "SELECT " + ", ".join(_COMPVER_ATTR_NAMES.keys()) + " " + \
            "FROM compver " + \
            "WHERE id = ?"
        rows = self._db.query_sql(sql_query, cv_id)
        cv_attrs = dict(zip(_COMPVER_ATTR_NAMES.keys(), rows[0]))

        # release
        ccm_release = self.release_from_object(ccm_object)
        ccm_release_name = self.release_name(ccm_release)
        release_attrs = {'release': ccm_release_name}

        return {**attrib_attrs, **cv_attrs, **release_attrs}

    def related_from(self, ccm_object, relation_name):
        from_cv = ccm_object.id
        sql_query = \
            "SELECT cv.id, cv.cvtype " + \
            "FROM relate rel INNER JOIN compver cv ON (rel.from_cv = cv.id) " + \
            "WHERE rel.to_cv = ? AND rel.name = ?"
        rows = self._db.query_sql(sql_query, from_cv, relation_name)
        return [self._construct_object(row[0], row[1]) for row in rows]

    def related_to(self, ccm_object, relation_name):
        from_cv = ccm_object.id
        sql_query = \
            "SELECT cv.id, cv.cvtype " + \
            "FROM relate rel INNER JOIN compver cv ON (rel.to_cv = cv.id) " + \
            "WHERE rel.from_cv = ? AND rel.name = ?"
        rows = self._db.query_sql(sql_query, from_cv, relation_name)
        return [self._construct_object(row[0], row[1]) for row in rows]

    def related_all(self, ccm_object):
        relateds = {'from': {}, 'to': {}}
        cv_id = ccm_object.id

        sql_query = \
            "SELECT 'to', rel.name, cv.id, cv.cvtype " + \
            "FROM relate rel INNER JOIN compver cv ON (rel.to_cv = cv.id) " + \
            "WHERE rel.from_cv = ? " + \
            "UNION " + \
            "SELECT 'from', rel.name, cv.id, cv.cvtype " + \
            "FROM relate rel INNER JOIN compver cv ON (rel.from_cv = cv.id) " + \
            "WHERE rel.to_cv = ? "
        rows = self._db.query_sql(sql_query, cv_id, cv_id)
        for row in rows:
            direction = row[0]
            rel_name = row[1]
            ccm_obj = self._construct_object(row[2], row[3])
            relateds[direction][rel_name] = relateds[direction].get(rel_name, [])
            relateds[direction][rel_name].append(ccm_obj)

        return relateds

    def bound_children(self, has_asm, has_parent):
        asm_id = has_asm.id
        parent_id = has_parent.id
        sql_query = \
            "SELECT cv.id, cv.cvtype " + \
            "FROM bind INNER JOIN compver cv ON (bind.has_child = cv.id) " + \
            "WHERE has_asm = ? AND has_parent = ?"
        rows = self._db.query_sql(sql_query, asm_id, parent_id)
        return [self._construct_object(row[0], row[1]) for row in rows]

    def contents_dir(self, ccm_object):
        dir_id = ccm_object.id
        sql_query = \
            "SELECT bsite.info " + \
            "FROM bsite " + \
            "WHERE bsite.is_bsite_of = ?" + \
            "ORDER BY bsite.info"
        rows = self._db.query_sql(sql_query, dir_id)
        return [row[0] for row in rows]

