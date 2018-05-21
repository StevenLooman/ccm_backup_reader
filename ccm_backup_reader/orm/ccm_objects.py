# -*- coding: utf-8 -*-

import os.path
import re
from datetime import datetime

from ccm_backup_reader import ccm_utils
from ccm_backup_reader.ccm_error import CcmError
from ccm_backup_reader.ccm_archive_reader import CcmArchiveReader


class CcmRelease:

    def __init__(self, ccm_orm, id):
        self._orm = ccm_orm
        self._id = id

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._orm.release_name(self)

    @property
    def related_objects(self):
        return self._orm.objects_by_release(self)

    def __repr__(self):
        return "<{}({}, {})>".format(type(self).__name__, self.id, self.name)

    def __str__(self):
        return "<{}({}, {})>".format(type(self).__name__, self.id, self.name)


class CcmObject:

    def __init__(self, ccm_orm, id):
        self._orm = ccm_orm
        self._id = id

    @property
    def id(self):
        return self._id

    @property
    def fpn(self):
        return self._orm.object_fpn(self)

    @property
    def four_part_name(self):
        delim = self._orm.delim
        return "{name}{delim}{version}:{type}:{instance}".format(**self.fpn, delim=delim)

    @property
    def full_name(self):
        fpn = self.fpn
        return '{}/{}/{}/{}'.format(self.instance, self.type, self.name, self.version)

    @property
    def part_name(self):
        parts = self.full_name.split('/')
        return '/'.join(parts[0:3])

    @property
    def name(self):
        return self.fpn['name']

    @property
    def version(self):
        return self.fpn['version']

    @property
    def type(self):
        return self.fpn['type']

    @property
    def instance(self):
        return self.fpn['instance']

    @property
    def attributes(self):
        attribs = {}
        for key, value in self._orm.object_attributes(self).items():
            if isinstance(value, str):
                attribs[key] = ccm_utils.deserialize_textval(value)
            else:
                attribs[key] = value
        return attribs

    def attribute(self, name):
        return self.attributes[name]

    def __getitem__(self, key):
        return self.attribute(key)

    def related_from(self, relation_name):
        return self._orm.related_from(self, relation_name)

    def related_to(self, relation_name):
        return self._orm.related_to(self, relation_name)

    def related_all(self):
        return self._orm.related_all(self)

    @property
    def successors(self):
        return self.related_to('successor')

    @property
    def predecessors(self):
        return self.related_from('successor')

    @property
    def status(self):
        status_log = self['status_log']
        entries = status_log.split('\n')
        last_entry = entries[-1]
        return re.match(r".*'(\w+)'.*", last_entry).group(1)

    def status_time(self, status):
        last_time = datetime(1, 1, 1)
        status_log = self['status_log']
        entries = status_log.split('\n')
        for entry in reversed(entries):
            entry_status = re.match(r".*'(\w+)'.*", entry).group(1)
            time_str = re.match(r"(.*): Status set to", entry).group(1)
            time = datetime.strptime(time_str, "%a %b %d %H:%M:%S %Y")
            last_time = max(last_time, time)

            if entry_status == status:
                return time

        #return last_time

    @property
    def release(self):
        return self._orm.release_from_object(self)

    def __repr__(self):
        return "<{}({}, {})>".format(type(self).__name__, self.id, self.four_part_name)

    def __str__(self):
        return "<{}({}, {})>".format(type(self).__name__, self.id, self.four_part_name)

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)


class CcmBaseline(CcmObject):

    @property
    def tasks(self):
        return self.related_to('task_in_baseline')


class CcmReleaseDef(CcmObject):
    pass


class CcmProjectGrouping(CcmObject):
    pass


class CcmProcessRule(CcmObject):

    @property
    def release(self):
        return self.related_from('pr_in_release')[0]

    @property
    def folders(self):
        return self.related_to('folder_in_rpt')

    @property
    def folder_templates(self):
        return self.related_to('folder_template_in_rpt')


class CcmProject(CcmObject):

    @property
    def baseline_project(self):
        baseline_projects = self.related_to('baseline_project')
        if not baseline_projects:
            return None
        if len(baseline_projects) > 1:
            raise Exception("Multiple baseline projects found")

        return baseline_projects[0]
    
    @property
    def baseline(self):
        baselines = self.related_from('project_in_baseline')
        if not baselines:
            return

        return baselines[0]

    @property
    def structure(self):
        structure = {}

        # build top level
        children = self._orm.bound_children(self, self)
        for child in children:
            structure[child] = '/' + child.name

        # recurse over all children
        work = list(children)
        while work:
            current = work.pop()
            if current.type != 'dir':
                continue

            children = self._orm.bound_children(self, current)
            for child in children:
                structure[child] = structure[current] + '/' + child.name

            work += children

        return structure

    @property
    def tasks(self):
        return self.related_to('task_in_rp')

    @property
    def folders(self):
        return self.related_to('folder_in_rp')


class CcmFolder(CcmObject):

    @property
    def projects(self):
        return self.related_from('folder_in_rp')

    @property
    def tasks(self):
        return self.related_to('task_in_folder')


class CcmFolderTemplate(CcmObject):
    pass


class CcmProblem(CcmObject):
    pass


class CcmTask(CcmObject):

    @property
    def completed_time(self):
        return self.status_time('completed')

    @property
    def associated_objects(self):
        return [obj for obj in self.related_to('associated_cv')]


class CcmDirectory(CcmObject):

    @property
    def integrate_time(self):
        return self.status_time('integrate')

    @property
    def contents(self):
        contents = self._orm.contents_dir(self)
        return set(contents)

    @property
    def tasks(self):
        return self.related_from('associated_cv')


class CcmFile(CcmObject):

    @property
    def integrate_time(self):
        return self.status_time('integrate')

    @property
    def tasks(self):
        return self.related_from('associated_cv')

    @property
    def data(self):
        source = ccm_utils.deserialize_textval(self['source'])
        if not source:
            raise CcmError("No data")

        lines = source.split('\n')
        type_, version, archive_path = lines[0], lines[1], lines[2]
        backup_path = self._orm._db.backup_path
        path = os.path.join(backup_path, 'st_root', archive_path)
        if not os.path.exists(path):
            raise Error("File not found in backup archive: " + path)
        if type_ == 'ccm_delta':
            archive_reader = CcmArchiveReader(path)
            buffer = archive_reader.extract(version)
            return buffer
        else:
            cmd = ['rcs', 'co', '-p' + version, path]
            sp = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = sp.communicate()
            ret_code = sp.returncode
            if ret_code != 0:
                raise Exception("Error calling: " + ' '.join(cmd))
            return out
