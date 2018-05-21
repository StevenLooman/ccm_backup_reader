#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import itertools
import operator
import os.path
import subprocess
import sys
import time
from collections import deque
from pprint import pprint

from ccm_backup_reader import ccm_utils
from ccm_backup_reader import CcmDb
from ccm_backup_reader.ccm_error import CcmError
from ccm_backup_reader.orm import CcmOrm


class RecursiveIterator:

    def __init__(self, ccm_object, relation):
        self._ccm_object = ccm_object
        self._relation = relation

        self._work = self._get_nexts(ccm_object)
        self._done = []

    def __iter__(self):
        return self

    def _get_nexts(self, item):
        return getattr(item, self._relation)

    def __next__(self):
        if not self._work:
            raise StopIteration()

        item = self._work.pop()
        if item in self._done:
            return self.__next__()
        self._done.append(item)
        self._work += self._get_nexts(item)

        return item


def task_changes(task, structure):
    changes = []
    task_structure = {}

    task_objects = [o for o in task.associated_objects if o.type != 'project']
    dir_objects = [o for o in task_objects if o.type == 'dir']
    task_names = {o.part_name: o for o in task_objects}
    structure_names = {o.part_name: o for o in structure.keys()}

    known_dirs = []
    for obj in dir_objects:
        # already known? a renamed object
        # XXX: should not use the old directory
        #if obj in structure:
        #    known_dirs.append(obj)
        #    task_structure[obj] = structure[obj]
        #    continue

        # any predecessors known?
        # there is no guarantuee that our predecessor has the same path!
        #   what if the dir was changed _and_ moved?
        for pred in RecursiveIterator(obj, 'predecessors'):
            if pred in structure:
                known_dirs.append(obj)
                task_structure[obj] = structure[pred]
                break

    # build directory structure in task, record adds/removed
    work = deque(known_dirs)
    while work:
        obj = work.popleft()
        if obj.type != 'dir':
            continue

        contents_cur = set(obj.contents)
        # XXX TODO: what if predecessor was never included in this project? we assume a linear version/project flow here...
        #           we should take the predecessor in structure, instead
        preds = [pred for pred in RecursiveIterator(obj, 'predecessors') if pred in structure]
        contents_preds = set(*[pred.contents for pred in preds])
        #contents_preds = set(*[pred.contents for pred in obj.predecessors])
        added = contents_cur - contents_preds
        removed = contents_preds - contents_cur

        # we are changed as well..
        if obj.predecessors:
            change = {'type': 'update', 'old': obj.predecessors[0], 'new': obj, 'time': obj.status_time('integrate')}
            changes.append(change)

        for name in added:
            added_obj = None
            if name in task_names:
                added_obj = task_names[name]
                task_structure[added_obj] = task_structure[obj] + '/' + added_obj.name
                work.append(added_obj)
            else:
                # might be a rename, try structure first
                if name in structure_names:
                    added_obj = structure_names[name]
                    task_structure[added_obj] = task_structure[obj] + '/' + added_obj.name
                    # XXX TODO: recurse all children
                else:
                    # XXX TODO:
                    import ipdb; ipdb.set_trace()

            if added_obj:
                change = {'type': 'add', 'object': added_obj, 'path': task_structure[added_obj], 'time': obj.status_time('integrate')}
                changes.append(change)
            else:
                print('missing added_obj')

        for name in removed:
            removed_obj = None
            if name in task_names:
                removed_obj = task_names[name]
                task_structure[removed_obj] = task_structure[obj] + '/' + removed_obj.name
                work.append(removed_obj)
            else:
                # might be a rename, try structure first
                if name in structure_names:
                    removed_obj = structure_names[name]
                    task_structure[removed_obj] = task_structure[obj] + '/' + removed_obj.name
                    # XXX TODO: recurse all children
                else:
                    # XXX TODO:
                    import ipdb; ipdb.set_trace()

            if removed_obj:
                change = {'type': 'delete', 'object': removed_obj, 'path': task_structure[removed_obj], 'time': obj.status_time('integrate')}
                changes.append(change)
            else:
                print('missing removed_obj')

    # XXX TODO: renames, merge add+delete into rename

    # record updates
    file_objects = [o for o in task_objects if o.type != 'dir']
    for obj in file_objects:
        if obj in task_structure:
            continue

        for pred in RecursiveIterator(obj, 'predecessors'):
            # XXX TODO: multiple predecessors?
            #           are we actually interested in the predecessors here?
            #           we should handle that during creation of commits
            #           but might also be important when handling merges?
            # prefer task_structure, in case a predecessor (in the same task) is already handled
            if pred in task_structure:
                task_structure[obj] = task_structure[pred]
                change = {'type': 'update', 'old': pred, 'new': obj, 'time': obj.status_time('integrate')}
                changes.append(change)
                break
            elif pred in structure:
                task_structure[obj] = structure[pred]
                change = {'type': 'update', 'old': pred, 'new': obj, 'time': obj.status_time('integrate')}
                changes.append(change)
                break
        else:
            print('missing predecessor?', obj)

    return changes


def apply_changes(task, changes, orig_structure):
    structure = orig_structure.copy()
    for change in changes:
        type_ = change['type']
        if type_ == 'update':
            old = change['old']
            new = change['new']
            structure[new] = structure[old]
            del structure[old]
        elif type_ == 'add':
            obj = change['object']
            path = change['path']
            structure[obj] = path
        elif type_ == 'delete':
            obj = change['object']
            del structure[obj]

    return structure


def commit_msg_for_file(ccm_object):
    msg = []

    msg += ['Synergy-object: {}'.format(ccm_object.four_part_name)]
    msg += []

    tasks = ccm_object.related_from('associated_cv')
    for task in tasks:
        task_synopsis = task['task_synopsis']
        task_number = task['task_number']
        resolver = task['resolver']

        msg += [task_synopsis]
        msg += ['Synergy-task-number: {}'.format(task_number)]
        msg += ['Synergy-resolver: {}'.format(resolver)]
        msg += ['Synergy-task-object: {}'.format(task.four_part_name)]

    return '\n'.join(msg)


def write_file_blob(fd, ccm_object, mark):
    try:
        data = ccm_object.data
    except CcmError:
        return False
    data_length = len(data)

    fd.write('# blob for {}\n'.format(ccm_object.four_part_name).encode('utf-8'))
    fd.write('blob\n'.encode('utf-8'))
    fd.write('mark :{}\n'.format(mark).encode('utf-8'))
    fd.write('data {}\n'.format(data_length).encode('utf-8'))
    fd.write(data)

    return True


def write_file_commit(fd, ccm_object, mark, commit_marks, file_mark=None, file_path=None):
    integrate_time = ccm_object.integrate_time
    timestamp = int(time.mktime(integrate_time.timetuple()))
    commit_msg = commit_msg_for_file(ccm_object)

    fd.write('# commit for {}\n'.format(ccm_object.four_part_name).encode('utf-8'))
    fd.write('commit refs/heads/master\n'.encode('utf-8'))
    fd.write('mark :{}\n'.format(mark).encode('utf-8'))
    fd.write('author Enexis <noreply@enexis.nl> {} +0000\n'.format(timestamp).encode('utf-8'))
    fd.write('committer Enexis <noreply@enexis.nl> {} +0000\n'.format(timestamp).encode('utf-8'))
    fd.write('data {}\n'.format(len(commit_msg)).encode('utf-8'))
    fd.write((commit_msg + '\n').encode('utf-8'))

    if commit_marks:
        fd.write('from :{}\n'.format(commit_marks[0]).encode('utf-8'))
        for merge_mark in commit_marks[1:]:
            fd.write('merge :{}\n'.format(merge_mark).encode('utf-8'))
    if file_mark:
        fd.write('M 100644 :{} {}\n'.format(file_mark, file_path).encode('utf-8'))


def write_task_commit(fd, task, mark):
    pass
    # XXX TODO


def write_project_commit(fd, project, mark, commit_marks, file_marks):
    structure = project.structure
    # XXX TODO


def diff_dict(dict_a, dict_b, report=False):
    keys_a_extra = set(dict_a.keys()) - set(dict_b.keys())
    keys_b_extra = set(dict_b.keys()) - set(dict_a.keys())
    keys_intersect = set(dict_a.keys()).intersection(dict_b.keys())

    diff = False
    for key in keys_intersect:
        value_a = dict_a[key]
        value_b = dict_b[key]
        if value_a != value_b:
            diff = True
            if report:
                print(' {}: {} != {}'.format(key, value_a, value_b))

    for key in keys_a_extra:
        if report:
            value_a = dict_a[key]
            print('+{}: {}'.format(key, value_a))
        diff = True

    for key in keys_b_extra:
        if report:
            value_b = dict_b[key]
            print('-{}: {}'.format(key, value_b))
        diff = True

    if report:
        print('diff? {}'.format(diff))
    return not diff


def main():
    db = CcmDb("synergy_backup/")
    orm = CcmOrm(db)

    end_project = orm.object_by_fpn('dummy~current:project:1')
    print('Using project as end project: {}'.format(end_project))

    # Approach:
    # 1. get all projects, in order
    # 2. for each project
    #    a. get baseline project, do commit of it
    #    b. get all completed tasks in project, completed in the project
    #    c. replay all changes from tasks, do commit per file

    # 1. get all projects, in order
    projects = []
    current = end_project
    while current:
        projects.append(current)
        current = current.baseline_project
    projects.reverse()

    # 2. get all files for each project
    with open(sys.argv[1], 'wb') as fd:
        mark = 0
        commit_marks = {}
        file_marks = {}

        for project in projects:
            baseline_project = project.baseline_project
            if not baseline_project:
                continue
            print('Project: {}'.format(project))
            print('Baseline project: {}'.format(baseline_project))

            #project_baseline = project.related_from('project_in_baseline')[0]
            #baseline_project_baseline = baseline_project.related_from('project_in_baseline')[0]
            #print('project_baseline', project_baseline)
            #print('baseline_project_baseline', baseline_project_baseline)

            print('Including all tasks from project: {}'.format(project))

            # a. get baseline project, do a commit of it
            mark = write_project_commit(fd, baseline_project, mark, commit_marks, file_marks)

            # b. get all completed tasks in project, completed in the project
            project_tasks = project.tasks
            for folder in project.folders:
                project_tasks += folder.tasks

            time_from = baseline_project.status_time('integrate') or baseline_project.status_time('prep')
            time_to = project.status_time('integrate') or project.status_time('prep')
            #tasks = [task for task in project_tasks if task.status == 'completed' and time_from < task.status_time('completed') < time_to]
            #tasks = [task for task in project_tasks if task.status == 'completed']
            #tasks = project_baseline.related_to('task_in_baseline')
            tasks = [obj for obj in baseline_project.release.related_objects if obj.type == 'task' and obj.status == 'completed']

            # c. replay all changes from tasks, do commit per file
            structure = baseline_project.structure
            tasks = sorted(tasks, key=lambda t: t.status_time('completed'))
            for task in tasks:
                print('Including task: {}'.format(task))
                changes = task_changes(task, structure)
                pprint(changes)
                structure = apply_changes(task, changes, structure)

                # finish it with some sort of a tag/commit, recording integration of task
                mark = write_task_commit(fd, task, mark)

            # in the end, structure and structure_post should be the same
            structure_pre = baseline_project.structure
            structure_post = project.structure
            print('Diffing resulting structure with project structure')
            same = diff_dict(structure, structure_post, report=True)
            #if not same:
            #    print('diff')


if __name__ == '__main__':
    main()
