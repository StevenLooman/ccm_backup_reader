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


def write_project_commit(fd, project, mark, commit_marks, file_marks):
    structure = project.structure


def write_task_commit(fd, task, mark, commit_marks, file_marks):
    pass


def diff_project_structure(project_a, project_b):
    # objects and partial names
    project_a_objects = project_a.structure.keys()
    project_b_objects = project_b.structure.keys()
    project_a_part_names = {obj.part_name: obj for obj in project_a_objects}
    project_b_part_names = {obj.part_name: obj for obj in project_b_objects}

    # determine updated objects, match on partial_name
    a_names = set(project_a_part_names.keys())
    b_names = set(project_b_part_names.keys())
    updated_names = a_names.intersection(b_names)
    updated = [
        [project_a_part_names[part_name], project_b_part_names[part_name]]
        for part_name in updated_names
        if project_a_part_names[part_name] != project_b_part_names[part_name]
    ]

    # determine newly added objects
    added_names = b_names - a_names
    added = [project_b_part_names[name] for name in added_names]

    # determine removed objects
    removed_names = a_names - b_names
    removed = [project_a_part_names[name] for name in removed_names]

    # determine unchanged
    unchanged = set(project_a_objects).intersection(project_b_objects)

    return updated, added, removed, unchanged


def versions_between_objects(object_from, object_to):
    """
    Get all versions between two objects.
    Excludes given versions.
    """
    # XXX TODO: not really the way, to be fair
    timestamp_from = object_from.status_time('integrate')
    timestamp_to = object_to.status_time('integrate')

    # XXX TODO: what about parallel versions, if one is release and the other not?
    #           how do we known which object is in the release, at all? we don't
    return [obj
            for obj in RecursiveIterator(object_from, 'successors')
            if obj.status == 'integrate' and timestamp_from < obj.status_time('integrate') < timestamp_to]


def versions_between_projects(objects, project_from, project_to):
    timestamp_from = project_from.status_time('integrate')
    timestamp_to = project_to.status_time('integrate')
    return [obj
            for obj in objects
            if obj.status == 'integrate' and timestamp_from < obj.status_time('integrate') < timestamp_to]


def task_in_project(project, task):
    # 1: in reconfigure_properties of project
    if task in project.tasks:
        return True
    for folder in project.folders:
        if task in folder.tasks:
            return True

    # 2: task in baseline of project
    baseline = project.baseline
    if baseline:
        if task in baseline.tasks:
            return True


def get_project_chain(end_project):
    projects = []
    current = end_project
    while current:
        projects.append(current)
        current = current.baseline_project
    projects.reverse()
    return projects


def version_at_timestamp(timestamp, objects):
    filtered = [o for o in objects if o.status == 'integrate' or o.status == 'released']
    time_sorted = sorted(filtered, key=lambda o: o.status_time('integrate'))

    #if objects and objects[0].id == 238158:
    #    import ipdb; ipdb.set_trace()

    # object with timestamp itself?
    for obj in time_sorted:
        if obj.status_time('integrate') == timestamp:
            return obj

    # determine nearest index before timestamp
    idx_before = None
    for idx, obj in enumerate(time_sorted):
        if obj.status_time('integrate') < timestamp:
            idx_before = idx
        else:
            break

    # determine nearest index after timestamp
    idx_after = None
    for idx, obj in reversed(list(enumerate(time_sorted))):
        if obj.status_time('integrate') > timestamp:
            idx_after = idx
        else:
            break

    # prefer before
    if idx_before is not None:
        return time_sorted[idx_before]
    if idx_after is not None:
        return time_sorted[idx_after]


def expand_directory_changes(src_object, old_dir, new_dir):
    objects = []

    orm = src_object._orm
    old_contents = set(old_dir.contents) if old_dir else set()
    new_contents = set(new_dir.contents) if new_dir else set()

    added_names = new_contents - old_contents
    removed_names = old_contents - new_contents

    timestamp = src_object.status_time('integrate')
    # expand added to all objects between baseline_project.timestamp to project.timestamp
    for added_name in added_names:
        potentials = orm.objects_by_partial_name(added_name)
        version_at_add = version_at_timestamp(timestamp, potentials)
        if version_at_add:
            objects += [version_at_add]
        else:
            print('Could not find any satisfying version for add for %s' % (added_name))

        if version_at_add.type == 'dir':
            recursed = expand_directory_changes(src_object, None, version_at_add)
            objects += recursed

    # expand remove to all objects between baseline_project.timestamp to project.timestamp
    for removed_name in removed_names:
        potentials = orm.objects_by_partial_name(removed_name)
        version_at_remove = version_at_timestamp(timestamp, potentials)
        if version_at_remove:
            objects += [version_at_remove]
        else:
            print('Could not find any satisfying version for remove for %s' % (removed_name))

        if version_at_remove.type == 'dir':
            recursed = expand_directory_changes(src_object, version_at_remove, None)
            objects += recursed

    return objects



def get_touched_objects(baseline_project, project):
    updated_objects, added_objects, removed_objects, untouched_objects = diff_project_structure(baseline_project, project)

    baseline_project_timestamp = baseline_project.status_time('integrate') or baseline_project.status_time('released')
    project_timestamp = project.status_time('integrate') or project.status_time('released')
    project_structure = project.structure

    # updated: check all successor versions from baseline_porject
    # removed: find parent-dir entry when removed, include all versions until that update (till this project)
    # added: find parent-dir entry when added, include all version from that update (till this project)
    touched_objects = []
    expanded_objects = {}

    # updated
    for object_from, object_to in updated_objects:
        objects = versions_between_objects(object_from, object_to) + [object_to]
        touched_objects += objects

        if object_from.type == 'dir':
            a, b = itertools.tee([object_from] + objects)
            next(b, None)
            for old_dir, new_dir in zip(a, b):
                if not old_dir or not new_dir:
                    continue
                dir_objects = expand_directory_changes(object_to, old_dir, new_dir)
                touched_objects += dir_objects

                # store additional changes, which might not be recorded in task
                if object_to not in expanded_objects:
                    expanded_objects[object_to] = set()
                current = expanded_objects[object_to]
                expanded_objects[object_to] = current.union(set(dir_objects))

    # added, all successors after adding including added object
    for added_object in added_objects:
        objs = [
            obj
            for obj in RecursiveIterator(added_object, 'successors')
            if obj.status == 'integrated' and baseline_project_timestamp < obj.status_time('integrated') < project_timestamp]
        objects = [added_object] + objs
        touched_objects += objects

        if added_object.type == 'dir':
            dir_objects = expand_directory_changes(added_object, None, added_object)
            touched_objects += dir_objects

            if added_object not in expanded_objects:
                expanded_objects[added_object] = set()
            current = expanded_objects[added_object]
            expanded_objects[added_object] = current.union(set(dir_objects))

    # removed, all predecessors before removal excluding removed object
    for removed_object in removed_objects:
        objects = [
            obj
            for obj in RecursiveIterator(removed_object, 'predecessors')
            if obj.status == 'integrated' and baseline_project_timestamp < obj.status_time('integrated') < project_timestamp]
        touched_objects += objects

        if removed_object.type == 'dir':
            dir_objects = expand_directory_changes(removed_object, removed_object, None)
            touched_objects += dir_objects

            if removed_object not in expanded_objects:
                expanded_objects[removed_object] = set()
            current = expanded_objects[removed_object]
            expanded_objects[removed_object] = current.union(set(dir_objects))

    return set(touched_objects), expanded_objects


def get_tasks_from_objects(project, objects):
    project_structure = project.structure
    tasks_objects = {}

    for obj in objects:
        # get path
        path = None
        for obj in [obj] + list(RecursiveIterator(obj, 'successors')):
            if obj in project_structure:
                path = project_structure[obj]
                break
        else:
            print('Could not find path for: %s' % obj)

        # get task
        task_found = False
        for task in obj.tasks:
            if not task_in_project(project, task):
                print('%s not using %s, via %s' % (project.four_part_name, task.four_part_name, obj.four_part_name))
                continue

            if task not in tasks_objects:
                tasks_objects[task] = {}

            tasks_objects[task][obj] = path
            task_found = True

        if not task_found:
            print('No task for object: %s' % obj)

    return tasks_objects


def main():
    db = CcmDb("/cygdrive/d/nobackup/gnr")
    orm = CcmOrm(db)

    end_project = orm.object_by_fpn('dummy~current:project:1')
    print('Using project as end project: {}'.format(end_project))

    # Approach:
    # 1. get all projects, in order
    # 2. for each project
    #    a. get baseline project, do commit of it
    #    b. get all removed objects, added objects in this project
    #    c. for all removed/added objects (step b), get history between projects
    #    d. for all objects (step c), determine tasks

    # 1. get all projects, in order
    projects = get_project_chain(end_project)

    # 2. for each project
    for project in projects:
        baseline_project = project.baseline_project
        if not baseline_project:
            continue

        print('Project: %s' % project)
        print('Baseline project: %s' % baseline_project)

        # a. get baseline project, do commit of it
        #write_project_commit(baseline_project)

        # b. get touched objects
        touched_objects, expanded_objects = get_touched_objects(baseline_project, project)

        # c. for all changes, get tasks and path for object
        tasks_objects = get_tasks_from_objects(project, touched_objects)

        # e. for all tasks, do a commit
        sorted_tasks = sorted(tasks_objects.keys(), key=lambda task: task.status_time('completed'))
        for task in sorted_tasks:
            print(task)
            pprint(tasks_objects[task])
            #write_task_commit(fd, task, mark, commit_marks, file_marks):
            print()
        print()


if __name__ == '__main__':
    main()
