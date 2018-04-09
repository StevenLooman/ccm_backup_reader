# -*- coding: utf-8 -*-

import argparse

from ccm_backup_reader.commands.ccm_command import CcmCommand


class CcmFinduse(CcmCommand):

    def _init_arg_parser(self):
        parser = argparse.ArgumentParser(description='ccm finduse')
        parser.add_argument('-task', action='store_true')
        parser.add_argument('-released_proj', action='store_true')
        parser.add_argument('object_spec')
        return parser

    def run(self):
        fpn = self._args.object_spec
        predicate = {}

        if self._args.task:
            if self._args.released_proj:
                predicate = {'status': 'released'}

            task_number = self._db.attr(fpn, 'task_number')
            task_synopsis = self._db.attr(fpn, 'task_synopsis')
            print("Task {}: {}".format(task_number, task_synopsis))

            results = self._db.finduse_task(fpn)
            print('\tProjects:')
            for row in results:
                if not self.satisfies_predicate(row, predicate):
                    continue

                fpn = row['objectname']
                print("\t\t{}".format(fpn))
        else:
            raise NotImplementedError()

    def satisfies_predicate(self, row, predicate):
        for key, value in predicate.items():
            if row.get(key) != value:
                return False
        return True
