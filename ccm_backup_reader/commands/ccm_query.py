# -*- coding: utf-8 -*-

import argparse
import re

from ccm_backup_reader.commands.ccm_command import CcmCommand


class CcmQuery(CcmCommand):

    def _init_arg_parser(self):
        parser = argparse.ArgumentParser(description='ccm query')
        parser.add_argument('-f', '--format', default='%objectname')
        parser.add_argument('-nf', '--noformat', action='store_true')
        parser.add_argument('-u', '--unnumbered', action='store_true')
        parser.add_argument('query')
        return parser

    def run(self):
        ccm_query = self._args.query
        rows = self._db.query(ccm_query)

        # upgrade pattern to something usable
        pattern = re.compile('%([a-z]+)')
        format = self._args.format
        format = pattern.sub(lambda m: "{" + m.group(1) + "}", format)

        # print results
        for idx, row in enumerate(rows):
            out = format.format(**row)
            if self._args.unnumbered:
                print("{}".format(out))
            else:
                print("{}) {}".format(idx + 1, out))
