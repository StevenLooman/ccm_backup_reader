# -*- coding: utf-8 -*-

import argparse

from ccm_backup_reader.commands.ccm_command import CcmCommand


class CcmDelim(CcmCommand):

    def _init_arg_parser(self):
        parser = argparse.ArgumentParser(description='ccm delim')
        return parser

    def run(self):
        delim = self._db.delim()
        print(delim)
