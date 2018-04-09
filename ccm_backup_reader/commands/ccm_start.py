# -*- coding: utf-8 -*-

import argparse

from ccm_backup_reader.commands.ccm_command import CcmCommand


class CcmStart(CcmCommand):

    def _init_arg_parser(self):
        parser = argparse.ArgumentParser(description='ccm start')
        return parser

    def run(self):
        pass
