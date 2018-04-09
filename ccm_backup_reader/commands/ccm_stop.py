# -*- coding: utf-8 -*-

import argparse

from ccm_backup_reader.commands.ccm_command import CcmCommand


class CcmStop(CcmCommand):

    def _init_arg_parser(self):
        parser = argparse.ArgumentParser(description='ccm stop')
        return parser

    def run(self):
        pass
