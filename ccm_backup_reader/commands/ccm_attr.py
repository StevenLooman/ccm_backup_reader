# -*- coding: utf-8 -*-

import argparse

from ccm_backup_reader.commands.ccm_command import CcmCommand


class CcmAttr(CcmCommand):

    def _init_arg_parser(self):
        parser = argparse.ArgumentParser(description='ccm attr')
        parser.add_argument('-l', '--list', action='store_true')
        parser.add_argument('-s', '--show')
        parser.add_argument('object_spec')
        return parser

    def run(self):
        fpn = self._args.object_spec
        if self._args.show:
            attr_name = self._args.show
            self._show_attrs(fpn, attr_name)
        elif self._args.list:
            self._list_attrs(fpn)

    def _show_attrs(self, fpn, attr_name):
        attr_value = self._db.attr(fpn, attr_name)
        if attr_value:
            print(attr_value)

    def _list_attrs(self, fpn):
        attrs = self._db.attrs(fpn)
        for attr_name, data_type in attrs.items():
            print("{}\t{}\t{}".format(attr_name, data_type, 'local'))
