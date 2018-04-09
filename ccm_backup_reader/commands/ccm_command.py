# -*- coding: utf-8 -*-

class CcmCommand:

    def __init__(self, args, db):
        parser = self._init_arg_parser()
        self._args = parser.parse_args(args)
        self._db = db

    def run():
        raise NotImplementedError()
