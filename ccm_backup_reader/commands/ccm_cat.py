# -*- coding: utf-8 -*-

import argparse
import os.path
import re
import subprocess
import sys

from ccm_backup_reader.ccm_archive_reader import CcmArchiveReader
from ccm_backup_reader.commands.ccm_command import CcmCommand
from ccm_backup_reader.commands.ccm_command_error import CcmCommandError


class CcmCatError(CcmCommandError):
    pass


class CcmCat(CcmCommand):

    def _init_arg_parser(self):
        parser = argparse.ArgumentParser(description='ccm cat')
        parser.add_argument('file_spec')
        return parser

    def _read_ccm_rcs(self, rcs_file, version):
        # ensure file exists
        backup_path = self._db.backup_path
        path = os.path.join(backup_path, 'st_root', path)
        if not os.path.exists(path):
            raise CcmCatError("File not found in backup archive: " + path)

        # cat file and pipe to stdout
        cmd = ['rcs', 'co', '-p' + version, path]
        sp = subprocess.Popen(cmd, stderr=subprocess.PIPE)
        out, err = sp.communicate()
        ret_code = sp.returncode
        if ret_code != 0:
            raise CcmCatError("Error calling 'rcs co'")
        print(out)

    def _read_ccm_delta(self, archive, version):
        # ensure file exists
        backup_path = self._db.backup_path
        path = os.path.join(backup_path, 'st_root', archive)
        if not os.path.exists(path):
            raise CcmCatError("File not found in backup archive: " + path)

        archive_reader = CcmArchiveReader(path)
        buffer = archive_reader.extract(version)

        fp = os.fdopen(sys.stdout.fileno(), 'wb')
        fp.write(buffer)

    def run(self):
        # get source attrib for fpn
        fpn = self._args.file_spec
        source = self._db.attr(fpn, 'source')
        if not source:
            raise CcmCatError("Object not found: " + fpn)

        # parse results
        lines = source.split('\n')
        version = lines[1]
        path = lines[2]
        if lines[0] == 'ccm_rcs':
            self._read_ccm_rcs(path, version)
        elif lines[0] == 'ccm_delta':
            self._read_ccm_delta(path, version)
        else:
            raise CcmCatError("Don't know how to handle this:\n" + source)
