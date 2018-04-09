# -*- coding: utf-8 -*-

import argparse
import os
import subprocess
import sys
import tempfile

from ccm_backup_reader.commands.ccm_command import CcmCommand
from ccm_backup_reader.commands.ccm_command_error import CcmCommandError


class CcmDiffError(CcmCommandError):
    pass


class CcmDiff(CcmCommand):

    def _init_arg_parser(self):
        parser = argparse.ArgumentParser(description='ccm diff')
        parser.add_argument('object_spec_1')
        parser.add_argument('object_spec_2')
        return parser

    def _diff_dir(self, fpn_1, fpn_2):
        contents_1 = self._db.contents_dir(fpn_1)
        contents_2 = self._db.contents_dir(fpn_2)

        desc_1, tf_1 = tempfile.mkstemp()
        fd_1 = os.fdopen(desc_1, 'w')
        desc_2, tf_2 = tempfile.mkstemp()
        fd_2 = os.fdopen(desc_2, 'w')

        try:
            fd_1.write('\n'.join(contents_1) + '\n')
            fd_2.write('\n'.join(contents_2) + '\n')
        finally:
            fd_1.close()
            fd_2.close()

        out = ''
        try:
            cmd = ['diff', tf_1, tf_2]
            sp = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = sp.communicate()
            ret_code = sp.returncode
        finally:
            os.unlink(tf_1)
            os.unlink(tf_2)

        fp = os.fdopen(sys.stdout.fileno(), 'wb')
        fp.write(out)

    def _diff_other(self, fpn_1, fpn_2):
        fpn_1 = self._args.object_spec_1
        source_1 = self._db.attr(fpn_1, 'source')
        if not source_1:
            raise CcmDiffError("Object not found: " + fpn_1)

        fpn_2 = self._args.object_spec_2
        source_2 = self._db.attr(fpn_2, 'source')
        if not source_2:
            raise CcmDiffError("Object not found: " + fpn_2)

        # parse results
        lines = source_2.split('\n')
        if lines[0] != 'ccm_rcs':
            raise CcmDiffError("Not ccm_rcs, don't know how to handle this:\n" + source_2)
        version = lines[1]
        path = lines[2]

        # ensure file exists
        backup_path = self._db.backup_path
        path = os.path.join(backup_path, 'st_root', path)
        if not os.path.exists(path):
            raise CcmDiffError("File not found in backup archive: " + path)

        # cat file and pipe to stdout
        cmd = ['rcsdiff', '-q', '-r' + rev1, '-r' + rev2, path]
        sp = subprocess.Popen(cmd, stderr=subprocess.PIPE)
        out, err = sp.communicate()
        ret_code = sp.returncode
        if ret_code != 0:
            raise ccmdifferror("error calling 'rcsdiff'")
        print(out)


    def run(self):

        # get source attrib for fpns
        fpn_1 = self._args.object_spec_1
        fpn_2 = self._args.object_spec_2
        type_1 = self._db.attr(fpn_1, 'cvtype')
        if not type_1:
            raise CcmDiffError("Object not found: " + fpn_1)

        if type_1 == 'dir':
            self._diff_dir(fpn_1, fpn_2)
        else:
            self._diff_other(fpn_1, fpn_2)
