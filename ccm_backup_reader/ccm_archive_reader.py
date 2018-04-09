#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import xml.etree.ElementTree as ET
import zipfile

from io import BytesIO


class XDeltaApplier:

    @staticmethod
    def apply(source_fd, target_fd, patch_fd):
        while True:
            cmd = patch_fd.read(1)
            if len(cmd) == 0:
                return
            cmd = cmd[0]

            copy_from_source = (cmd & 128) != 0
            length = XDeltaApplier._read_a(cmd, patch_fd);
            if copy_from_source:
                offset = XDeltaApplier._read_b(patch_fd)
                while length != 0:
                    # copy from source to target
                    source_fd.seek(offset)
                    b = source_fd.read(length)
                    target_fd.write(b)

                    length -= len(b)
                    offset += len(b)
            else:
                while length != 0:
                    b = patch_fd.read(length)
                    target_fd.write(b)
                    length -= len(b)

    @staticmethod
    def _read_a(start, fd):
        number = 0
        bit_count = 0

        while True:
            if bit_count > 62:
                raise Exception("Invalid state")

            if bit_count == 0:
                number = start & 63
                if (start & 64) == 0:
                    break

                bit_count = 6
            else:
                read = fd.read(1)[0]
                number |= (read & 127) << bit_count
                if (read & 128) == 0:
                    break

                bit_count += 7

        return number

    @staticmethod
    def _read_b(fd):
        """ read a long """
        number = 0
        bit_count = 0

        while True:
            read = fd.read(1)[0]

            if bit_count == 56:
                number |= (read << 56)
                break

            number |= (read & 127) << bit_count
            if (read & 128) == 0:
                break

            bit_count += 7

        return number


class CcmArchiveReader:

    def __init__(self, archive):
        self._archive = archive
        self._zip_file = zipfile.ZipFile(archive, 'r')

        metadata_text = self._zip_file.read('META-INF/ARCHIVE-HEADER')
        self._metadata = ET.fromstring(metadata_text)

    def extract(self, version):
        entry = self._find_top_entry()
        data = self._extract_top_version()

        while True:
            full_name = entry['fullName']
            if full_name == version:
                return data

            entry, data = self._extract_predecessor(full_name, data)

    def _find_top_entry(self):
        for entry in self._metadata.findall("entry"):
            if entry.find('predecessor') is not None:
                continue

            return self._entry_to_dict(entry)

    def _find_entry(self, full_name):
        for entry in self._metadata.findall("entry"):
            entry_full_name = entry.find('fullName').text
            if entry_full_name != full_name:
                continue

            return self._entry_to_dict(entry)

    def _find_entry_with_predecessor(self, predecessor):
        for entry in self._metadata.findall("entry"):
            if entry.find('predecessor') is None:
                continue

            entry_predecessor = entry.find('predecessor').text
            if entry_predecessor != predecessor:
                continue

            return self._entry_to_dict(entry)

    def _entry_to_dict(self, entry):
        d = {}
        for child in entry:
            key = child.tag
            value = child.text
            d[key] = value
        return d

    def _extract_top_version(self):
        entry = self._find_top_entry()
        full_name = entry['fullName']
        return self._extract_version(full_name)

    def _extract_version(self, full_name):
        return self._zip_file.read(full_name)

    def _extract_predecessor(self, version, data):
        # get metadata
        entry = self._find_entry_with_predecessor(version)
        delta_format = entry['deltaFormat']
        if delta_format != 'XDELTA':
            raise NotImplementedError("Don't know how to handle format: " + delta_format)

        # get patch
        full_name = entry['fullName']
        patch = self._extract_version(full_name)

        # apply patch
        source_fd = BytesIO(data)
        target_fd = BytesIO()
        patch_fd = BytesIO(patch)
        XDeltaApplier.apply(source_fd, target_fd, patch_fd)
        target_fd.seek(0)
        return entry, target_fd.read()
