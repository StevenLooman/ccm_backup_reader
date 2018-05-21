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
        # build entry-list to follow
        entries = self._find_entries_to_entry(version)
        entries.reverse()

        # read data from first entry
        data = self._extract_version(entries[0])
        entries = entries[1:]

        # keep applying patches
        for entry in entries:
            data = self._apply_version(entry, data)

        return data

    def _find_entries_to_entry(self, version):
        entry = self._find_entry(version)
        entries = [entry]
        while 'predecessor' in entry:
            predecessor = entry['predecessor']
            entry = self._find_entry(predecessor)
            entries.append(entry)
        return entries

    def _extract_version(self, entry):
        full_name = entry['fullName']
        return self._zip_file.read(full_name)

    def _apply_version(self, entry, data):
        patch = self._extract_version(entry)

        # apply patch
        source_fd = BytesIO(data)
        target_fd = BytesIO()
        patch_fd = BytesIO(patch)
        XDeltaApplier.apply(source_fd, target_fd, patch_fd)
        target_fd.seek(0)
        return target_fd.read()

    def _find_entry(self, full_name):
        for entry in self._metadata.findall("entry"):
            entry_full_name = entry.find('fullName').text
            if entry_full_name != full_name:
                continue

            return self._entry_to_dict(entry)

    def _entry_to_dict(self, entry):
        d = {}
        for child in entry:
            key = child.tag
            value = child.text
            d[key] = value
        return d

    def _find_entry_with_predecessor(self, predecessor):
        for entry in self._metadata.findall("entry"):
            if entry.find('predecessor') is None:
                continue

            entry_predecessor = entry.find('predecessor').text
            if entry_predecessor != predecessor:
                continue

            return self._entry_to_dict(entry)



    def _find_top_entry(self):
        last_entry = self._metadata.findall("entry")[-1]
        return self._entry_to_dict(last_entry)


    def _extract_top_version(self):
        entry = self._find_top_entry()
        full_name = entry['fullName']
        return self._extract_version(full_name)

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
