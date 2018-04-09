#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import ccm_backup_reader.ccm_utils as ccm_utils


class TestEscaping:

    def test_unescape_text_ol_1(self):
        assert ccm_utils.unescape_text_ol("')a") == "\ta"

    def test_unescape_text_ol_2(self):
        assert ccm_utils.unescape_text_ol('ol1,`b"``&') == "ol1,…"

    def test_unescape_text_ol_3(self):
        assert ccm_utils.unescape_text_ol('ol1,`b"`"|') == "ol1,“"

    def test_unescape_text_ol_4(self):
        assert ccm_utils.unescape_text_ol("ol1,`C`+") == "ol1,ë"


    def test_unescape_text_1(self):
        assert ccm_utils.unescape_text("a\*b") == "a\nb"

    #def test_unescape_text_2(self):
    #    assert ccm_utils.unescape_text("a\\*b") == "a\*b"

    def test_unescape_text_3(self):
        assert ccm_utils.unescape_text("a\\nb") == "a\\nb"
