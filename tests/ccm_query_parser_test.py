#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from ccm_backup_reader.ccm_query_parser import SqlQueryBuilder


class TestQueryParser:

    def test_and(self):
        ccm_query = "name='name' and version='1' and type='ascii' and instance='1'"
        builder = SqlQueryBuilder('~')
        sql_query = builder.build(ccm_query)
        assert sql_query == "SELECT * FROM compver WHERE name = 'name' AND version = '1' AND type = 'ascii' AND instance = '1'"

    def test_or(self):
        ccm_query = "name='name' or version='1' or type='ascii' or instance='1'"
        builder = SqlQueryBuilder('~')
        sql_query = builder.build(ccm_query)
        assert sql_query == "SELECT * FROM compver WHERE name = 'name' OR version = '1' OR type = 'ascii' OR instance = '1'"

    def test_parens(self):
        ccm_query = "name='name' and (version='1' or version='2')"
        builder = SqlQueryBuilder('~')
        sql_query = builder.build(ccm_query)
        assert sql_query == "SELECT * FROM compver WHERE name = 'name' AND (version = '1' OR version = '2')"

    def test_function_call_is_successor_of(self):
        ccm_query = "is_successor_of('test.txt~7:ascii:1')"
        builder = SqlQueryBuilder('~')
        sql_query = builder.build(ccm_query)
        assert sql_query == \
"SELECT * " + \
"FROM compver " + \
"WHERE id = (" + \
    "SELECT to_cv " + \
    "FROM compver INNER JOIN relate ON (compver.id = relate.from_cv) " + \
    "WHERE compver.name = 'test.txt' AND compver.version = '7' AND compver.cvtype = 'ascii' AND compver.subsystem = '1' AND relate.name = 'successor'" + \
")"

    def test_function_call_is_predecessor_of(self):
        ccm_query = "is_predecessor_of('test.txt~7:ascii:1')"
        builder = SqlQueryBuilder('~')
        sql_query = builder.build(ccm_query)
        assert sql_query == \
"SELECT * " + \
"FROM compver " + \
"WHERE id = (" + \
    "SELECT from_cv " + \
    "FROM compver INNER JOIN relate ON (compver.id = relate.to_cv) " + \
    "WHERE compver.name = 'test.txt' AND compver.version = '7' AND compver.cvtype = 'ascii' AND compver.subsystem = '1' AND relate.name = 'successor'" + \
")"

    #def test_function_call_2(self):
    #    ccm_query = "is_child_of('test.txt~7:ascii:1', 'project_name~1:project:1')"
    #    builder = SqlQueryBuilder('~')
    #    sql_query = builder.build(ccm_query)
    #    assert sql_query == "SELECT * FROM compver INNER JOIN relate ON (id = relate.from_cv) WHERE name = 'test.txt' AND version = '7' AND cvtype = 'ascii' AND subsystem = '1'"




if __name__ == '__main__':
    TestQueryParser.test_function_call(None)
