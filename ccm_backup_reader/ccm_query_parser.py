#!/usr/bin/env python2


from parsimonious import Grammar
from parsimonious import NodeVisitor

from ccm_backup_reader import ccm_utils


ccm_query_grammar = Grammar(
"""
    query = or_term
    or_term = paren_l? and_term paren_r? _ (or _ paren_l? and_term paren_r?)*
    and_term = paren_l? not_term paren_r? _ (and _ paren_l? not_term paren_r?)*
    not_term = not? term
    paren_l = "(" _
    paren_r = ")" _
    or = "or" _
    and = "and" _
    not = "not" _

    term = function_call / attribute_match
    function_call = identifier "(" _ string _ ("," _ string _)* ")" _
    attribute_match = identifier _ comparator _ atom _
    comparator = "=" / "match"

    atom = identifier / string
    identifier = ~"[a-z_]+"i _
    string = ~"'[^']*'"i _
    _ = ' '*
""")


ATTRIBUTE_TABLE = {
    'cvid': 'cv.cvid',
    'objectname': 'objectname',
    'name': 'cv.name',
    'version': 'cv.version',
    'instance': 'cv.subsystem',
    'type': 'cv.cvtype',
    'owner': 'cv.owner',
    'create_time': 'cv.create_time',
    'status': 'ccm_status(attrib.textval)',
}


def descendants_with_expr_name(node, expr_name):
    descendants = []
    for child in node.children:
        if child.expr_name == expr_name:
            descendants.append(child)

        child_descendants = descendants_with_expr_name(child, expr_name)
        descendants.extend(child_descendants)
    return descendants


class SqlQueryBuilder:

    COMPVER_COLUMNS = [
        'cvid',
        'objectname',
        'name',
        'version',
        'instance',
        'type',
        'owner',
        'create_time',
        'status',
    ]

    def __init__(self, delim):
        self._delim = delim

    def build(self, ccm_query):
        node = ccm_query_grammar.parse(ccm_query)

        visitor = SqlQueryBuilderVisitor(self._delim)
        visitor.visit(node)

        return visitor.sql_query


class SqlQueryBuilderVisitor(NodeVisitor):

    def __init__(self, delim):
        self._delim = delim
        self.sql_query = \
            "SELECT cv.id AS cvid, cv.name || '" + delim + "' || cv.version || ':' || cv.cvtype || ':' || cv.subsystem AS objectname, cv.name, cv.version, cv.subsystem AS instance, cv.cvtype AS type, cv.owner, cv.create_time, ccm_status(attrib.textval) AS status " + \
            "FROM compver cv LEFT JOIN attrib ON (cv.id = attrib.is_attr_of) " + \
            "WHERE attrib.name = 'status_log' AND "

    def visit_paren_l(self, node, visited_nodes):
        self.sql_query += "("
        return node

    def visit_paren_r(self, node, visited_nodes):
        self.sql_query += ")"
        return node

    def visit_or(self, node, visited_nodes):
        self.sql_query += " OR "
        return node

    def visit_and(self, node, visited_nodes):
        self.sql_query += " AND "
        return node

    def visit_not(self, node, visited_nodes):
        self.sql_query += " NOT "
        return node

    def visit_attribute_match(self, node, visited_nodes):
        identifier = node.children[0].text.rstrip()
        op = node.children[2].text.rstrip()
        value = node.children[4].text.rstrip()

        identifier = ATTRIBUTE_TABLE.get(identifier, identifier)
        sql_op = 'like' if op == 'match' else op
        value = value.repalce("*", "%") if op == 'match' else value

        self.sql_query += "{} {} {}".format(identifier, sql_op, value)

        return node

    def visit_function_call(self, node, visited_nodes):
        function = node.children[0].text.rstrip()
        args = [n.text.rstrip()[1:-1] for n in descendants_with_expr_name(node, 'string')]

        if function == 'is_successor_of':
            fpn = ccm_utils.parse_fpn(args[0], self._delim)
            self.sql_query += ("cv.id = (" + \
                "SELECT relate.to_cv " + \
                "FROM compver INNER JOIN relate ON (compver.id = relate.from_cv) " + \
                "WHERE compver.name = '{name}' AND compver.version = '{version}' AND compver.cvtype = '{type}' AND compver.subsystem = '{instance}' AND " + \
                      "relate.name = 'successor'" + \
                ")").format(**fpn)
        elif function == 'is_predecessor_of':
            fpn = ccm_utils.parse_fpn(args[0], self._delim)
            self.sql_query += ("cv.id = (" + \
                "SELECT relate.from_cv " + \
                "FROM compver INNER JOIN relate ON (compver.id = relate.to_cv) " + \
                "WHERE compver.name = '{name}' AND compver.version = '{version}' AND compver.cvtype = '{type}' AND compver.subsystem = '{instance}' AND " + \
                      "relate.name = 'successor'" + \
                ")").format(**fpn)
        elif function == 'is_child_of':
            fpn = ccm_utils.parse_fpn(args[0], self._delim)
            project_fpn = ccm_utils.parse_fpn(args[1], self._delim)
            self.sql_query += ("cv.id IN (" + \
                "SELECT bind.has_child " + \
                "FROM bind INNER JOIN compver cv1 ON (bind.has_asm = cv1.id) INNER JOIN compver cv2 on (bind.has_parent = cv2.id) " + \
                "WHERE cv1.name = '{0[name]}' AND cv1.version = '{0[version]}' AND cv1.cvtype = '{0[type]}' AND cv1.subsystem = '{0[instance]}' AND " + \
                      "cv2.name = '{1[name]}' AND cv2.version = '{1[version]}' AND cv2.cvtype = '{1[type]}' AND cv2.subsystem = '{1[instance]}'" + \
                ")").format(project_fpn, fpn)
        elif function == 'is_member_of':
            fpn = ccm_utils.parse_fpn(args[0], self._delim)
            self.sql_query += ("cv.id IN (" + \
                "SELECT cv2.id " + \
                "FROM compver cv1 INNER JOIN bind ON (cv1.id = bind.has_asm) INNER JOIN compver cv2 ON (bind.has_child = cv2.id) " + \
                "WHERE cv1.name = '{name}' AND cv1.version = '{version}' AND cv1.cvtype = '{type}' AND cv1.subsystem = '{instance}'" + \
                ")").format(**fpn)
        elif function == 'has_member':
            fpn = ccm_utils.parse_fpn(args[0], self._delim)
            self.sql_query += ("cv.id IN (" + \
                "SELECT cv1.id " + \
                "FROM bind INNER JOIN compver cv1 ON (bind.has_asm = cv1.id) INNER JOIN compver cv2 ON (bind.has_child = cv2.id) " + \
                "WHERE cv2.name = '{name}' AND cv2.version = '{version}' AND cv2.cvtype = '{type}' AND cv2.subsystem = '{instance}'" + \
                ")").format(**fpn)
        elif function == 'is_baseline_project_of':
            fpn = ccm_utils.parse_fpn(args[0], self._delim)
            self.sql_query += ("cv.id = (" + \
                "SELECT relate.to_cv " + \
                "FROM compver INNER JOIN relate ON (compver.id = relate.from_cv) " + \
                "WHERE compver.name = '{name}' AND compver.version = '{version}' AND compver.cvtype = '{type}' AND compver.subsystem = '{instance}' AND " + \
                      "relate.name = 'baseline_project'" + \
                ")").format(**fpn)
        elif function == 'has_baseline_project':
            fpn = ccm_utils.parse_fpn(args[0], self._delim)
            self.sql_query += ("cv.id IN (" + \
                "SELECT relate.from_cv " + \
                "FROM relate INNER JOIN compver ON (relate.to_cv = compver.id) " + \
                "WHERE compver.name = '{name}' AND compver.version = '{version}' AND compver.cvtype = '{type}' AND compver.subsystem = '{instance}' AND " + \
                      "relate.name = 'baseline_project'"
                ")").format(**fpn)
        else:
            raise NotImplementedError()

        return node

    def generic_visit(self, node, visited_nodes):
        return node
