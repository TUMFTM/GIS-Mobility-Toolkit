# -*- coding: utf-8 -*-
__author__ = "David Ziegler"
__copyright__ = "Copyright 2021, David Ziegler"
__credits__ = ["David Ziegler"]
__license__ = "MIT"
__version__ = "0.1"
__status__ = "Production"
__maintainer__ = "David Ziegler"
__email__ = "david.ziegler@tum.de"
__status__ = "Production"

import re, itertools
import sqlfluff
from collections import defaultdict, Iterable
from bs4 import BeautifulSoup
from more_itertools import unique_justseen
from sqlglot import parse_one, parse, exp
from sqlglot.dialects import postgres
from sqlglot.tokens import Tokenizer, TokenType
from sqlglot.trie import new_trie
from jsonpath_ng import jsonpath
from jsonpath_ng.ext import parse


def flatten(xs):
    for x in xs:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            yield from flatten(x)
        else:
            yield x


def remove_duplicates(k):
    k.sort()
    return list(k for k, _ in itertools.groupby(k))


class MapnikSqlParser(object):

    re_outer_clamps = re.compile(r"\(((?:[^()]*\([^()]*\))*[^()]*?)\)")
    re_color_hash = re.compile(r"#[0-9a-f]{3,6}")
    re_inside_clamps = re.compile(r"\((?P<content>[.\s\S]+)\)")
    re_way_replace = re.compile(
        r"(?P<content>[^\w\d](way,))(?=(?:(?!osm_id)(?:[\s\S]))*?(?:[^\w\d]way|$))"
    )

    def __init__(self, mapnik_file):

        class PSQL(postgres.Postgres):

            def __init__(self) -> None:
                super().__init__()

            class Tokenizer(postgres.Postgres.Tokenizer):

                def __new__(cls):
                    cls.KEYWORD_TRIE = new_trie(
                        key.upper() for key in {
                            **cls.KEYWORDS,
                            **{
                                comment: TokenType.COMMENT
                                for comment in cls._COMMENTS
                            },
                            **{
                                quote: TokenType.QUOTE
                                for quote in cls._QUOTES
                            },
                            **{
                                bit_string: TokenType.BIT_STRING
                                for bit_string in cls._BIT_STRINGS
                            },
                            **{
                                hex_string: TokenType.HEX_STRING
                                for hex_string in cls._HEX_STRINGS
                            },
                            **{
                                byte_string: TokenType.BYTE_STRING
                                for byte_string in cls._BYTE_STRINGS
                            },
                        }
                        if " " in key or any(single in key
                                             for single in cls.SINGLE_TOKENS))
                    return super().__new__(cls)

        self.mapnik_file = mapnik_file
        self.mapnik_info = {}
        self.mapnik_loaded = False

    def parse_mapnik_styles(self, styles):
        _style_mapper = defaultdict(list)
        _rule_collection = []
        for _style in styles:
            for _rule in _style.findAll("Rule"):
                _rule_properties = {
                    "MaxScale": None,
                    "MinScale": None,
                    "Filter": None,
                    "Styles": None
                }

                _styles = _rule.findChildren()

                #Find MaxScaleDenominator
                __filter = _rule.find("MaxScaleDenominator")
                if __filter is not None:
                    _rule_properties["MaxScale"] = int(__filter.text)
                    _styles = __filter.fetchNextSiblings()

                #Find MinScaleDenominator
                __filter = _rule.find("MinScaleDenominator")
                if __filter is not None:
                    _rule_properties["MinScale"] = int(__filter.text)
                    _styles = __filter.fetchNextSiblings()

                #Find Filter
                __filter = _rule.find("Filter")
                if __filter is not None:
                    _rule_properties["Filter"] = self.re_outer_clamps.findall(
                        __filter.text)
                    _rule_properties["Filter"] = [
                        _e for _e in _rule_properties["Filter"]
                        if "way_pixels" not in _e
                    ]
                    _styles = __filter.fetchNextSiblings()

                #Scan for styles
                _rule_properties["Styles"] = frozenset(
                    frozenset([_f.name])
                    | frozenset(self.re_color_hash.findall(str(_f)))
                    for _f in _styles)

                #Eliminate doublets
                _rule_collection.append(_rule_properties)

        #Sort and filter on unique
        _rule_collection = sorted(
            _rule_collection,
            key=lambda x: tuple([x["Filter"] or [], x["MaxScale"] or 0]))
        _rule_collection_unique = list(
            map(lambda x: (x["Filter"], x["MaxScale"]),
                unique_justseen(_rule_collection, key=lambda x: x["Filter"])))

        #Add rules
        for _rule in _rule_collection:
            if (_rule["Filter"], _rule["MaxScale"]) in _rule_collection_unique:
                _style_mapper[_rule["Styles"]].append(_rule["Filter"])

        return _style_mapper

    def sqlfluff_back_to_string(self, ast) -> str:
        actual_ast = ast['file']

        def bla(x):
            if isinstance(x, str):
                yield x
            elif isinstance(x, list):
                for l in x:
                    yield from bla(l)
            elif isinstance(x, dict):
                for k, v in x.items():
                    yield from bla(v)

        return "".join(bla(actual_ast))

    def remove_comments(self, sql):
        # Remove single-line comments
        sql = re.sub(r'--.*?\n', '\n', sql)
        # Remove multi-line comments
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        return sql

    def remove_outer_curly_bracket(self, expr):
        r = re.search(r'\((.*)\)', expr, re.DOTALL)
        return r.group(1) if r else expr

    def get_ast_select_parents(self, sql_ast, filter: str):
        all_select = parse(rf'$..select_statement[?(@..{filter})]').find(
            sql_ast)
        outer_select = parse(
            rf'$..select_statement[?(@..select_statement..{filter})]').find(
                sql_ast)
        inner_select = [
            a for a in all_select
            if a.id_pseudopath not in (o.id_pseudopath for o in outer_select)
        ]
        return inner_select, outer_select

    def remove_ast_section(self, sql_ast, filter_query, seperators=''):
        # Filter elements
        if isinstance(filter_query, str):
            filter_elements = parse(filter_query).find(sql_ast)
        else:
            filter_elements = filter_query

        # Filter groups
        contexts = {}
        filter_groups = defaultdict(list)
        for filter_element in filter_elements:
            contexts[str(
                filter_element.context.id_pseudopath)] = filter_element.context
            filter_groups[str(
                filter_element.context.id_pseudopath)].append(filter_element)

        for k, _ in filter_groups.items():
            filter_groups[k] += parse(seperators).find(contexts[k].value)
            filter_groups[k] = sorted(filter_groups[k],
                                      key=lambda x: x.path.index)

        remove = defaultdict(list)
        for k, _filter_elements in filter_groups.items():
            _context = contexts[k].value
            _prev_idx = 0
            for j, _filter_element in enumerate(_filter_elements):
                _idx = _context.index(_filter_element.value)
                _idx_next = (_context.index(_filter_elements[j + 1].value) if
                             (j +
                              1 < len(_filter_elements)) else len(_context))

                if _idx == _idx_next:
                    remove[k] += range(_prev_idx, len(_context))
                else:
                    _next_is_seperator = ((j + 1 < len(_filter_elements))
                                          and (_filter_elements[j + 1]
                                               not in filter_elements))
                    if _next_is_seperator:
                        remove[k] += range(_prev_idx, _idx_next + 1)
                    else:
                        remove[k] += range(_prev_idx, _idx_next)
                    _prev_idx = _idx_next

        for ctx, lst in remove.items():
            identifiers = []

            # Get identifiers that will be deleted and then delete them
            for idx in sorted(set(lst), reverse=True):
                identifiers += [
                    x.value for x in parse(
                        r'$.select_clause_element.*.naked_identifier').find(
                            contexts[ctx].value[idx])
                ]
                del contexts[ctx].value[idx]
            if len(contexts[ctx].value) == 0:
                for k in list(contexts[ctx].context.value.keys()):
                    del contexts[ctx].context.value[k]
        return sql_ast

    def replace_ast_section(self, sql_ast, filter_query, replacements: dict):
        # Filter elements
        if isinstance(filter_query, str):
            matches = parse(filter_query).find(sql_ast)
        else:
            matches = filter_query

        for m in matches:
            _path = str(m.path) if not isinstance(
                m.path, jsonpath.Index) else m.path.index
            m.context.value[_path] = replacements.get(str(m.value), m.value)

        return sql_ast

    def add_ast_select_section(self, sql_ast, filter_query, add_query,
                               elements):
        if isinstance(filter_query, str):
            filter_elements = parse(filter_query).find(sql_ast)
        else:
            filter_elements = filter_query
        for fe in filter_elements:
            for match in parse(add_query).find(fe.context.value):
                identifiers = [
                    x.value for x in parse(
                        r'$.select_clause_element.*.naked_identifier').find(
                            match.value)
                ]
                is_wildcard = (len(
                    parse(r'$.[*].select_clause_element.wildcard_expression').
                    find(match.value)) > 0)
                # If just a single select was given, extend to multi select
                if isinstance(match.value, dict):
                    for k, v in match.context.value.items():
                        if v == match.value:
                            match.context.value[k] = [{
                                _k: _v
                            } for _k, _v in match.value.items()]
                            match.value = match.context.value[k]
                            break
                for element in elements:
                    if isinstance(match.value, list):
                        if not is_wildcard and parse(
                                '$.select_clause_element.*.naked_identifier'
                        ).find(element)[0].value not in identifiers:
                            match.value += [{
                                'comma': ','
                            }, {
                                'whitespace': ' '
                            }, element]
        return sql_ast

    def parse_mapnik_sql(self, sql, label=None):
        """ 
        This parser parses the mapnik sql statements and changes them to be usable directly as materialized view inside the database, e.g., for deriving landuse
         """
        _sql = f"{self.remove_outer_curly_bracket(sql)}"
        _sql = self.remove_comments(_sql)
        _sql_ast = sqlfluff.parse(re.sub(r'!(.*?)!', r'__\1__', _sql),
                                  dialect="postgres")
        #Replace scale denominator against smallest to achieve highest resolution/include all elements
        _sql_ast = self.replace_ast_section(
            _sql_ast,
            r'$..select_clause..@[?(@.naked_identifier =~ ".*__[\\S]+__.*")].naked_identifier',
            replacements={'__scale_denominator__': "1"})
        #Remove "where" placeholder entries to not filter over a certain region or depending on the zoom factor, but to include all results
        _sql_ast = self.remove_ast_section(
            _sql_ast,
            r'$..where_clause.expression[?(@..naked_identifier =~ ".*__[\\S]+__.*")]',
            seperators=r'$.@[?(@.binary_operator =~ "AND|OR")]')

        #Add osm_id and osm_type to the query to differentiate between the different sources and to make sure that this information is existing fo further processing
        for _type in ('point', 'polygon'):
            inner_select, outer_select = self.get_ast_select_parents(
                _sql_ast,
                f'table_reference.naked_identifier = "planet_osm_{_type}"')
            _sql_ast = self.add_ast_select_section(
                _sql_ast, outer_select, r'$[*].select_clause', [{
                    'select_clause_element': {
                        'column_reference': {
                            'naked_identifier': 'osm_id'
                        }
                    }
                }, {
                    'select_clause_element': {
                        'column_reference': {
                            'naked_identifier': 'osm_type'
                        }
                    }
                }])
            _sql_ast = self.add_ast_select_section(
                _sql_ast, inner_select, r'$[*].select_clause', [{
                    'select_clause_element': {
                        'column_reference': {
                            'naked_identifier': 'osm_id'
                        }
                    }
                }, {
                    'select_clause_element': {
                        'column_reference': {
                            'quoted_literal': f"'{_type}'"
                        },
                        'whitespace': ' ',
                        'alias_expression': {
                            'keyword': 'AS',
                            'whitespace': ' ',
                            'naked_identifier': 'osm_type'
                        }
                    }
                }])
        #Inset building feature to consider living areas for amenity-filtering, which completes the landuse filtering approach.
        if label == "amenity-points":
            feature_end = parse(
                r'$..select_clause[?(@..alias_expression.naked_identifier = "feature")].select_clause_element[?(@..function_name_identifier = "COALESCE")].bracketed[?(@.end_bracket = ")")]'
            ).find(_sql_ast)
            if len(feature_end) > 0:
                additional_content = [{
                    'comma': ','
                }, {
                    'whitespace': ' '
                }, {
                    'newline': '\n'
                }, {
                    'expression': [{
                        'cast_expression': {
                            'quoted_literal': "'building_'",
                            'casting_operator': '::',
                            'data_type': {
                                'keyword': 'text'
                            }
                        }
                    }, {
                        'whitespace': ' '
                    }, {
                        'binary_operator': [{
                            'pipe': '|'
                        }, {
                            'pipe': '|'
                        }]
                    }, {
                        'newline': '\n'
                    }, {
                        'whitespace': ' '
                    }, {
                        'case_expression': [{
                            'keyword': 'CASE'
                        }, {
                            'newline': '\n'
                        }, {
                            'whitespace': ' '
                        }, {
                            'when_clause': [{
                                'keyword': 'WHEN'
                            }, {
                                'whitespace': ' '
                            }, {
                                'expression': [{
                                    'column_reference': [{
                                        'naked_identifier':
                                        '_'
                                    }, {
                                        'dot': '.'
                                    }, {
                                        'naked_identifier':
                                        'building'
                                    }]
                                }, {
                                    'whitespace': ' '
                                }, {
                                    'keyword': 'IS'
                                }, {
                                    'whitespace': ' '
                                }, {
                                    'keyword': 'NOT'
                                }, {
                                    'whitespace': ' '
                                }, {
                                    'null_literal': 'NULL'
                                }, {
                                    'whitespace': ' '
                                }, {
                                    'binary_operator': 'AND'
                                }, {
                                    'whitespace': ' '
                                }, {
                                    'column_reference': [{
                                        'naked_identifier':
                                        '_'
                                    }, {
                                        'dot': '.'
                                    }, {
                                        'naked_identifier':
                                        'way_area'
                                    }]
                                }, {
                                    'whitespace': ' '
                                }, {
                                    'keyword': 'IS'
                                }, {
                                    'whitespace': ' '
                                }, {
                                    'keyword': 'NOT'
                                }, {
                                    'whitespace': ' '
                                }, {
                                    'null_literal': 'NULL'
                                }]
                            }, {
                                'whitespace': ' '
                            }, {
                                'keyword': 'THEN'
                            }, {
                                'whitespace': ' '
                            }, {
                                'expression': {
                                    'column_reference': [{
                                        'naked_identifier':
                                        '_'
                                    }, {
                                        'dot': '.'
                                    }, {
                                        'naked_identifier':
                                        'building'
                                    }]
                                }
                            }]
                        }, {
                            'newline': '\n'
                        }, {
                            'whitespace': ' '
                        }, {
                            'else_clause': {
                                'keyword': 'ELSE',
                                'whitespace': ' ',
                                'expression': {
                                    'cast_expression': {
                                        'null_literal': 'NULL',
                                        'casting_operator': '::',
                                        'data_type': {
                                            'keyword': 'text'
                                        }
                                    }
                                }
                            }
                        }, {
                            'newline': '\n'
                        }, {
                            'whitespace': ' '
                        }, {
                            'keyword': 'END'
                        }]
                    }]
                }, {
                    'newline': '\n'
                }, {
                    'whitespace': ' '
                }]
                feature_end = feature_end[0]
                for i, element in enumerate(additional_content):
                    feature_end.context.value.insert(
                        feature_end.path.index + i, element)

        return self.sqlfluff_back_to_string(_sql_ast)

    def get_description(self, name):
        return self.mapnik_info.get(name, None)

    def load_mapnik(self):
        '''
        Loads the mapnik information and sql statements and parses them.
        '''
        bs = BeautifulSoup(open(self.mapnik_file), 'xml')

        layers = bs.findAll('Layer')
        for _layer in layers:
            #Extract sql
            _name = _layer.attrs["name"]
            _sql = _layer.findAll('Parameter', {"name": "table"})[0].get_text()

            #Extract styles
            _styles = []
            _layer = _layer.findPreviousSibling()

            while _layer.name == 'Style':
                _styles.append(_layer)
                _layer = _layer.findPreviousSibling()
            #Todo, getter & setter
            self.mapnik_info[_name] = {
                "sql":
                lambda _sql=_sql, _name=_name: self.parse_mapnik_sql(
                    _sql, label=_name),
                "styles":
                self.parse_mapnik_styles(_styles)
            }

        self.mapnik_loaded = True
        return self.mapnik_info


if __name__ == '__main__':
    import os
    _dir = os.path.dirname(__file__)

    _mn = MapnikSqlParser(os.path.join(_dir, "config", "mapnik.xml"))
    _mn.load_mapnik()
    _sql = _mn.get_description("amenity-points")
    _sql = _sql["sql"]()
    print(_sql)
