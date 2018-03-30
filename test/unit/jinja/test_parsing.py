from unittest import TestCase

from dbt.clients.jinja import JinjaParser


def ref(*args):
    raise RuntimeError('ref was actually called!')


class TestParsing(TestCase):

    def test__basic_ref(self):
        template = """
{{ref('a')}}
"""

        result = JinjaParser.parse(template)

        self.assertEquals(
            result,
            {
                'calls': {
                    'ref': [
                        {'args': [{'type': 'constant', 'value': 'a'}],
                         'kwargs': {}}
                    ]
                }
            })

    def test__macro(self):
        template = """
{% macro a(b) %}
  {{ref(b)}}
{% endmacro %}
"""

        result = JinjaParser.parse(template)

        self.assertEquals(
            result,
            {
                'calls': {
                    'ref': [
                        {'args': [{'type': 'variable', 'name': 'b'}],
                         'kwargs': {}}
                    ]
                }
            })

    def test__config(self):
        template = """
{{config(materialized='table')}}
"""

        result = JinjaParser.parse(template)

        self.assertEquals(
            result,
            {
                'calls': {
                    'config': [
                        {
                            'args': [],
                            'kwargs': {
                                'materialized': {
                                    'type': 'constant',
                                    'value': 'table'
                                }
                            }
                        }
                    ]
                }
            })

    def test__config_var(self):
        template = """
{{config(materialized=var('materialization'))}}
"""

        result = JinjaParser.parse(template)

        self.assertEquals(
            result,
            {
                'calls': {
                    'config': [
                        {
                            'args': [],
                            'kwargs': {
                                'materialized': {
                                    'type': 'constant',
                                    'value': 'table'
                                }
                            }
                        }
                    ]
                }
            })
