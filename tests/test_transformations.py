# -*- coding: utf-8 -*-
"""
    test_transformations
    ~~~~~~~~~~~~~~~~~~~~

    Tests the transformations
"""

import pytest

from concord_sources import Identity, Map, Filter, Reduce


@pytest.fixture
def sentences():
    return [
        "This module implements a number of iterator building blocks inspired by constructs from APL, Haskell, and SML. Each has been recast in a form suitable for Python.",
        "The module standardizes a core set of fast, memory efficient tools that are useful by themselves or in combination. Together, they form an “iterator algebra” making it possible to construct specialized tools succinctly and efficiently in pure Python.",
        "For instance, SML provides a tabulation tool: tabulate(f) which produces a sequence f(0), f(1), .... The same effect can be achieved in Python by combining imap() and count() to form imap(f, count()).",
        "These tools and their built-in counterparts also work well with the high-speed functions in the operator module. For example, the multiplication operator can be mapped across two vectors to form an efficient dot-product: sum(imap(operator.mul, vector1, vector2)).",
    ]


def sentence_mapper(key, sentence):
    for word in sentence.split(' '):
        value = word.lower()
        yield value, value


def word_mapper(key, value):
    return key, 1


def word_filter(word, value):
    return word.lower()[0] == 't'


def count_word(word, values):
    return word, sum(values)


def map_counted(key, value):
    return 0, value


def sum_words(key, values):
    return key, sum(values)


@pytest.mark.unit
def test_wordcount(sentences):
    pipeline = [
        Identity(),
        Map(sentence_mapper),
        Filter(word_filter),
        Map(word_mapper),
        Reduce(count_word),
        Map(map_counted),
        Reduce(sum_words)
    ]
    iterator = enumerate(sentences)
    for transformation in pipeline:
        iterator = transformation.apply(iterator)
    results = list(iterator)
    assert results == [(0, 22)]
