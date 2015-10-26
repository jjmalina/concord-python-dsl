# -*- coding: utf-8 -*-
"""
    concord_sources
    ~~~~~~~~~~~~~~~

    Express concord computations like Spark
"""

__version__ = '0.1.0'

import sys
import json
import time
import types
import operator
import traceback
from collections import defaultdict

from concord.computation import Computation, Metadata, serve_computation


class ComputationWrapper(object, Computation):
    """
    Workaround for
    `TypeError: a new-style class can't have only classic bases`
    """
    pass


def time_millis(add=0.):
    """current time in milliseconds

    :param add: Seconds to add.
    :type add: float.

    :returns: the current or future time depending on `add`
    :rtype: int
    """
    return int(round(time.time() * 1000 + add * 1000.))


class Identity(object):
    def __init__(self, function=lambda x: x):
        self._function = function

    def apply(self, iterable):
        for k, v in iterable:
            yield k, v


class Map(Identity):
    def apply(self, iterable):
        for key, value in iterable:
            try:
                result = self._function(key, value)
            except Exception as e:
                sys.stderr.write("Error in mapper:\n")
                sys.stderr.write(traceback.format_exc() + '\n')
                raise e
            if isinstance(result, types.GeneratorType):
                for k, v in result:
                    yield k, v
            else:
                yield result[0], result[1]


class Filter(Identity):
    def apply(self, iterable):
        for key, value in iterable:
            try:
                predicate = self._function(key, value)
            except Exception as e:
                sys.stderr.write("Error in filter:\n")
                sys.stderr.write(traceback.format_exc() + '\n')
                raise e
            if predicate:
                yield key, value


class Reduce(Identity):
    def __init__(self, *args, **kwargs):
        super(Reduce, self).__init__(*args, **kwargs)
        self._keys = defaultdict(list)

    def apply(self, iterable):
        for key, value in iterable:
            self._keys[key].append(value)
        grouped = sorted(self._keys.iteritems(), key=operator.itemgetter(0))
        for key, values in grouped:
            try:
                reduced = self._function(key, values)
            except Exception as e:
                sys.stderr.write("Error in reducer:\n")
                sys.stderr.write(traceback.format_exc() + '\n')
                raise e
            yield reduced[0], reduced[1]
        # reset state
        self._keys = defaultdict(list)


class InputSource(object):
    def __init__(self, name, istream, batch_interval=1):
        # name of the computation
        self.name = name

        # batch interval to process a window
        self._batch_interval = batch_interval

        # concord istream to process
        self._istream = istream

        # output streams
        self._ostreams = []

        # Identitys of computation
        self._add_to_pre_batch = False
        self._pre_batch_computations = [Identity()]
        self._post_batch_computations = [Identity()]

        self._batch_record_callbacks = []

        self._window_tuples = []

    def serialize(self, value, serializer):
        if serializer == 'json':
            return json.dumps(value)

    def _get_metadata(self):
        def metadata(computation):
            return Metadata(
                name=self.name,
                istreams=[self._istream],
                ostreams=map(operator.itemgetter(0), self._ostreams)
            )
        return metadata

    def _get_init(self):
        def init(computation, ctx):
            ctx.set_timer('window', time_millis(self._batch_interval))
        return init

    def handle_batch(self, ctx):
        window_tuples = self._window_tuples
        self._window_tuples = []

        # generate the tranformation
        identity = self._post_batch_computations[0]
        iterator = identity.apply(window_tuples)
        for transformation in self._post_batch_computations[1:]:
            iterator = transformation.apply(iterator)

        for key, value in list(iterator):
            for fn in self._batch_record_callbacks:
                fn(key, value)
            for ostream, serializer in self._ostreams:
                data = self.serialize(value, serializer)
                ctx.produce_record(self.name, str(key), data)

    def _get_process_timer(self):
        """Generate the process_timer method

        This is where the window ultimately gets processed
        """
        def process_timer(computation, ctx, key, time):
            try:
                self.handle_batch(ctx)
            except Exception as e:
                sys.stderr.write("Erro in handle_batch:\n")
                sys.stderr.write(traceback.format_exc() + '\n')
                raise e
            ctx.set_timer('window', time_millis(self._batch_interval))

        return process_timer

    def _get_process_record(self):
        """Generate the process_record method given all computations

            Apply pre batch computations
        """
        def process_record(computation, ctx, record):
            tuple_ = (record.key, record.data)

            # generate the transformation
            identity = self._pre_batch_computations[0]
            iterator = identity.apply([tuple_])
            for transformation in self._pre_batch_computations[1:]:
                iterator = transformation.apply(iterator)

            # evaluate the transformation
            self._window_tuples.append(list(iterator)[0])
        return process_record

    def _get_computations_list(self):
        if self._add_to_pre_batch:
            return self._pre_batch_computations
        return self._post_batch_computations

    def map(self, fn):
        computations = self._get_computations_list()
        computations.append(Map(fn))
        return self

    def filter(self, fn):
        computations = self._get_computations_list()
        computations.append(Filter(fn))
        return self

    def reduce(self, fn):
        self._add_to_pre_batch = False
        computations = self._get_computations_list()
        computations.append(Reduce(fn))
        return self

    def sink(self, ostream, serializer='json'):
        self._ostreams.append([ostream, serializer])
        return self

    def on_batched_record(self, fn):
        self._batch_record_callbacks.append(fn)

    def run(self):
        """
        Generate the concord Computation class and run
        via serve_computation
        """
        attrs = {
            'metadata': self._get_metadata(),
            'init': self._get_init(),
            'process_timer': self._get_process_timer(),
            'process_record': self._get_process_record(),
        }
        computation = type(self.name, (ComputationWrapper,), attrs)
        serve_computation(computation())
