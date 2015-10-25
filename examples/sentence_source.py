import time
from concord.computation import (
    Computation,
    Metadata,
    serve_computation
)


def time_millis():
    return int(round(time.time() * 1000))


class SentenceSource(Computation):
    def __init__(self):
        self.sentences = [
            u"This module implements a number of iterator building blocks inspired by constructs from APL, Haskell, and SML. Each has been recast in a form suitable for Python.",
            u"The module standardizes a core set of fast, memory efficient tools that are useful by themselves or in combination. Together, they form an 'iterator algebra' making it possible to construct specialized tools succinctly and efficiently in pure Python.",
            u"For instance, SML provides a tabulation tool: tabulate(f) which produces a sequence f(0), f(1), .... The same effect can be achieved in Python by combining imap() and count() to form imap(f, count()).",
            u"These tools and their built-in counterparts also work well with the high-speed functions in the operator module. For example, the multiplication operator can be mapped across two vectors to form an efficient dot-product: sum(imap(operator.mul, vector1, vector2)).",
        ]

    def sample(self):
        """returns a random word"""
        import random
        return random.choice(self.sentences)

    def init(self, ctx):
        self.concord_logger.info("Source initialized")
        ctx.set_timer('loop', time_millis())

    def process_timer(self, ctx, key, time):
        # stream, key, value. empty value, no need for val
        for i in range(0, 1024):
            ctx.produce_record("sentences", str(i), self.sample())

        # emit records every 500ms
        ctx.set_timer("main_loop", time_millis() + 5000)

    def process_record(self, ctx, record):
        raise Exception('process_record not implemented')

    def metadata(self):
        return Metadata(
            name='sentence-source',
            istreams=[],
            ostreams=['sentences'])

serve_computation(SentenceSource())
