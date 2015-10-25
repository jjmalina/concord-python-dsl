# Concord Python DSL

A simple Spark-like DSL for [Concord's Python client](https://github.com/concord/concord-py).

## Example

```
import sys
from concord_sources import InputSource


def print_word_count(key, value):
    sys.stderr.write(str(value) + " words counted\n")


source = InputSource('sentence-counter', 'sentences')
words = source.map(
    lambda key, val: ((word, word) for word in val.split(' '))
)
word_count = words \
    .map(lambda key, word: (word, 1)) \
    .reduce(lambda word, counts: (word, sum(counts))) \
    .map(lambda word, frequency: (0, frequency)) \
    .reduce(lambda key, counts: (key, sum(counts)))

word_count.sink('word-count')
word_count.on_batched_record(print_word_count)
word_count.run()
```
