import unittest
from pipeline import *
import sys

@pipefilter
def double(target):
    while True:
        value = (yield)
        print("Doubling {}".format(value))
        target.send(value * 2)


class TestError(Exception):
    pass


class IterSinkTest(unittest.TestCase):
    def test(self):
        @iter_sink
        def append_to_list(i, l):
            for item in i:
                l.append(item)

        results = []
        iter_source(range(10)) | (double() | append_to_list(results))
        self.assertEqual(results, [0, 2, 4, 6, 8, 10, 12, 14, 16, 18])

    def test_error(self):
        class TestException(Exception):
            pass

        @iter_sink
        def consume_iterator(i):
            for counter, value in enumerate(i):
                if counter > 4:
                    print("Raising exception")
                    raise TestException("Test error")
                else:
                    print("Got {}".format(value))

        def pipeline():
            iter_source(range(10)) | (double() | consume_iterator())

        self.assertRaises(TestException, pipeline)

    def test_filter(self):
        @iter_filter
        def power_of_two(i):
            for value in i:
                yield 2 ** value

        results = []
        iter_source(range(5)) | (double() | power_of_two() | appender(results))
        self.assertEqual(results, [1, 4, 16, 64, 256])

    def test_halfspeed_filter(self):
        @iter_filter
        def joiner(i):
            while True:
                a = next(i)
                b = next(i)
                yield a + " " + b

        results = []
        iter_source(["this", "is", "a", "test"]) | (joiner() | appender(results))
        self.assertEqual(results, ["this is", "a test"])

    def test_doublespeed_filter(self):
        @iter_filter
        def doubler(i):
            while True:
                v = next(i)
                yield "X: " + v
                yield "Y: " + v

        results = []
        iter_source(["this", "is", "a", "test"]) | (doubler() | appender(results))
        self.assertEqual(results, ["X: this", "Y: this", "X: is", "Y: is",
                                   "X: a", "Y: a", "X: test", "Y: test"])

    def test_immediate_exception(self):
        @iter_filter
        def error_filter(i):
            raise TestError()
        
        try:
            iter_source(["a", "test"]) | (error_filter())
        except TestError:
            pass
        else:
            assert False, "Expected TestError exception"

    def test_exception_after_iteration(self):
        @iter_filter
        def error_filter(i):
            for v in i:
                print(v)
            print("About to raise exception")
            raise TestError()

        try:
            iter_source(["a", "test"]) | (error_filter())
        except TestError:
            pass
        else:
            assert False, "Expected TestError exception"
