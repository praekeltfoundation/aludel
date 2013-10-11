from functools import partial

from twisted.trial.unittest import TestCase

from .doubles import FakeThreadPool, FakeReactorThreads


def put_args_in_list(argslist, *args, **kw):
    """A toy function that we call in/from fake threads."""
    argslist.extend([args, kw])


class TestFakeThreadPool(TestCase):
    def test_callInThread(self):
        pool = FakeThreadPool()
        foo = []
        pool.callInThread(put_args_in_list, foo, 'thing', kw='val')
        assert foo == [('thing',), {'kw': 'val'}]

    def test_callInThreadWithCallback_no_callback(self):
        pool = FakeThreadPool()
        foo = []
        pool.callInThreadWithCallback(
            None, put_args_in_list, foo, 'thing', kw='val')
        assert foo == [('thing',), {'kw': 'val'}]

    def test_callInThreadWithCallback_success(self):
        pool = FakeThreadPool()

        def func(*args, **kw):
            return args, kw

        foo = []
        pool.callInThreadWithCallback(
            partial(put_args_in_list, foo), func, 'thing', kw='val')
        assert foo == [(True, (('thing',), {'kw': 'val'})), {}]

    def test_callInThreadWithCallback_failure(self):
        pool = FakeThreadPool()

        def func(*args, **kw):
            raise Exception()

        foo = []
        pool.callInThreadWithCallback(partial(put_args_in_list, foo), func)
        failure = foo[0][1]
        assert foo == [(False, failure), {}]
        assert isinstance(failure.value, Exception)


class TestFakeReactorThreads(TestCase):
    def test_getThreadPool(self):
        reactor = FakeReactorThreads()
        assert isinstance(reactor.getThreadPool(), FakeThreadPool)

    def test_callInThread(self):
        reactor = FakeReactorThreads()
        foo = []
        reactor.callInThread(put_args_in_list, foo, 'thing', kw='val')
        assert foo == [('thing',), {'kw': 'val'}]

    def test_callFromThread(self):
        reactor = FakeReactorThreads()
        foo = []
        reactor.callFromThread(put_args_in_list, foo, 'thing', kw='val')
        assert foo == [('thing',), {'kw': 'val'}]
