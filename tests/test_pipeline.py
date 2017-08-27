import pytest
from core.pipelines import OkPipeline


class Foo:
    foo_attr = ''

    def __init__(self, attr):
        self.foo_attr = attr


class Bar:
    bar_attr = ''

    def __init__(self, foo, attr):
        self.foo = foo
        self.bar_attr = attr


def test_pipeline_rget():
    pipe = OkPipeline(Bar, 'foo', [])
    assert pipe._rget_attr(Bar(Foo('hello world'), '123'), 'bar_attr') == '123'
    assert pipe._rget_attr(Bar(Foo('hello world'), '123'), 'foo.foo_attr') == 'hello world'
    with pytest.raises(AttributeError):
        pipe._rget_attr(Bar(Foo('hello world'), '123'), 'foo.foo_attr2')


def test_pipeline_rset():
    pipe = OkPipeline(Bar, 'foo', [])
    bar = Bar(Foo('hello world'), '123')
    pipe._rset_attr(bar, 'foo.bar_attr', 'foobar123')
    pipe._rset_attr(bar, 'foo.foo_attr', 'foo123')
    pipe._rset_attr(bar, 'bar_attr', 'bar123')
    assert pipe._rget_attr(bar, 'foo.bar_attr') == 'foobar123'
    assert pipe._rget_attr(bar, 'bar_attr') == 'bar123'
    assert pipe._rget_attr(bar, 'foo.foo_attr') == 'foo123'
