from ochre.component import Component
import typing as t


class MyComponent(Component):
    a: str
    b: int | None = None
    c: t.Callable | None = None
    d: Component | None = None


def my_function(x):
    return x + 1

def test():

    c = MyComponent('test', a='value', b=123, c=lambda x: x + 1, d=MyComponent('child', a='child_value', c=my_function))

    r = c.encode()

    import pprint

    pprint.pprint(r)

    assert r['_builds']['MyComponent:child']['c'].startswith(':import:')
    assert r['c'].startswith(':blob:g')

    decoded = MyComponent.decode(r)

    assert isinstance(decoded, MyComponent)

    assert isinstance(decoded.d, MyComponent)

    assert callable(decoded.c)


    assert decoded.a == 'value'

    assert decoded.b == 123