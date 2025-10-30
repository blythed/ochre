from ochre.component import Component


class MyComponent(Component):
    breaks = ('param1', 'param2', 'sub')
    param1: str
    param2: int
    param3: float = 0.5
    sub: Component | None = None


def test_merkle_tree_one_level():

    c1 = MyComponent('test', param1='value1', param2=123, param3=0.75)
    c2 = MyComponent('test', param1='value1', param2=123, param3=0.75)
    c3 = MyComponent('test', param1='value1', param2=123, param3=0.5)
    c4 = MyComponent('test', param1='value1', param2=1234, param3=0.75)

    assert c1.uuid == c2.uuid
    assert c1.hash == c2.hash

    assert c1.uuid == c3.uuid
    assert c1.hash != c3.hash

    assert c1.uuid != c4.uuid
    assert c1.hash != c4.hash


def test_merkle_tree_nested():

    c1 = MyComponent(
        'test',
        param1='value1',
        param2=123,
        param3=0.75,
        sub=MyComponent('child', param1='child_value', param2=1)
    )

    c2 = MyComponent(
        'test',
        param1='value1',
        param2=123,
        param3=0.8,
        sub=MyComponent('child', param1='child_value', param2=1)
    )
    
    c3 = MyComponent(
        'test',
        param1='value1',
        param2=123,
        param3=0.75,
        sub=MyComponent('child', param1='child_value', param2=3)
    )

    assert c1.uuid == c2.uuid
    assert c1.hash != c2.hash

    assert c1.uuid != c3.uuid
    assert c1.hash != c3.hash