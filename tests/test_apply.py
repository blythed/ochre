import os
import typing as t

import pytest

from ochre import Component, apply
from ochre.constants import REGISTRY


class MyComponent(Component):
    a: str
    b: Component | None = None
    c: t.Any | None = None
    d: str | None = None

    def create(self):
        self.d = "created"
        os.system(f'touch {REGISTRY}/{self.component}/{self.identifier}_created.txt')


def my_function():
    ...


@pytest.fixture(autouse=True)
def cleanup():
    yield
    os.system(f'rm -rf ./{REGISTRY}/MyComponent')


def create_component():
    return MyComponent(
        "test",
        a="value",
        b=MyComponent("child", a="child_value"),
        c=my_function,
    )


def test():
    component = create_component()

    assert component.identifier == "test"
    assert component.a == "value"
    assert component.b.identifier == "child"
    assert component.b.a == "child_value"

    r = component.encode()

    decoded = Component.decode(r)

    decoded.show()

    import json
    print(json.dumps(r, indent=2))

    apply(component, force=True)

    assert os.path.exists(f"./{REGISTRY}/MyComponent/test/component.json")
    assert os.path.exists(f"./{REGISTRY}/MyComponent/test_created.txt")
    assert os.path.exists(f"./{REGISTRY}/MyComponent/child_created.txt")
    