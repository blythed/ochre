import datetime
import importlib
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

import typer
from ochre.component import Component
from ochre.core import apply as ochre_apply, destroy as ochre_destroy

app = typer.Typer(help="CLI for running Ochre components")

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

load_dotenv()


def resolve_module(module: str) -> str:
    """
    Convert a .py path into a Python module name if needed.
    
    :param module: The module to resolve.
    """
    if module.endswith(".py"):
        return Path(module).with_suffix("").as_posix().replace("/", ".")
    return module


@app.command()
def apply(module: str, force: bool = False, pipeline_name: str = 'main', clean: bool = False):
    """
    Import a pipeline and run `ochre.core.apply`.

    :param module: The module to import. Can be a .py file or a Python module name.
    :param force: Whether to force the application of the component.
    """

    if module.endswith(".py"):
        module = module.replace(".py", "")
    module = module.replace('/', '.')
    mod = importlib.import_module(module)
    component = getattr(mod, pipeline_name)()
    ochre_apply(component=component, force=force, clean=clean)


@app.command()
def reapply(component: str, identifier: str):
    """Refresh an `ochre.Component`.

    :param component: The component to update.
    :param identifier: The identifier of the component to update.
    """
    print(datetime.datetime.now().isoformat(), "Refreshing component...")

    component = Component.load(component, identifier)
    ochre_apply(component=component, force=True, schedule=False)


@app.command()
def test(path: str, destroy: bool = True, entrypoint: str | None = None):
    """
    Import a class and run its inbuilt smoke test using `ochre.core.apply`.

    :param path: The path to the module. Can be a `.py` file or a Python module name.
    :param destroy: Whether to destroy the component after applying it.
    """
    path = path.replace('.py', '')
    path = path.replace('::', '.')
    path = path.replace('/', '.')
    module = '.'.join(path.split('.')[:-1])
    cls = path.split('.')[-1]
    cls = getattr(importlib.import_module(resolve_module(module)), cls)
    component = cls.build_example()
    try:
        ochre_apply(component=component, force=True)
        if entrypoint:
            getattr(component, entrypoint)()
    finally:
        if destroy:
            ochre_destroy(component=component, force=True)


@app.command()
def enter(component: str, identifier: str, entrypoint: str = "main"):
    """
    Import a module and run its `main` function using ochre.core.apply.
    """
    component = Component.load(component, identifier)
    getattr(component, entrypoint)()


@app.command()
def destroy(path: str, force: bool = False, entrypoint: str = 'main'):
    """
    Import a pipeline and run ochre.core.destroy.
    """

    if os.path.exists(path + '/component.json'):
        path = path.replace(".py", "")
        _, component, identifier = path.rsplit("/", 2)  
        component = Component.load(component, identifier)

    else:
        path = path.replace('.py', '')
        path = path.replace('::', '.')
        path = path.replace('/', '.')
        component = getattr(importlib.import_module(path), entrypoint)()

    ochre_destroy(component=component, force=force)


if __name__ == "__main__":
    app()
