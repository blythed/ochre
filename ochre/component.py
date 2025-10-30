"""The component module provides the base class for all components.""" 
from abc import ABCMeta
import base64
import copy
import dataclasses as dc
import importlib
import inspect
import io
import json
import os
import shutil
import typing as t
from collections import OrderedDict, defaultdict
from contextlib import redirect_stdout
from warnings import warn

import dill
import rich
from rich.tree import Tree
from rich.text import Text

from ochre.constants import KEY_BUILDS, LENGTH_UUID, REGISTRY
from ochre.exceptions import NotFound
from ochre.misc import asdict, extract_parameters, hash_item, lazy_classproperty, replace_parameters


def _build_info_from_path(path: str):
    if os.path.exists(os.path.join(path, "component.json")):
        config = os.path.join(path, "component.json")
        with open(config) as f:
            config_object = json.load(f)
    else:
        raise FileNotFoundError(
            f'`component.json` does not exist in the path {path}'
        )
    return config_object


class ComponentMeta(ABCMeta):
    """Metaclass that merges docstrings # noqa."""

    def __new__(mcs, name, bases, namespace):
        """Create a new class with merged docstrings # noqa."""
        # Prepare namespace by extracting annotations and handling fields
        annotations = namespace.get('__annotations__', {})
        for k, v in list(namespace.items()):
            if isinstance(v, (type, dc.InitVar)):
                annotations[k] = v
            if isinstance(v, dc.Field):
                if v.type is not None:
                    annotations[k] = v.type

        # Update namespace with proper annotations
        namespace['__annotations__'] = annotations

        # Determine if any bases are dataclasses and
        # apply the appropriate dataclass decorator
        is_base = (
            namespace.get('__module__', '') == 'ochre.component'
            and name == 'Component'
        )

        dataclass_params = namespace.get('_dataclass_params', {}).copy()
        if bases and any(dc.is_dataclass(b) for b in bases) and not is_base:
            dataclass_params['kw_only'] = True
            # Derived classes: kw_only=True
        else:
            # Base class: kw_only=False
            dataclass_params['kw_only'] = False

        cls = dc.dataclass(**dataclass_params, repr=False)(
            super().__new__(mcs, name, bases, namespace)
        )

        # Merge docstrings from parent classes
        parent_doc = next(
            (parent.__doc__ for parent in inspect.getmro(cls)[1:] if parent.__doc__),
            None,
        )
        if parent_doc:
            parent_params = extract_parameters(parent_doc)
            child_doc = cls.__doc__ or ''
            child_params = extract_parameters(child_doc)
            for k in child_params:
                parent_params[k] = child_params[k]
            placeholder_doc = replace_parameters(child_doc)
            param_string = ''
            for k, v in parent_params.items():
                v = '\n    '.join(v)
                param_string += f':param {k}: {v}\n'
            cls.__doc__ = placeholder_doc.replace('!!!', param_string)

        return cls


class Component(metaclass=ComponentMeta):
    """Base class for all components in https://superduper.io.

    Class to represent superduper.io serializable entities that can be saved into a database.

    :param identifier: Identifier of the instance.
    :param data: Data stored about the component.
    :param upstream: A list of upstream components.
    :param compute_kwargs: Keyword arguments to manage the compute environment.
    """
    breaks: t.ClassVar[t.Sequence] = ()
    metadata_fields: t.ClassVar[t.Dict[str, t.Type]] = {
        'uuid': str,
        'component': str,
        '_path': str,
    }
    identifier: str

    def __post_init__(self):

        self.context: str | None = None

        self._original_parameters: t.Dict | None = None

        self._uuid = None
        self._hash = None
        self._merkle_tree_breaks = None
        self._merkle_tree = None
        self._parents: t.List[t.Tuple[Component, str]] = []
        self._children: t.List[Component] = []
        self._metadata_children: t.List[Component] = []

        self.read()

        self._handle_parent_children()

        assert self.identifier, "Identifier cannot be empty or None"

    def status(self) -> str:
        if os.path.exists(f"{REGISTRY}/{self.component}/{self.identifier}/.status/error"):
            return "error"
        if os.path.exists(f"{REGISTRY}/{self.component}/{self.identifier}/.status/complete"):
            return "complete"
        elif os.path.exists(f"{REGISTRY}/{self.component}/{self.identifier}/.status/in_progress"):
            return "in_progress"
        return "uninitialized"

    def save_file(self, filename: str, text: str | None = None, content: bytes | None = None):
        """Save a file to the blobs directory of the component.

        :param filename: Name of the file to save.
        :param text: Text content of the file to save.
        :param content: Content of the file to save.
        """
        path = f"{REGISTRY}/{self.component}/{self.identifier}/files"
        os.makedirs(path, exist_ok=True)
        assert text is not None or content is not None, "Either text or content must be provided"

        if text is not None:
            with open(os.path.join(path, filename), "w") as f:
                f.write(text)
        else:
            with open(os.path.join(path, filename), "wb") as f:
                f.write(content)

    def rm_file(self, filename: str):
        """Remove a file from the blobs directory of the component.

        :param filename: Name of the file to save.
        """
        try:
            os.remove(os.path.join(f"{REGISTRY}/{self.component}/{self.identifier}/files/", filename))
        except FileNotFoundError:
            pass

    def read_text_file(self, filename: str) -> str:
        """Read a text file from the blobs directory of the component.

        :param filename: Name of the file to read.
        """
        path = f"{REGISTRY}/{self.component}/{self.identifier}/files"
        with open(os.path.join(path, filename), "r") as f:
            return f.read()

    def read_binary_file(self, filename: str) -> bytes:
        """Read a binary file from the blobs directory of the component.

        :param filename: Name of the file to read.
        """
        path = f"{REGISTRY}/{self.component}/{self.identifier}/files"
        with open(os.path.join(path, filename), "rb") as f:
            return f.read()

    def _handle_parent_children(self):
        for child, position in self._get_children_with_positions():
            child_uuids = [p[0].uuid for p in child._parents]
            if position in self.metadata_fields:
                self._metadata_children.append(child)
            else:
                self._children.append(child)
            if self.uuid not in child_uuids:
                child._parents.append((self, position))

    @property
    def uuid(self):
        """Get UUID."""
        if self._uuid is None:
            breaking = hash_item(
                [self.component, self.identifier]
                + [
                    self.merkle_tree_breaks[k]
                    for k in self.breaks
                    if k in self.merkle_tree_breaks
                ]
            )
            return breaking[:LENGTH_UUID]
        return self._uuid

    @property
    def hash(self):
        """Get hash."""
        if self._hash is None:
            t = self.merkle_tree
            breaking_hashes = [t[k] for k in self.breaks if k in t]
            non_breaking_hashes = [t[k] for k in t if k not in self.breaks]
            breaking = hash_item(breaking_hashes)
            non_breaking = hash_item(non_breaking_hashes)
            self._hash = breaking[:32] + non_breaking[:32]
        return self._hash

    @property
    def merkle_tree_breaks(self):
        """Get merkle tree for breaking changes."""
        if self._merkle_tree_breaks is None:
            self._merkle_tree_breaks = self._build_merkle_tree(breaks=True)
        return self._merkle_tree_breaks

    @property
    def merkle_tree(self):
        """Get merkle tree."""
        if self._merkle_tree is None:
            self._merkle_tree = self._build_merkle_tree(breaks=False)
        return self._merkle_tree

    def _do_hash_item(self, key, value):
        if key in self.fields:
            self.merkle_tree[key] = self._hash_item(value, breaks=False)
        if key in self.breaks:
            warn(f'Breaking change detected in {self.component}.{key}')
            self.merkle_tree_breaks[key] = self._hash_item(value, breaks=True)

    @lazy_classproperty
    def fields(cls):
        return [f.name for f in dc.fields(cls)]

    def __setattr__(self, key, value):
        if key in self.metadata_fields or key not in self.fields:
            return super().__setattr__(key, value)

        # initialization phase
        if '_merkle_tree' not in self.__dict__:
            return super().__setattr__(key, value)

        if getattr(self, key) == value:
            return

        _ = self.merkle_tree

        if key not in self.merkle_tree:
            return super().__setattr__(key, value)

        previous_uuid = self.uuid

        super().__setattr__(key, value)

        _ = self.merkle_tree_breaks

        self._do_hash_item(key, value)

        if previous_uuid == self.uuid:
            return

        for parent, position in self._parents:
            parent._do_hash_item(position, getattr(parent, position))

    def _build_tree(self, depth: int, tree=None):
        """Show the component."""
        if tree is None:
            from rich.tree import Tree

            tree = Tree(f"{self.huuid}")
        if depth == 0:
            return tree

        for k, v in self.dict().items():
            if isinstance(v, Component):
                subtree = tree.add(f"{k}: {v.huuid}")
                v._build_tree(depth - 1, subtree)
            else:
                if v:
                    if isinstance(v, dict):
                        subtree = tree.add(k)
                        for sub_k, sub_v in v.items():
                            subtree.add(f"{sub_k}: {sub_v}")
                    else:
                        tree.add(f"{k}: {v}")
        return tree

    def _show_repr(self, depth: int = -1):
        with redirect_stdout(io.StringIO()) as buffer:
            self.show(depth=depth)
            return buffer.getvalue()

    def show(self, depth: int = -1):
        """Show the component in a tree format.

        :param depth: Depth of the tree to show.
        """
        tree_repr = self._build_tree(depth)
        rich.print(tree_repr)

    @uuid.setter
    def uuid(self, value):
        """Set UUID.

        :param value: The UUID to set.
        """
        self._uuid = value

    def _build_merkle_tree(self, breaks: bool):
        """Get the merkle tree of the component.

        :param breaks: If set `true` only regard the parameters which break a version.
        """
        r = self.dict(metadata=False)
        if breaks:
            keys = sorted([k for k in r.keys() if k in self.breaks])
        else:
            keys = sorted([k for k in r.keys() if k not in self.metadata_fields])

        tree = OrderedDict([(k, self._hash_item(r[k], breaks=breaks)) for k in keys])
        return tree

    @classmethod
    def _hash_item(cls, item: t.Any, breaks: bool = False):
        if isinstance(item, Component):
            if breaks:
                return item.uuid
            return item.hash
        return hash_item(item)

    def diff(self, other: "Component", depth: int = -1):
        """Show differences between two components as a rich tree.

        :param other: The other component to compare.
        :param depth: Maximum recursion depth (-1 = unlimited).
        """
        def _build(c1, c2, d, level=0):
            if type(c1) is not type(c2):
                try:
                    root = Tree(
                        f"[bold]{c1.component}/{c1.identifier}[/bold] != [bold]{c2.component}/{c2.identifier}[/bold]"
                    )
                except AttributeError:
                    root = Tree(
                        f"[bold]{c1.component}/{c1.identifier}[/bold] != [bold]{c2}[/bold]"
                    )
                return root

            root = Tree(
                f"[bold]{c1.component}/{c1.identifier}[/bold]"
            )

            # go through all fields
            for k in set(c1.dict().keys()) | set(c2.dict().keys()):
                v1 = c1.dict().get(k, None)
                v2 = c2.dict().get(k, None)

                # skip metadata fields for now
                if k in c1.metadata_fields:
                    continue

                color = "red" if k in self.breaks else "yellow"

                # recurse if both are components
                if isinstance(v1, Component) and isinstance(v2, Component):
                    if v1.hash != v2.hash:
                        if d == -1 or level < d:
                            subtree = _build(v1, v2, d, level + 1)
                            root.add(f"[bold][{color}]{k}[/{color}][/bold]").add(subtree)
                        else:
                            node = root.add(f"[bold][{color}]{k}[/{color}][/bold]")
                            node.add(Text(f"self: {v1.huuid}", style="green"))
                            node.add(Text(f"other: {v2.huuid}", style="cyan"))
                else:
                    if v1 != v2:
                        node = root.add(f"[bold][{color}]{k}[/{color}][/bold]")
                        node.add(Text(f"self: {v1}", style="green"))
                        node.add(Text(f"other: {v2}", style="cyan"))

            return root

        diff_tree = _build(self, other, depth)
        rich.print(diff_tree)

    @property
    def component(self):
        return self.__class__.__name__

    @property
    def huuid(self):
        """Return a human-readable uuid."""
        return f'{self.component}/{self.identifier}/{self.uuid}'

    def _get_children_with_positions(self):

        r = self.dict()

        out = defaultdict(list)

        def _get_children_from_item(item, k):
            if isinstance(item, Component):
                out[k].append(item)
            elif isinstance(item, dict):
                for v in item.values():
                    _get_children_from_item(v, k)
            elif isinstance(item, (tuple, list)):
                for x in item:
                    _get_children_from_item(x, k)

        for k, v in r.items():
            _get_children_from_item(v, k)

        to_return = []

        for k, v in out.items():
            for x in v:
                to_return.append((x, k))

        return to_return

    def get_children(self, deep: bool = False, metadata=True) -> t.List["Component"]:
        """Get all the children of the component.

        :param deep: If set `True` get all recursively.
        :param metadata: Get component children also in the metadata.
        """
        if not deep:
            if not metadata:
                return self.children
            else:
                out = {}
                for v in self.children + self.metadata_children:
                    out[v.uuid] = v
                return list(out.values())

        lookup: t.Dict[int, "Component"] = {}
        for v in self.children:
            lookup[v.uuid] = v
        if deep:
            children = list(lookup.values())
            for v in children:
                sub = v.get_children(deep=True, metadata=metadata)
                for s in sub:
                    lookup[s.uuid] = s
        return list(lookup.values())

    @property
    def children(self):
        """Get all the child components of the component."""
        return self._children

    @property
    def metadata_children(self):
        """Get all the child components of the component."""
        return self._metadata_children

    @staticmethod
    def load(component: str, identifier: str) -> 'Component':
        """
        Read a `Component` instance from a directory created with `.export`.

        :param component: Name of the component to load.
        :param identifier: Identifier of the component to load.

        Expected directory structure:
        ```
        |_component.json
        ```
        """
        path = f"{REGISTRY}/{component}/{identifier}"
        config_object = _build_info_from_path(path=path)
        return Component.decode(config_object)

    def rm(self, deep: bool = False):
        """
        Remove `self` from the registry.

        :param deep: If set `True` remove all children also.
        """
        path = f"{REGISTRY}/{self.component}/{self.identifier}"
        parent_path = f'{REGISTRY}/{self.component}'
        try:
            shutil.rmtree(path)
            if not os.listdir(parent_path):
                os.rmdir(parent_path)
        except FileNotFoundError:
            pass
        if deep:
            for child in self.get_children():
                child.rm(deep=True)

    def save(self, deep: bool = False):
        """
        Save `self` to a directory using super-duper protocol.

        Created directory structure:
        ```
        |_component.json
        ```
        """
        path = f"{REGISTRY}/{self.component}/{self.identifier}"

        os.makedirs(path, exist_ok=True)

        r = self.encode(deep=deep)

        with open(os.path.join(path, "component.json"), "w") as f:
            json.dump(r, f, indent=2)

    def encode(self, deep: bool = True) -> t.Dict:
        """Encode component as JSON and files.

        After encoding everything is a vanilla dictionary (JSON + bytes).
        """
        return self.dict().encode(deep=deep)

    def dict(self, metadata: bool = True) -> t.Dict[str, t.Any]:
        """Return dictionary representation of the object."""
        r = asdict(self)
        if metadata:
            for k, v in self.metadata.items():
                r[k] = v
        return Document(r)

    @property
    def metadata(self):
        """Get metadata of the component."""
        return {k: getattr(self, k) for k in self.metadata_fields}

    @property
    def _path(self):
        return f"{self.__class__.__module__}.{self.__class__.__name__}"

    @staticmethod
    def get_cls_from_path(path):
        """Get class from a path.

        :param path: Import path to the class.
        """
        parts = path.split('.')
        cls = parts[-1]
        module = '.'.join(parts[:-1])
        module = importlib.import_module(module)
        out = getattr(module, cls)
        return out

    @classmethod
    def from_dict(cls, r: t.Dict):
        """Create a `Component` instance from a dictionary.
        
        :param r: Dictionary to create the instance from.
        """
        signature_params = inspect.signature(cls.__init__).parameters
        in_signature = {k: v for k, v in r.items() if k in signature_params}
        in_metadata = {
            k: v for k, v in r.items() if k in getattr(cls, 'metadata_fields', {})
        }
        assert set(in_signature) | set(in_metadata) == set(
            r
        ), f'Unexpected parameters in dict not in signature or metadata fields of {cls.__name__}: {set(r) - (set(in_signature) | set(in_metadata))}'
        out = cls(**in_signature)
        for k, v in r.items():
            if k in getattr(cls, 'metadata_fields', {}):
                try:
                    setattr(out, k, v)
                except AttributeError:
                    pass  # can't set property
        return out

    @classmethod
    def decode(cls: t.Type['Component'], r):
        """Decode a dictionary component into a `Component` instance.

        :param r: Object to be decoded.
        """
        cls = cls.get_cls_from_path(r['_path'])
        r = Document.decode(r)
        return cls.from_dict(r)

    ### CRUD hooks

    def create(self):
        """Actions performed on creating the component."""
        pass

    def read(self):
        """Hook after reading the component from the registry."""
        pass

    def update(self):
        """Hook on updating the component."""
        pass

    def delete(self):
        """Hook to call on deleting the component."""
        pass

    @classmethod
    def build_example(cls) -> 'Component':
        raise NotImplementedError

    def __call__(self):
        return self


def _convert_base64_to_bytes(str_: str) -> bytes:
    return base64.b64decode(str_)


def _convert_bytes_to_base64(bytes_: bytes) -> str:
    return base64.b64encode(bytes_).decode('utf-8')


def dill_encode(item) -> str:
    return _convert_bytes_to_base64(dill.dumps(item, recurse=True, byref=False))


def dill_decode(item: str):
    return dill.loads(_convert_base64_to_bytes(item))


class Document(dict):
    """A wrapper around a `dict` including a schema and encoding.

    The document data is used to dump that resource to
    a mix of json-able content, imports, `Component` references and `base64` encoded `bytes`
    """

    def encode(self, deep: bool = True) -> t.Dict:
        """Encode the document to a format that can be used in a database.

        After encoding everything is a vanilla dictionary (JSON + bytes).
        (Even a model, or artifact etc..)
        """

        if deep:
            out = {KEY_BUILDS: {}}
        else:
            out = {}

        def _encode(item):
            if isinstance(item, Component):
                key = f'{item.component}:{item.identifier}'
                if deep and key not in out[KEY_BUILDS]:
                    tmp = item.encode()
                    tmp.pop('identifier')
                    out[KEY_BUILDS].update(tmp.pop(KEY_BUILDS))
                    out[KEY_BUILDS][key] = tmp
                return f'?{key}'
            elif ((inspect.isfunction(item) and item.__name__ != '<lambda>') or inspect.isclass(item)) and item.__module__ != '__main__':
                return ':import:' + item.__module__ + '.' + item.__name__
            elif isinstance(item, (list, tuple)):
                return [_encode(x) for x in item]
            elif isinstance(item, dict):
                return {k: _encode(v) for k, v in item.items()}
            elif not isinstance(item, (type(None), str, int, bool)):
                return ':blob:' + dill_encode(item)
            else:
                return item

        for k, v in self.items():
            out[k] = _encode(v)

        return out

    @classmethod
    def decode(cls, r):
        """Converts any dictionary into a Document or a Leaf.

        :param r: The encoded data.
        """
        r = copy.deepcopy(r)
        builds = r.pop(KEY_BUILDS, {})
        def _decode(item):
            if isinstance(item, str) and item.startswith('?'):
                key = item[1:]
                if key not in builds:
                    try:
                        builds[key] = Component.load(*key.split(':'))
                    except FileNotFoundError:
                        raise NotFound(f'Cannot find component {key} in builds or registry')
                elif isinstance(builds[key], dict):
                    r = {'identifier': key.split(':')[-1], **builds[key]}
                    builds[key] = Component.decode({**r, KEY_BUILDS: r.get(KEY_BUILDS, {})})
                else:
                    assert isinstance(builds[key], Component), f'Expected a Component instance, got {type(builds[key])}'
                return builds[key]
            elif isinstance(item, str) and item.startswith(':blob:'):
                return dill_decode(item[len(':blob:'):])
            elif isinstance(item, str) and item.startswith(':import:'):
                path = item[len(':import:'):]
                parts = path.split('.')
                func = parts[-1]
                module = '.'.join(parts[:-1])
                module = importlib.import_module(module)
                return getattr(module, func)
            elif isinstance(item, list):
                return [_decode(x) for x in item]
            elif isinstance(item, dict):
                return {k: _decode(v) for k, v in item.items()}
            else:
                return item

        decoded = {k: _decode(v) for k, v in r.items()}
        return Document(decoded)

    def __repr__(self) -> str:
        return f'Document({repr(dict(self))})'
