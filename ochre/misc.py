from collections import defaultdict
import copy
import dataclasses as dc
import hashlib
from rich.tree import Tree
import types
import typing as t
from weakref import WeakKeyDictionary


def hash_item(item: t.Any) -> str:
    """Hash an item.

    :param item: The item to hash.
    """
    if item is None:
        return hashlib.sha256(('<NoneType>' + str(item)).encode()).hexdigest()
    if isinstance(item, bytearray):
        return hashlib.sha256(item).hexdigest()
    if isinstance(item, str):
        return hashlib.sha256(str(item).encode()).hexdigest()
    if isinstance(item, float):
        return hashlib.sha256(('<float>' + str(item)).encode()).hexdigest()
    if isinstance(item, int):
        return hashlib.sha256(('<int>' + str(item)).encode()).hexdigest()
    if isinstance(item, bool):
        return hashlib.sha256(('<bool>' + str(item)).encode()).hexdigest()
    if isinstance(item, (list, tuple)):
        hashes = []
        for i in item:
            hashes.append(hash_item(i))
        hashes = ''.join(hashes)
        return hashlib.sha256(hashes.encode()).hexdigest()
    if isinstance(item, dict):
        keys = sorted(item.keys())
        hashes = []
        for k in keys:
            hashes.append((hash_item(k), hash_item(item[k])))  # type: ignore[arg-type]
        return hashlib.sha256(str(hashes).encode()).hexdigest()
    return hashlib.sha256(str(item).encode()).hexdigest()



def replace_parameters(doc, placeholder: str = '!!!'):
    """
    Replace parameters in a doc-string with a placeholder.

    :param doc: Sphinx-styled docstring.
    :param placeholder: Placeholder to replace parameters with.
    """
    doc = [x.strip() for x in doc.split('\n')]
    lines = []
    had_parameters = False
    parameters_done = False
    for line in doc:
        if parameters_done:
            lines.append(line)
            continue

        if not had_parameters and line.startswith(':param'):
            lines.append(placeholder)
            had_parameters = True
            assert not parameters_done, 'Can\'t have multiple parameter sections'
            continue

        if had_parameters and line.startswith(':param'):
            continue

        if not line.strip() and had_parameters:
            parameters_done = True

        if had_parameters and not parameters_done:
            continue

        lines.append(line)

    if not had_parameters:
        lines = lines + ['\n' + placeholder]

    return '\n'.join(lines)


def extract_parameters(doc):
    """
    Extracts and organizes parameter descriptions from a Sphinx-styled docstring.

    :param doc: Sphinx-styled docstring.
                Docstring may have multiple lines
    """
    lines = [x.strip() for x in doc.split('\n')]
    was_doc = False
    import re

    params = defaultdict(list)
    for line in lines:
        if line.startswith(':param'):
            was_doc = True
            match = re.search(r':param[ ]+(.*):(.*)$', line)
            param = match.groups()[0]
            params[param].append(match.groups()[1].strip())
        if not line.startswith(':') and was_doc and line.strip():
            params[param].append(line.strip())
        if not line.strip():
            was_doc = False
    return params


_ATOMIC_TYPES = frozenset(
    {
        type(None),
        bool,
        int,
        float,
        str,
        # Other common types
        complex,
        bytes,
        # Other types that are also unaffected by deepcopy
        type(...),
        type(NotImplemented),
        types.CodeType,
        types.BuiltinFunctionType,
        types.FunctionType,
        type,
        range,
        property,
    }
)


def asdict(obj, *, copy_method=copy.copy) -> t.Dict[str, t.Any]:
    """Convert the dataclass instance to a dict.

    Custom ``asdict`` function which exports a dataclass object into a dict,
    with a option to choose for nested non atomic objects copy strategy.

    :param obj: The dataclass instance to
    :param copy_method: The copy method to use for non atomic objects
    """
    if not dc.is_dataclass(obj):
        raise TypeError("asdict() should be called on dataclass instances")
    return _asdict_inner(obj, dict, copy_method, top=True)


def _asdict_inner(obj, dict_factory, copy_method, top=False) -> t.Any:
    # source
    # https://github.com/python/cpython/blob/55e29a6100eb4aa89c3f510d4335b953364dd74e/Lib/dataclasses.py#L1428
    from .component import Component

    if type(obj) in _ATOMIC_TYPES:
        return obj
    elif not top and isinstance(obj, Component):
        return obj
    elif dc.is_dataclass(obj) and not isinstance(obj, type):
        # fast path for the common case
        return {
            f.name: _asdict_inner(getattr(obj, f.name), dict, copy_method)
            for f in dc.fields(obj)
        }
    elif isinstance(obj, tuple) and hasattr(obj, '_fields'):
        # obj is a namedtuple.  Recurse into it, but the returned
        # object is another namedtuple of the same type.  This is
        # similar to how other list- or tuple-derived classes are
        # treated (see below), but we just need to create them
        # differently because a namedtuple's __init__ needs to be
        # called differently (see bpo-34363).

        # I'm not using namedtuple's _asdict()
        # method, because:
        # - it does not recurse in to the namedtuple fields and
        #   convert them to dicts (using dict_factory).
        # - I don't actually want to return a dict here.  The main
        #   use case here is json.dumps, and it handles converting
        #   namedtuples to lists.  Admittedly we're losing some
        #   information here when we produce a json list instead of a
        #   dict.  Note that if we returned dicts here instead of
        #   namedtuples, we could no longer call asdict() on a data
        #   structure where a namedtuple was used as a dict key.

        return type(obj)(*[_asdict_inner(v, dict_factory, copy_method) for v in obj])
    elif isinstance(obj, (list, tuple)):
        # Assume we can create an object of this type by passing in a
        # generator (which is not true for namedtuples, handled
        # above).
        return type(obj)(_asdict_inner(v, dict_factory, copy_method) for v in obj)
    elif isinstance(obj, dict):
        if hasattr(type(obj), 'default_factory'):
            # obj is a defaultdict, which has a different constructor from
            # dict as it requires the default_factory as its first arg.
            result = type(obj)(getattr(obj, 'default_factory'))
            for k, v in obj.items():
                result[_asdict_inner(k, dict_factory, copy_method)] = _asdict_inner(
                    v, dict_factory, copy_method
                )
            return result
        return type(obj)(
            (
                _asdict_inner(k, dict_factory, copy_method),
                _asdict_inner(v, dict_factory, copy_method),
            )
            for k, v in obj.items()
        )
    else:
        return copy_method(obj)


def dict_to_ascii_table(d):
    """
    Return a single string that represents an ASCII table.

    Each key/value in the dict is a column.
    Columns are centered and padded based on the widest
    string needed (key or value).

    :param d: Convert a dictionary to a table.
    """
    if not d:
        return "<empty dictionary>"

    keys = list(d.keys())
    vals = list(d.values())

    # Determine the needed width for each column
    widths = [max(len(str(k)), len(str(v))) for k, v in zip(keys, vals)]

    def center_text(text, width):
        """Center text within a given width using spaces."""
        text = str(text)
        if len(text) >= width:
            return text  # already as wide or wider, won't cut off
        # Calculate left/right spaces for centering
        left_spaces = (width - len(text)) // 2
        right_spaces = width - len(text) - left_spaces
        return " " * left_spaces + text + " " * right_spaces

    # Build the header row (keys)
    header_row = " | ".join(center_text(k, w) for k, w in zip(keys, widths))

    # Build a separator row with + in the middle
    separator_row = "-+-".join("-" * w for w in widths)

    # Build the value row
    value_row = " | ".join(center_text(v, w) for v, w in zip(vals, widths))

    # Combine them with line breaks
    return "\n".join([header_row, separator_row, value_row])


def dict_to_tree(dictionary, root: str = 'root', tree=None):
    """
    Convert a dictionary to a `rich.Tree`.

    :param dictionary: Input dict
    :param root: Name of root
    :param tree: Ignore
    """
    if tree is None:
        tree = Tree(root)

    for key, value in dictionary.items():
        if isinstance(value, dict):
            # If the value is another dictionary, create a subtree
            subtree = tree.add(f"[bold yellow]{key}")
            dict_to_tree(value, root=root, tree=subtree)
        elif key == 'status':
            # Add the key and value as a leaf node
            if value == 'breaking':
                tree.add(f"[bold cyan]{key}: [red]{value}")
            elif value == 'update':
                tree.add(f"[bold cyan]{key}: [blue]{value}")
            else:
                tree.add(f"[bold cyan]{key}: [green]{value}")

    return tree

class lazy_classproperty:
    """
    Descriptor that computes the value once per owner class.

    It caches the computed value in a WeakKeyDictionary keyed by the owner.

    :param func: Function to compute the value.
    """

    def __init__(self, func):
        self.func = func
        self._cache = WeakKeyDictionary()

    def __get__(self, instance, owner):
        # Check if the owner class already has a cached value.
        if owner not in self._cache:
            # Compute and cache the value for this owner class.
            self._cache[owner] = self.func(owner)

        return self._cache[owner]
