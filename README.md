# Overview

`ochre` is a framework for building pipelines in Python, in a way which mirrors frameworks such as Terraform. These pipelines are:

- Declarative
- Compositional
- Idempotent
- Incremental

Since the framework is in Python, it's suitable for things other than pure infrastructure as code. In particular, `ochre` was originally designed for state-management jobs in AI. For example:

- provisioning state in AI applications
- synchronizing data-sources from a remote source, to a database
- keeping a vector index over a database table up-to-date.
- preparing data for model training and training amodel on that data

**Note** `ochre` is a distillation of the `apply` algorithm of [`superduper-framework`](https://github.com/superduper-io/superduper). If you are interested in database integration as a first-class citizen in your pipelines, then you might prefer to look there first.

## Usage

### 1. Develop `Component` sub-classes with CRUD semantics

```python
from ochre import Component


class MyProcessor(Component):
    breaks = ('chunk_size', 'model')  # define breaking changes
    
    # add dataclass parameters - accessible via `self.model` etc..
    model: str = 'some-model'
    chunk_size: int = 500
    label: str = 'test'
    my_subcomponent: Component  # subcomponents jobs go first on apply

    def create(self):
        # called the first time the instance is applied
        print(f"CREATE {self.identifier}")

    def read(self):
        # called everytime the class is loaded from file
        print(f"READ {self.identifier}")

    def update(self):
        # called if the class is re-applied with non-breaking changes
        print(f"UPDATE {self.identifier}")

    def delete(self):
        # teardown method to reverse state changes
        print(f"DELETE {self.identifier}")

```


### 2. Build pipelines as parametrized compositions of `Component` sub-classes

```python
main = MyProcessor('test', my_subcomponent=Component('sub'))
```

### 3. Apply with one-command

All `.create()` methods called recursively as jobs, with corresponding state established.

In `bash`

```bash
ochre apply examples/my_script.py
```

Or in `python`:

```python
import ochre

ochre.apply(main)
```

Deployment plan:

| Event type | Details |
| --- | --- |
| `CREATE` | `Component/sub/009bfeff53633822.create` |
| `CREATE` | `MyProcessor/test/3d5a194b731b4b68.create: deps→0` |

### 4. Update with the same command

Non-breaking change - runs `main.update()`, but not on child:

```python
import ochre

main.label = 'other'

ochre.apply(main)
```

Deployment plan: 

| Event type | Details |
| --- | --- |
| `UPDATE` | `MyProcessor/test/3d5a194b731b4b68.update` |


Breaking change - runs `main.delete()`and then `main.create`:

```python
main.chunk_size = 100

ochre.apply(main)
```

Deployment plan:

| Event type | Details |
| --- | --- |
| `DELETE` | `MyProcessor/test/3d5a194b731b4b68.delete` |
| `CREATE` | `MyProcessor/test/3d5a194b731b4b68.create` |

## Explanation

An `ochre.Component` is a dataclass, whose data/ parameters are annotated exactly 
as Python dataclasses (without the `@dc.dataclass`). Each `ochre.Component` has 4 methods
which take 0 parameters: `.create()`, `.read()`, `.update()`, `.delete()`.

When `ochre.apply(ComponentImpl(**params))` is called, then the following cascade is called:

1. When the component is instantiated, `.read()` is called
2. If the `Component` has not yet been applied, then `.create` is called
3. If the `Component` has been applied, (judged by `component.identifier`), but the data on the class has changed 
   then the `.update()` method 
4. If the data in the class in `Component.breaks` has changed, then this corresponds to a breaking change. 
   The old version will be destroyed with `.delete()` and a new version established.
5. If the `Component` has not changed, then nothing happens
6. If the `Component` contains other `Component` instances in it's fields, then those are applied with the same rules also

When `ochre.destroy(component)`  is called, then the `.delete()` method of the `Component` and recursively its subcomponents
are called.

## Hashing algorithm

`ochre` includes a crypotgraphic scheme for detecting pipeline changes. This is how the framework detects what has changed — and whether it’s a breaking or non-breaking change — without you writing custom diff logic.

Each `ochre.Component` includes 2 Merkle trees in `Component.merkle_tree` and `Component.merkle_tree_breaks`. These 
hash the dataclass parameters, and `Component.metadata_fields` of the class instance, looking for changes recursively into sub-components. These 2 Merkle trees feed into `Component.uuid` and `Component.hash` which look for breaking and non-breaking changes respectively.

If a change is found in breaking fields, then this is reflected in `Component.uuid`. All other changes are reflected in `Component.hash`. `ochre` uses these hashes to decide whether to call `create`, `update` or `delete` when `apply(...)` is called.

## Why do this?

Computations in AI often require an initial setup, and then some bookkeeping/ housekeeping on a schedule. See the following table for examples:

| TASK | CREATE | UPDATE | DELETE |
| --- | --- | --- | --- |
| Vector-search | Vectorize all items in a table | Vectorize new/ changed items in a table | Drop the search index and/ or table |
| Train model | Create features, fit, deploy model | Create latest features, retrain, deploy model | Delete/ teardown the model |
| Data-sync | Pull data from remote into database | Pull latest data and updates into database | Delete table |

In the `./examples` directory you will find a range of examples illustrating the usage of `ochre`.