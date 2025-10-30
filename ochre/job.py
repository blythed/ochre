import dataclasses as dc
import datetime
import os
import shutil
from traceback import format_exc
import typing as t
import uuid


@dc.dataclass
class Future:
    """
    Future output.

    :param job_id: job identifier
    """
    job_id: str


import dataclasses as dc
import typing as t
from .constants import LENGTH_UUID, REGISTRY


@dc.dataclass
class Job:
    data: t.Dict
    time: str = dc.field(default_factory=lambda: str(datetime.datetime.now()))
    job_id: str = dc.field(default_factory=lambda: str(uuid.uuid4()).replace('-', '')[:LENGTH_UUID])
    method: t.Literal['create', 'update', 'delete'] = 'create'
    dependencies: t.List[str] = dc.field(default_factory=list)
    raises: bool = True  # whether to raise exceptions during execution

    def __post_init__(self):
        assert self.method in ['create', 'update', 'delete'], "Method must be one of 'create', 'update', 'delete'"

    @property
    def component(self):
        """Return the component name."""
        return self.data['component']

    @property
    def identifier(self):
        """Return the component identifier."""
        return self.data['identifier']

    @property
    def uuid(self):
        """Return the component uuid."""
        return self.data['uuid']

    @property
    def huuid(self):
        """Return the hashed uuid."""
        return f'{self.component}/{self.identifier}/{self.uuid}.{self.method}'

    def execute(self):
        """Execute the job event.

        :param db: Datalayer instance
        """
        previous_version = None
        from .component import Component
        try:
            os.makedirs(f"{REGISTRY}/{self.component}/{self.identifier}/.status", exist_ok=True)
            with open(f"{REGISTRY}/{self.component}/{self.identifier}/.status/in_progress", "w") as f:
                pass

            if os.path.exists(f"{REGISTRY}/{self.component}/{self.identifier}/component.json"):
                previous_version = Component.load(self.component, self.identifier)

            try:
                c = Component.decode(self.data)
            except Exception as e:
                print(format_exc())
                raise 

            getattr(c, self.method)()

            with open(f"{REGISTRY}/{self.component}/{self.identifier}/.status/complete", "w") as f:
                pass

            os.remove(f"{REGISTRY}/{self.component}/{self.identifier}/.status/in_progress")

            if self.method in ['create', 'update']:
                c.save()
            elif self.method == 'delete':
                if previous_version:
                    previous_version.rm()
            else:
                raise ValueError(f"Cannot execute method {self.method}")

        except Exception as e:
            with open(f"{REGISTRY}/{self.component}/{self.identifier}/.status/error", "w") as f:
                f.write(str(e))

            if self.raises:
                if previous_version:
                    previous_version.save()
                raise e