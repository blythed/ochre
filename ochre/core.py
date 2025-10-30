from collections import defaultdict
import dataclasses as dc
import importlib
import shutil
import typing as t

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ochre.component import Component
from ochre.constants import REGISTRY
from ochre.job import Job

console = Console()


@dc.dataclass
class Plan:
    """A deployment plan that contains a list of events to be executed.

    This class is used to represent the deployment plan that will be executed
    by the cluster scheduler.

    :param jobs: A list of jobs to be executed.
    """

    jobs: t.Dict[str, t.List[Job]]
    executor: str = 'simple'

    def __post_init__(self):
        """Post-initialization checks."""
        if not self.jobs:
            raise ValueError('No jobs to execute!')
        module = importlib.import_module(f'ochre.executors.{self.executor}')
        self.executor_obj = getattr(module, 'Executor')()

    def apply(self):
        """Execute the plan by publishing the events to the cluster scheduler."""
        self.executor_obj.execute(self.jobs)

    def show(self):
        """Show the plan in a human-readable format."""
        console.print(Panel('Deployment plan', style='bold blue'))

        consolidated: list[tuple[str, str, str]] = []

        job_lookup = {}

        all_jobs = [job for jobs in self.jobs.values() for job in jobs]

        for idx, job in enumerate(all_jobs):
            int_dependencies = [
                str(job_lookup[job_id]) for job_id in job.dependencies
            ]
            dep_str = (
                f": deps→{','.join(int_dependencies)}" if int_dependencies else ''
            )
            consolidated.append(
                (str(idx), job.method.upper(), f'{job.huuid}{dep_str}')
            )
            job_lookup[job.job_id] = len(consolidated) - 1

        tbl = Table(show_header=True, header_style='bold magenta')
        tbl.add_column('#', style='cyan', no_wrap=True)
        tbl.add_column('Event type', style='magenta')
        tbl.add_column('Details', style='white')

        for row in consolidated:
            tbl.add_row(*row)

        console.print(
            Panel(tbl, title='Deployment plan – events overview', border_style='green')
        )


def apply(component: Component, force: bool = False, execute: bool = True, clean: bool = False, schedule: bool = True):
    """Apply a `superduper.Component`.

    :param component: The component to apply.
    :param force: Whether to force the application without confirmation.
    :param execute: Whether to execute the plan (or just show it).
    :param clean: Whether to delete resources that are no longer needed.
    :param schedule: Whether to (re-)schedule the component if it has a cron attribute
    """

    # -----------------------------------------------------------------------
    # 1. Show the component that is about to be applied
    # -----------------------------------------------------------------------
    console.print(Panel('Component to apply', style='bold green'))
    component.show()

    # -----------------------------------------------------------------------
    # 2. Analyse what needs to change
    # -----------------------------------------------------------------------
    try:
        previous = Component.load(component.component, component.identifier)
        existing_components = {}
        existing_components[previous.component + ':' + previous.identifier] = previous
        for child in previous.get_children(deep=True):
            existing_components[child.component + ':' + child.identifier] = child
    except FileNotFoundError:
        previous = None
        existing_components = {}

    jobs = _apply(
        object=component,
        jobs=defaultdict(list),
        existing_components=existing_components,
        processed_components=set(),
        clean=clean,
    )

    # Nothing to do? Exit early.
    if not jobs:
        console.print(Panel('No changes needed – doing nothing!', style='bold yellow'))
        return

    # -----------------------------------------------------------------------
    # 3. Present the diff (if any) and the deployment plan
    # -----------------------------------------------------------------------
    if previous:
        console.print(Panel('Changes detected to existing component', style='bold yellow'))
        previous.diff(component)

    plan = Plan(jobs=jobs)

    plan.show()

    # -----------------------------------------------------------------------
    # 4. Confirm (unless --force) and execute the plan
    # -----------------------------------------------------------------------

    if not force:
        if not click.confirm(
            '\033[1mPlease approve this deployment plan.\033[0m', default=True
        ):
            return plan

    if execute:
        component.save()
        plan.apply()

    if schedule:
        if getattr(component, 'cron', None):
            plan.executor_obj.schedule(component)
        else:
            if getattr(previous, 'cron', None):
                plan.executor_obj.cancel_schedule(component)

    print(f'Component available at: {REGISTRY}/{component.component}/{component.identifier}')
    return plan


def _apply(
    object: Component,
    jobs: t.Dict[str, Job],
    processed_components: t.Set,
    existing_components: t.Dict[str, Component],
    clean: bool = False,
):
    if object.huuid in processed_components:
        return {}

    try:
        current = existing_components[object.component + ':' + object.identifier]

        if object.status == 'error':
            apply_status = 'new'

        elif current.hash == object.hash:
            apply_status = 'same'

        elif current.uuid == object.uuid:
            apply_status = 'update'

        else:
            apply_status = 'breaking'

    except KeyError:
        apply_status = 'new'

    def wrapper(child):
        nonlocal jobs, processed_components

        j_ = _apply(
            object=child,
            jobs=jobs,
            processed_components=processed_components,
            existing_components=existing_components,
            clean=clean,
        )

        jobs.update(j_)
        processed_components |= {j__.rsplit('.')[0] for j__ in j_}
        return f'&:component:{child.huuid}'

    # ------------------------------------------------------------------
    # Map _apply over all child components
    # ------------------------------------------------------------------
    _ = list(map(wrapper, object.children))

    # ------------------------------------------------------------------
    # Build create / update/ delete events depending on apply_status
    # ------------------------------------------------------------------
    if apply_status == 'same':
        return jobs

    dependencies = []
    for c in object.children:
        for j in jobs.get(c.huuid, []):
            dependencies.append(j.job_id)

    component_data = object.encode()

    if apply_status  == 'new':
        if clean:
            jobs[object.huuid].append(
                Job(
                    method='delete',
                    dependencies=dependencies,
                    data=component_data,
                    raises=False
                )
            )
        jobs[object.huuid].append(
            Job(
                method='create',
                dependencies=dependencies,
                data=component_data,
            )
        )
    
    elif apply_status == 'breaking':
        jobs[object.huuid].append(
            Job(
                data=component_data,
                dependencies=dependencies,
                method='delete'
            )
        )
        jobs[object.huuid].append(
            Job(
                data=component_data,
                method='create',
                dependencies=[jobs[object.huuid][0].job_id]
            )
        )

    elif apply_status == 'update':
        jobs[object.huuid].append(
            Job(
                data=component_data,
                method='update',
                dependencies=dependencies,
            )
        )

    else:
        raise ValueError(f'Unknown apply_status: {apply_status}')

    return jobs


def destroy(component: Component, force: bool = False, execute: bool = True):
    """Destroy a `superduper.Component`.

    :param component: The component to destroy.
    :param force: Whether to force the destruction without confirmation.
    :param execute: Whether to execute the plan (or just show it).
    """
    # -----------------------------------------------------------------------
    # 1. Show the component that is about to be destroyed
    # -----------------------------------------------------------------------
    console.print(Panel('Component to destroy', style='bold red'))

    component.show()

    jobs = _destroy(
        object=component,
        jobs=defaultdict(list),
        processed_components=set(),
    )

    plan = Plan(jobs=jobs)

    plan.show()

    # -----------------------------------------------------------------------
    # 4. Confirm (unless --force) and execute the plan
    # -----------------------------------------------------------------------

    if not force:
        if not click.confirm(
            '\033[1mPlease approve this plan.\033[0m', default=True
        ):
            return plan

    if getattr(component, 'cron', None):
        plan.executor_obj.cancel_schedule(component)

    if execute:
        plan.apply()
        shutil.rmtree(f'./{REGISTRY}/{component.component}/{component.identifier}', ignore_errors=True)

    return plan


def _destroy(object: Component, jobs: t.Dict, processed_components: t.Set, parent_job_id: str | None = None):
    """Teardown a `superduper.Component`."""
    if object.huuid in processed_components:
        return {}

    component_data = object.encode()

    jobs[object.huuid].append(
        Job(
            method='delete',
            dependencies=[parent_job_id] if parent_job_id else [],
            data=component_data,
            raises=False
        )
    )

    # order of jobs is inverted for destroy - first parent is deleted, then children
    # otherwise parent cannot load

    def wrapper(child):
        nonlocal jobs, processed_components

        j_ = _destroy(child, jobs=jobs, processed_components=processed_components, parent_job_id=jobs[object.huuid][-1].job_id)

        jobs.update(j_)
        processed_components |= {j__.rsplit('.')[0] for j__ in j_}
        return f'&:component:{child.huuid}'

    # ------------------------------------------------------------------
    # Map _destroy over all child components
    # ------------------------------------------------------------------
    _ = list(map(wrapper, object.children))

    return jobs
