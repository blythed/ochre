import os
import sys
import typing as t

from crontab import CronTab
from .base import Executor as BaseExecutor

from ochre.component import Component
from ochre.constants import REGISTRY
from ochre.job import Job


class Executor(BaseExecutor):

    def cancel_schedule(self, component: Component):
        if not os.path.exists('crontab.txt'):
            os.system('touch crontab.txt')
        cron = CronTab(tabfile='crontab.txt', user=True)
        cron.remove_all(comment=f'{component.component}/{component.identifier}')
        cron.write()
        print(f"Cancelled scheduling for component {component.identifier}")

    def schedule(self, component: Component):
        if not getattr(component, 'cron', None):
            raise ValueError("Component does not have a 'cron' attribute for scheduling.")

        print(f"Scheduling component {component.identifier} with cron '{component.cron}'")
        cron = CronTab(tabfile='crontab.txt', user=True)
        cmd = f'cd {os.getcwd()} && {sys.executable} -m ochre reapply {component.component} {component.identifier} > {REGISTRY}/{component.component}/{component.identifier}/cron.log 2>&1'
        job = cron.new(command=cmd, comment=f'{component.component}/{component.identifier}')
        job.setall(component.cron)
        cron.write()
        print(f"Scheduled job for component {component.identifier} with cron '{component.cron}'")

    def execute(self, jobs: t.Dict[str, t.List[Job]]):
        for component in jobs:
            for job in jobs[component]:
                job.execute()