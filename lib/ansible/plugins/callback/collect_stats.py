from __future__ import (absolute_import, division, print_function)
# (C) 2017 Allyson Bowles <github.com/akatch>
# (C) 2017 Ansible Project
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

# Make coding more python3-ish
import time
from ansible.plugins.callback import CallbackBase

__metaclass__ = type

DOCUMENTATION = '''
    callback: collect_stats
    type: aggregate
    short_description: collect and ship timing statistics for playbooks, plays, and tasks
    version_added: "2.0"
    description:
      - Collects elapsed time per task
      - Ships statistics to your preferred stats collector
    requirements:
      - whitelisting in configuration
'''


class CallbackModule(CallbackBase):
    """
    This callback module collects completion time by task, play, and playbook, then ships the
    results to a user-specified stats collector, such as Graphite.
    """
    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = 'aggregate'
    CALLBACK_NAME = 'collect_stats'
    CALLBACK_NEEDS_WHITELIST = True

    def _process_task_time(self):
        task_start_time = float(time.time())

        if self.prev_task_start_time > 0:
            task_name = self.prev_task_name
            task_runtime = float(task_start_time - self.prev_task_start_time)
            #print('%s runtime: %f' % (task_name, task_runtime))
            self.statistics[task_name] = task_runtime
        self.prev_task_start_time = task_start_time
        return

    def __init__(self):
        super(CallbackModule, self).__init__()
        self.prev_task_start_time = float(0)
        self.current_task_name = None
        self.prev_task_name = None
        self.statistics = dict()

    def v2_playbook_on_play_start(self, play):
        super(CallbackModule, self).v2_playbook_on_play_start(play)
        return

    def v2_playbook_on_task_start(self, task, is_conditional):
        super(CallbackModule, self).v2_playbook_on_task_start(task, is_conditional)
        self.current_task_name = task.get_name()
        self._process_task_time()
        self.prev_task_name = self.current_task_name
        return

    def v2_playbook_on_start(self, playbook):
        super(CallbackModule, self).v2_playbook_on_start(playbook)
        return

    def v2_playbook_on_stats(self, stats):
        self._process_task_time()
        super(CallbackModule, self).v2_playbook_on_stats(stats)
        for statname, statval in self.statistics.iteritems():
            print('%s: %f' % (statname, statval))
        return
