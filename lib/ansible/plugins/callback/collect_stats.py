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

    def _get_task_display_name(self, task):
        display_name = task.get_name().strip().split(" : ")

        task_display_name = display_name[-1]
        if task_display_name.startswith("include"):
            return
        else:
            return task_display_name

    def _process_task_time(self):
        task_start_time = float(time.time())

        if self.prev_task_start_time > 0:
            task_name = self.prev_task_name
            task_runtime = float(task_start_time - self.prev_task_start_time)
            task_runtime_statistic = self._format_statistic(task_runtime, task_start_time)
            self.statistics[self._format_statistic_schema(task_name)] = task_runtime_statistic
        self.prev_task_start_time = task_start_time
        return

    def _display_statistics(self):
        for statname, statval in self.statistics.iteritems():
            print('%s %s' % (statname, statval))
        return

    def _format_statistic(self, stat_value, stat_time):
        statistic = '%f %i' % (stat_value, int(stat_time))
        return statistic

    def _format_statistic_schema(self, stat_name):
        schema_prefix = 'hostname.playbook_yml'
        statistic_schema = '%s.%s' % (schema_prefix, stat_name)
        return statistic_schema

    def __init__(self):
        super(CallbackModule, self).__init__()
        self.prev_task_start_time = float(0)
        self.prev_task_name = None
        self.statistics = dict()

    def v2_playbook_on_play_start(self, play):
        super(CallbackModule, self).v2_playbook_on_play_start(play)
        return

    def v2_playbook_on_task_start(self, task, is_conditional):
        super(CallbackModule, self).v2_playbook_on_task_start(task, is_conditional)
        self._process_task_time()
        self.prev_task_name = self._get_task_display_name(task).replace(" ", "_").lower()
        return

    def v2_playbook_on_start(self, playbook):
        self.playbook_start_time = time.time()
        super(CallbackModule, self).v2_playbook_on_start(playbook)
        return

    def v2_playbook_on_stats(self, stats):
        self.playbook_end_time = time.time()
        self._process_task_time()
        super(CallbackModule, self).v2_playbook_on_stats(stats)
        playbook_runtime = float(self.playbook_end_time - self.playbook_start_time)
        playbook_runtime_stat = self._format_statistic(playbook_runtime, self.playbook_end_time)
        self.statistics[self._format_statistic_schema('playbook_runtime')] = playbook_runtime_stat
        self._display_statistics()
        return
