from __future__ import (absolute_import, division, print_function)
# (C) 2017 Allyson Bowles <github.com/akatch>
# (C) 2017 Ansible Project
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

# Make coding more python3-ish
import time
import socket
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

"""
Sort stats descending by value
"""

class AnsibleStatisticsReport():

    STATISTIC_SCHEMA_PREFIX = 'hireology.deployments'

    def __init__(self):
        self.statistics = dict()

    def add_statistic(self, stat_name, stat_start, stat_end):
        stat_value = self.calculate_statistic(stat_start, stat_end)
        stat_schema = '%s.%s' % (self.STATISTIC_SCHEMA_PREFIX, stat_name)
        self.statistics[stat_schema] = '%f %d' % (stat_value, stat_end)
        return self.statistics[stat_schema]

    def calculate_statistic(self, stat_start, stat_end):
        stat_value = stat_end - stat_start
        return stat_value

    def get_statistics(self):
        return self.statistics

    def sort_statistics(self):
        # FIXME sort by v.split().0
        sorted_stats = sorted(self.statistics.iteritems(), key=lambda (k, v): (v, k), reverse=True)
        return sorted_stats

    def print_statistics(self):
        for statname, statval in self.sort_statistics():
            print('%s %s' % (statname, statval))
        return

    def send_statistics_report(self, stats_host, stats_port):
        # TODO delegate this to a play host
        if stats_host and stats_port:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((stats_host, int(stats_port)))

            for statname, statval in self.sort_statistics():
                statline = '%s %s\n' % (statname, statval)
                sock.sendall(statline)

            sock.shutdown(socket.SHUT_WR)
            sock.close()
        #else raise a warning but proceed
        return

class CallbackModule(CallbackBase):
    """
    This callback module collects completion time by task, play, and playbook, then ships the
    results to a user-specified stats collector, such as Graphite.
    """
    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = 'aggregate'
    CALLBACK_NAME = 'collect_stats'
    CALLBACK_NEEDS_WHITELIST = True

    STATS_HOST = '127.0.0.1'
    STATS_PORT = 2003

    def _get_task_display_name(self, task):
        display_name = task.get_name().strip().split(" : ")

        task_display_name = display_name[-1]
        if task_display_name.startswith("include"):
            return
        else:
            return task_display_name

    def __init__(self):
        super(CallbackModule, self).__init__()
        self.prev_task_start_time = float(0)
        self.prev_task_name = None
        self.stat = AnsibleStatisticsReport()

    def v2_playbook_on_play_start(self, play):
        super(CallbackModule, self).v2_playbook_on_play_start(play)
        return

    def v2_playbook_on_task_start(self, task, is_conditional):
        prev_task_end_time = time.time()

        super(CallbackModule, self).v2_playbook_on_task_start(task, is_conditional)

        # process the *last* task that ran (if there was one)
        if self.prev_task_start_time > 0:
            self.stat.add_statistic(self.prev_task_name,
                                    self.prev_task_start_time,
                                    prev_task_end_time)

        # persist data for *current* task so it is available when the next task runs
        # final task will be processed when playbook stats run
        self.prev_task_start_time = prev_task_end_time
        self.prev_task_name = self._get_task_display_name(task).replace(" ", "_").lower()
        return

    def v2_playbook_on_start(self, playbook):
        self.playbook_start_time = time.time()
        super(CallbackModule, self).v2_playbook_on_start(playbook)
        return

    def v2_playbook_on_stats(self, stats):
        playbook_end_time = time.time()
        super(CallbackModule, self).v2_playbook_on_stats(stats)

        # process the last task
        self.stat.add_statistic(self.prev_task_name,
                                self.prev_task_start_time,
                                playbook_end_time)

        # process playbook runtime
        self.stat.add_statistic('playbook_runtime', self.playbook_start_time, playbook_end_time)

        #if verbosity > 0:
        #    self.stat.print_statistics()
        self.stat.send_statistics_report(self.STATS_HOST, self.STATS_PORT)
        return
