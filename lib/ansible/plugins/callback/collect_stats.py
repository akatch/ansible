# (C) 2017 Allyson Bowles <github.com/akatch>
# (C) 2017 Ansible Project
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import (absolute_import, division, print_function)
import time
import socket
import os.path
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


class AnsibleStatisticsReport():

    DEFAULT_STAT_PREFIX = 'ansible'

    # TODO make this configurable
    def __init__(self):
        self.statistics = dict()
        self.prefix = self.DEFAULT_STAT_PREFIX

    def set_prefix(self, new_prefix):
        self.prefix = new_prefix
        return self.prefix

    def add_statistic(self, stat_name, stat_start, stat_end):
        stat_value = self.calculate_statistic(stat_start, stat_end)
        stat_schema = '%s.%s' % (self.prefix, stat_name)
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

    # process each statistic period ( playbook, play, task, handler_task, etc )
    def process_statistics_period(self, prev_start_time, prev_end_time, prev_name):
        # process the *last* period that ran
        formatted_stat_name = self.format_statistic_name(prev_name)
        self.add_statistic(formatted_stat_name, prev_start_time, prev_end_time)

        # persist data for *current* task so it is available when the next task runs
        # final task will be processed when playbook stats run
        prev_start_time = prev_end_time
        return prev_start_time

    def format_statistic_name(self, task_name):
        # TODO strip special characters other than - and _
        # task.name from handlers still includes role prefix, so remove it
        formatted_task_name = task_name.replace(" ", "_").replace(".", "_").lower()
        return formatted_task_name

    def send_statistics_report(self, stats_host, stats_port):
        # TODO delegate this to a play host
        # currently the control box must be able to connect directly to STATS_HOST:STATS_PORT
        if stats_host and stats_port:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((stats_host, int(stats_port)))

            for statname, statval in self.sort_statistics():
                statline = '%s %s\n' % (statname, statval)
                sock.sendall(statline)

            # ensure the socket closes cleanly
            time.sleep(0.5)
            sock.shutdown(socket.SHUT_WR)
            sock.close()
        # TODO else raise a warning but proceed
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

    # TODO make these configurable
    STATS_HOST = '127.0.0.1'
    STATS_PORT = 2003
    STATS_PREFIX = 'hireology'

    def __init__(self):
        super(CallbackModule, self).__init__()
        self.prev_task_start_time = float(0)
        self.prev_task_name = None
        stat_prefix = self.STATS_PREFIX
        self.stat = AnsibleStatisticsReport()
        # TODO cant we just provide this at class instantiation
        self.stat.set_prefix(stat_prefix)

    def v2_playbook_on_play_start(self, play):
        super(CallbackModule, self).v2_playbook_on_play_start(play)
        return

    def v2_playbook_on_task_start(self, task, is_conditional):
        prev_task_end_time = time.time()

        super(CallbackModule, self).v2_playbook_on_task_start(task, is_conditional)

        # process the *last* task that ran (if there was one)
        if self.prev_task_start_time > 0:
            prev_task_end_time = self.stat.process_statistics_period(self.prev_task_start_time, prev_task_end_time, self.prev_task_name)

        self.prev_task_start_time = prev_task_end_time
        self.prev_task_name = task.name.split(":")[-1].strip().replace(" ", "_").lower()
        return

    def v2_playbook_on_cleanup_task_start(self, task):
        self.v2_playbook_on_task_start(task, False)

    def v2_playbook_on_handler_task_start(self, task):
        self.v2_playbook_on_task_start(task, False)

    def v2_playbook_on_start(self, playbook):
        self.playbook_start_time = time.time()
        # add playbook filename to stats prefix
        # FIXME i think this is naughty
        playbook_name = os.path.split(playbook._file_name)[1].replace('.', '_')
        self.stat.set_prefix('%s.%s' % (self.stat.prefix, playbook_name))
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
        #if play not in check_mode:
        self.stat.send_statistics_report(self.STATS_HOST, self.STATS_PORT)
        return
