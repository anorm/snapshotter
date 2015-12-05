#!/usr/bin/env python
"""
Basic script to create an empty python package containing one module
"""
from os.path import expanduser
import ConfigParser
import datetime
import math
import os
import re
import shutil
import StringIO

SNAPSHOT_FORMAT = '%Y-%m-%d %H:%M:%S'

DEBUG=1
INFO=2
WARNING=3
ERROR=4

def log(severity, message):
    if severity == DEBUG:
        print("Debug:   " + message)
        pass
    elif severity == INFO:
        print("Info:    " + message)
    elif severity == WARNING:
        print("Warning: " + message)
    elif severity == ERROR:
        print("Error:   " + message)

def indent(text, amount, ch=' '):
    padding = amount * ch
    return padding + ('\n' + padding).join(text.split('\n'))

def mkdir_recursive(path):
    log(DEBUG, 'Creating directory: {}'.format(path))
    sub_path = os.path.dirname(path)
    if not os.path.exists(sub_path):
        mkdir_recursive(sub_path)
    if not os.path.exists(path):
        os.mkdir(path)

def parse_timedelta(s):
    years  = 0
    months = 0
    weeks  = 0
    days   = 0
    hours  = 0

    try:
        delta = {'y':0, 'm':0, 'w':0, 'd':0, 'h':0}
        for num,unit in re.findall(r'(\d+)(\D+)', s):
            unit = unit[0].lower()
            delta[unit] = delta[unit] + int(num)
    except:
        log(ERROR, 'Unknown time format: {}'.format(s))
        raise

    return datetime.timedelta(delta['y'] * 365 + delta['m'] * 30 + delta['d'], # days
                              0,          # seconds
                              0,          # microseconds
                              0,          # milliseconds
                              0,          # minutes
                              delta['h'], # hours
                              delta['w']) # weeks

class TaskKeepRule:
    def __init__(self, age, number):
        self._age    = parse_timedelta(age)
        self._number = int(number)

    def get_age(self):
        return self._age

    def get_number(self):
        return self._number

    def __str__(self):
        ret = StringIO.StringIO()
        ret.write('Keep rule\n')
        ret.write('  Age:    {}\n'.format(self._age))
        ret.write('  Number: {}\n'.format(self._number))
        return ret.getvalue()

class Task:
    def __init__(self, name):
        self._name       = name
        self._keep_rules = []

    def add_keep_rule(self, age, number):
        self._keep_rules.append(TaskKeepRule(age, number))
        self._keep_rules.sort(key=lambda rule: rule.get_age())

    def get_name(self):
        return self._name

    def find_expired_snapshots(self, snapshots):
        snapshots.sort(key=lambda snapshot: snapshot['age'])

        for snapshot in snapshots:
            snapshot["keep"] = False

        largest_ideal_age = datetime.timedelta()
        rule_start        = datetime.timedelta()
        for rule in self._keep_rules:
            log(DEBUG, "Checking rule {} - {}, keep {}".format(rule_start, rule.get_age(), rule.get_number()))
            for i in range(0, rule.get_number()):
                affected = [(j,snapshot) for j,snapshot in enumerate(snapshots) if snapshot['age'] < rule.get_age() and not snapshot['keep']]
                if affected:
                    delta_t = (rule.get_age() - rule_start) / rule.get_number()
                    ideal_age =  (delta_t * i) + (delta_t / 2) + rule_start
                    if ideal_age > largest_ideal_age:
                        largest_ideal_age = ideal_age
                    log(DEBUG, "  Ideal age {}: {}".format(i, ideal_age))
                    affected.sort(key=lambda snapshot: math.fabs((snapshot[1]['age'] - ideal_age).total_seconds()))
                    for _,s in affected:
                        log(DEBUG, '    Candidate: {} - {}'.format(s['name'], (s['age'] - ideal_age).total_seconds()))
                    snapshots[affected[0][0]]['keep'] = True
                    log(DEBUG, '  Keeping {} - age: {}'.format(affected[0][1]['name'], affected[0][1]['age']))
            rule_start = rule.get_age()

        # Keep all snapshots older than oldest rule
        for snapshot in snapshots:
            if snapshot['age'] > largest_ideal_age:
                log(DEBUG, 'Keeping {}, older than oldest rule'.format(snapshot['name']))
                snapshot['keep'] = True

        expired = [snapshot for snapshot in snapshots if not snapshot['keep']]

        return expired

    def __str__(self):
        ret = StringIO.StringIO()
        ret.write('Task: {}\n'.format(self._name))
        for rule in self._keep_rules:
            ret.write(indent(str(rule), 4))
            ret.write('\n')

        return ret.getvalue()

class Snapshotter:
    def __init__(self):
        self._snapshot_base_path = None
        self._tasks              = []

    def add_task(self, task):
        self._tasks.append(task)

    def set_snapshot_base_path(self, path):
        self._snapshot_base_path = path

    def take_snapshot(self):
        mkdir_recursive(self._snapshot_base_path)

        for task in self._tasks:
            log(DEBUG, 'Taking snapshot of task: {}'.format(task.get_name()))
            task_snapshot_base_path = os.path.join(self._snapshot_base_path, task.get_name())
            timestamp = datetime.datetime.now().strftime(SNAPSHOT_FORMAT)
            task_snapshot_path = os.path.join(task_snapshot_base_path, timestamp)
            mkdir_recursive(task_snapshot_path)
            log(DEBUG, "Timestamp is {}".format(str(timestamp)))

    def perform_cleanup(self):
        now = datetime.datetime.now()
        for task in self._tasks:
            log(DEBUG, 'Perfoming cleanup of {}'.format(task.get_name()))
            task_snapshot_base_path = os.path.join(self._snapshot_base_path, task.get_name())
            snapshots = []
            for snapshot_name in next(os.walk(task_snapshot_base_path))[1]:
                timestamp = datetime.datetime.strptime(snapshot_name, SNAPSHOT_FORMAT)
                age = now - timestamp
                snapshots.append( {'name':      snapshot_name,
                                   'path':      os.path.join(task_snapshot_base_path, snapshot_name),
                                   'timestamp': timestamp,
                                   'age':       age} )
            log(DEBUG, 'Found {} existing snapshots for {}'.format(len(snapshots), task.get_name()))
            expired = task.find_expired_snapshots(snapshots)
            for snapshot in expired:
                log(INFO, 'Removing expired snapshot {}'.format(snapshot["name"]))
                shutil.rmtree(snapshot['path'])

    def __str__(self):
        ret = StringIO.StringIO()
        ret.write('Snapshotter\n')
        ret.write('  Snapshot base path: {}\n'.format(self._snapshot_base_path))
        for task in self._tasks:
            ret.write(indent(str(task), 2))
            ret.write('\n')
        return ret.getvalue()

def main():
    config = ConfigParser.ConfigParser()

    log(DEBUG, 'Reading config file: {}'.format(expanduser("~/.snapshotter")))
    config.read(expanduser("~/.snapshotter"))

    snapshotter = Snapshotter()
    snapshotter.set_snapshot_base_path(config.get('General', 'snapshot_base_path'))

    for task_name in config.sections():
        if task_name == 'General':
            continue

        task = Task(task_name)

        keep = 1
        while config.has_option(task_name, 'keep.{}.age'.format(keep)):
            task.add_keep_rule(config.get(task_name, 'keep.{}.age'.format(keep)),
                               config.get(task_name, 'keep.{}.number'.format(keep)))
            keep = keep + 1

        snapshotter.add_task(task)

    snapshotter.take_snapshot()
    snapshotter.perform_cleanup()

if __name__ == '__main__':
    main()