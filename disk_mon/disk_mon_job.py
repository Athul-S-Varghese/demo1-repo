#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Author: Libin N George
File having the program to run for disk usage monitoring.

Should be run only once.

Requires itspelogger to be installed (can be used with both python2 and python3)

"""
import sys
import time
from itspelogger import Logger
import schedule
from disk_mon_lib import check_disk_usage, read_disk_mon_config

def main(configfile='config.ini'):
    """
    main function which schedules the program for disk usage check.
    Args:
        configfile(str): configfile for disk monitoring module.
    """
    logger = Logger("DISK_MON")
    config = read_disk_mon_config(configfile)
    for disk in config:
        schedule.every(config[disk]["schedule_frequency"]) \
                .seconds.do(check_disk_usage, logger,
                            config[disk]["filesystem"],
                            config[disk]["warning_threshold"],
                            config[disk]["critical_threshold"])
    schedule.run_all()
    while True:
        schedule.run_pending()
        time.sleep(schedule.idle_seconds())

if __name__ == '__main__':
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        print("Provide the configuration file as argument.")

