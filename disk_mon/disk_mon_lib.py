#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Author: Libin N George
Module containing the functions for monitering the disk usage and related statictics.

Checkout https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html
for documentation examples.

Use pylint to check naming conventions and coding guidelines.

"""
import subprocess
import functools
from configparser import ConfigParser
import os
from itspelogger import Logger
import schedule



def check_mount(filesystem):
    '''
    Checks if the filesystem is mounted.
    Args:
        filesystem(str): filesystem to be checked.
    Returns:
        bool: True if filesystem is mounted, False otherwise.
    '''
    command = "grep -qs %s /proc/mounts" %(filesystem)
    return_value = os.system(command)
    return return_value == 0

def mount_filesystem(filesystem):
    """
    Mount the filesystem to /tmp/disk as read-only.
    Args:
        filesystem(str): filesystem to be mounted.
    Returns:
        bool: True if successful, False otherwise.
    """
    command = "mkdir /tmp/disk"
    os.system(command)
    command = "mount %s /tmp/disk -r" %(filesystem)
    return_value = os.system(command)
    return return_value == 0

def umount_filesystem(filesystem):
    """
    Unmount the filesystem.
    Args:
        filesystem(str): filesystem to be umounted.
    Returns:
        bool: True if successful, False otherwise.
    """
    command = "umount %s " %(filesystem)
    return_value = os.system(command)
    return return_value == 0

def get_all_filesystems():
    """
    returns all filesystems which are available in the system.
    Returns(list):
        list of string containing all filesystems.
    """
    child = subprocess.Popen(['lsblk', '-o', 'KNAME'], stdout=subprocess.PIPE)
    output = child.communicate()[0].strip().split(b'\n')[1:]
    filesystems = []
    for kname in output:
        filesystems.append("/dev/" + kname.decode("utf-8"))
    return filesystems

def read_disk_mon_config(configfile):
    """
    Reads config file and returns contents in dict data.
    Args:
        configfile(str): configfile for disk monitoring module.
    Returns:
        dictonary in having configuration for each disk/filesytem to be monitored.
    """
    config = ConfigParser()
    config.read(configfile)
    config_result = {}
    disks = config.get("DISKMONITOR", "DISKS").split(",")
    for disk in disks:
        config_result[disk] = {}
        config_result[disk]["schedule_frequency"] = int(config.get(disk, 'SCHEDULE_FREQUENCY',
                                                                   fallback=300))
        config_result[disk]["critical_threshold"] = int(config.get(disk, 'CRITICAL', fallback=90))
        config_result[disk]["warning_threshold"] = int(config.get(disk, 'WARNING', fallback=80))
        config_result[disk]["filesystem"] = config.get(disk, 'FILESYSTEM', fallback='/')
    return config_result


def get_disk_usage(location='/'):
    """
    Gets Disk Usage statictics for the device (filesystem) having the location.
    returns the disk Usage statictics
    Args:
        location(str): The location or the filesystem
    Returns:
        {
            'Filesystem': '/dev/sda7',
            'Size': '549G',
            'Used': '83G',
            'Avail': '439G',
            'Use%': '16%',
            'Mounted_on': '/'
        }
    """
    child = subprocess.Popen(['df', '-h', location], stdout=subprocess.PIPE)
    output = child.communicate()[0].strip().split(b'\n')
    data = output[1].split()
    result = {
        "Filesystem": data[0].decode("utf-8"),
        "Size" :data[1].decode("utf-8"),
        "Used" :data[2].decode("utf-8"),
        "Avail":data[3].decode("utf-8"),
        "Use%" :data[4].decode("utf-8"),
        "Mounted_on":data[5].decode("utf-8")
    }
    return result

def critical_alert(logger, disk, usage):
    """
    To be called when critical state for disk usage
    Args:
        logger: logger object
        disk: filesystem eg. /dev/sda7
        usage: usage percent eg. usage=95 when disk usage is 95%

    """
    logger.critical("Disk usage had reached %d%% for %s", usage, disk)

def warning_alert(logger, disk, usage):
    """
    To be called when warning state for disk usage
    Args:
        logger: logger object
        disk: filesystem eg. /dev/sda7
        usage: usage percent eg. usage=95 when disk usage is 95%
    """
    logger.warning("Disk usage had reached %d%% for %s", usage, disk)

def info_alert(logger, disk, usage):
    """
    To be called when normal state for disk usage.
    Args:
        logger: logger object
        disk: filesystem eg. /dev/sda7
        usage: usage percent eg. usage=95 when disk usage is 95%
    """
    logger.info("Current Disk usage is %d%% for %s", usage, disk)

def catch_exceptions(cancel_on_failure=False):
    """
    Warpper for schedule.
    """
    def catch_exceptions_decorator(job_func):
        """
        decorator funtion for the job_func
        """
        @functools.wraps(job_func)
        def wrapper(*args, **kwargs):
            """
            Inner warpper for the function.
            Assumes that first argument is a logger object.
            """
            try:
                return job_func(*args, **kwargs)
            except:
                import traceback
                logger = args[0]
                logger.error(traceback.format_exc())
                if cancel_on_failure:
                    return schedule.CancelJob
        return wrapper
    return catch_exceptions_decorator

@catch_exceptions(cancel_on_failure=False)
def check_disk_usage(logger, filesystem, warning_threshold, critical_threshold):
    """
    Check filesystem (filesystem) disk usage percent and triggers
    required alert.
    logger: logger object for logging.
    filesystem: filesystem port eg. /dev/sda7
    warning_threshold: warning threshold for disk usage
    critical_threshold: critical threshold for disk usage
    """
    disk_statictics = get_disk_usage(filesystem)
    if disk_statictics["Filesystem"] != filesystem:
        filesystems = get_all_filesystems()
        if filesystem not in filesystems:
            logger.error("Filesystem %s not found in the system.", filesystem)
            return
        if not check_mount(filesystem):
            if mount_filesystem(filesystem):
                disk_statictics = get_disk_usage(filesystem)
                umount_filesystem(filesystem)
    usage_percent = int(disk_statictics["Use%"][:-1])
    if usage_percent >= critical_threshold:
        critical_alert(logger, filesystem, usage_percent)
    elif usage_percent >= warning_threshold:
        warning_alert(logger, filesystem, usage_percent)
    else:
        info_alert(logger, filesystem, usage_percent)
