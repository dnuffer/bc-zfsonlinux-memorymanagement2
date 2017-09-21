#!/usr/bin/env python3
#
# Author: Peter Maloney
#
# Automatically adjusts the zfsonlinux memory limits as needed to free up or use up memory for max performance
# run it in screen, or nohup, or a cronjob.
# It should prevent multiple instances accidentally running.
#
# Licensed as GNU GPLv2 or newer

import sys
import argparse
import subprocess
import time
import os
import logging
import traceback
import fcntl

#====================
# constants
#====================

zfs_arc_meta_limit = "/sys/module/zfs/parameters/zfs_arc_meta_limit"
zfs_arc_max = "/sys/module/zfs/parameters/zfs_arc_max"

# from https://stackoverflow.com/questions/11269575/how-to-hide-output-of-subprocess-in-python-2-7
try:
    from subprocess import DEVNULL # py3k
except ImportError:
    # for older python that doesn't have subprocess.DEVNULL
    DEVNULL = open(os.devnull, 'wb')

#====================
# logging
#====================

logger = None

def log_verbose(self, message, *args, **kws):
    if self.isEnabledFor(logging.VERBOSE):
        self.log(logging.VERBOSE, message, *args, **kws)

def logging_init():
    global logger
    
    if not logger:
        logging.VERBOSE = 15
        logging.addLevelName(logging.VERBOSE, "VERBOSE")
        logging.Logger.verbose = log_verbose

        formatter = logging.Formatter(
            fmt='%(asctime)-15s.%(msecs)03d %(levelname)s: %(message)s',
            datefmt="%Y-%m-%d %H:%M:%S"
            )

        handler = logging.StreamHandler()
        handler.setFormatter(formatter)

        logger = logging.getLogger("zfs_repl4")

        logger.addHandler(handler)

    if cfg.debug:
        logger.setLevel(logging.DEBUG)
    elif cfg.verbose:
        logger.setLevel(logging.VERBOSE)
    elif cfg.quiet:
        logger.setLevel(logging.WARNING)
    else:
        logger.setLevel(logging.INFO)

#====================

# from https://stackoverflow.com/questions/6086976/how-to-get-a-complete-exception-stack-trace-in-python
def format_exception():
    exception_list = traceback.format_stack()
    exception_list = exception_list[:-2]
    exception_list.extend(traceback.format_tb(sys.exc_info()[2]))
    exception_list.extend(traceback.format_exception_only(sys.exc_info()[0], sys.exc_info()[1]))

    exception_str = "Traceback (most recent call last):\n"
    exception_str += "".join(exception_list)
    # Removing the last \n
    exception_str = exception_str[:-1]

    return exception_str


def active_pools():
    global cfg
    return get_pools() if cfg.pools == [] else cfg.pools


def get_pools():
    args = ["zpool", "list", "-H", "-o", "name"]

    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    ret = []
    for line in p.stdout:
        line = line.decode("utf-8").splitlines()[0]
        ret += [line]

    p.wait()

    if( p.returncode == 0 ):
        return ret

    raise Exception("Failed to list datasets. returncode = %s\n%s" % (p.returncode, read_file(p.stderr)))


def get_ram():
    args = ["free", "-m"]

    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    ret = []
    for line in p.stdout:
        line = line.decode("utf-8").splitlines()[0]
        ret += [line]

    p.wait()
    
    if( p.returncode == 0 ):
        return ret

    raise Exception("Failed to get ram, returncode = %s\n%s" % (p.returncode, read_file(p.stderr)))


free_version = None
def detect_free_version():
    ram = get_ram()
    
    if "available" in ram[0]:
        free_version = "3.3.10"
    else:
        free_version = "3.3.9"


def get_ram_total():
    ram = get_ram()
    
    field = None
    if free_version == "3.3.9":
        line = ram[1]
        field = int(line.split()[1])
    else:
        line = ram[1]
        field = int(line.split()[1])

    return field


def get_ram_used():
    ram = get_ram()
    
    if free_version == "3.3.9":
        line = ram[2]
        field = int(line.split()[2])
    else:
        line = ram[1]
        split = [int(v) for v in line.split()[1:]]
        
        # hmm... these are different, but which is better?
        ## 1. used+shared+buff/cache
        #field = split[1] + split[3] + split[4]
        
        # 2. total-available
        field = split[0] - split[5]

    return field


def drop_caches():
    args = ["sysctl", "vm.drop_caches=3"]

    p = subprocess.Popen(args, stdout=DEVNULL, stderr=subprocess.PIPE)

    p.wait()

    if( p.returncode == 0 ):
        return

    raise Exception("Failed to drop caches. returncode = %s\n%s" % (p.returncode, read_file(p.stderr)))


def restart_crashplan():
    args = ["service", "crashplan", "restart"]

    p = subprocess.Popen(args, stdout=DEVNULL, stderr=subprocess.PIPE)

    p.wait()

    if( p.returncode == 0 ):
        return

    raise Exception("Failed to restart crashplan. returncode = %s\n%s" % (p.returncode, read_file(p.stderr)))


def set_primarycache(value):
    for pool in active_pools():
        args = ["zfs", "set", "primarycache=%s" % value, pool]

        p = subprocess.Popen(args, stdout=DEVNULL, stderr=subprocess.PIPE)

        p.wait()

        if( p.returncode == 0 ):
            continue

        raise Exception("Failed to drop caches. returncode = %s\n%s" % (p.returncode, read_file(p.stderr)))


def auto_limits():
    global cfg
    
    total = get_ram_total()

    # the min and max setting that limit_gb can be adjusted to
    # new dynamic version... assuming max is based on max_percent
    if not cfg.min_gb:
        cfg.min_gb = int(total/1024 * (cfg.min_percent / 100) / 4)
    if not cfg.max_gb:
        cfg.max_gb = int(total/1024 * (cfg.max_percent / 100))

    # the min amd max of the "good" range where we would like to be
    # the min is used when the used memory is close to the max_good_percent
    # the max is used when the used memory is close to the min_good_percent
    # tuning:
    # - lowering min_good_gb or raising max_good_percent until you don't get "xx.xx % is > good; new setting" as often any more
    # - your goal is so that you are in the higher good range most of the time, but lower than max_percent
    if not cfg.min_good_gb:
        cfg.min_good_gb = int(total/1024 * (cfg.min_good_percent / 100)) #TODO: why was this /2 in the bash version?
    if not cfg.max_good_gb:
        cfg.max_good_gb = int(total/1024 * (cfg.max_good_percent / 100))


def adjust(percent, limit_gb, message):
    max_gb = int(limit_gb*1024*1024*1024)
    meta_limit = int((limit_gb-2)*1024*1024*1024)
    
    print("%2.2f %% is %s; new setting is %2.2f GB, meta_limit = %s, max = %s" % (percent, message, limit_gb, meta_limit, max_gb))
    
    with open(zfs_arc_meta_limit, "w") as f:
        f.write(str(meta_limit))

    with open(zfs_arc_max, "w") as f:
        f.write(str(max_gb))


def limit_init():
    global cfg
    
    # the target limit to set in the sysfs tunables
    # the initial value is set to match the old value unless it is out of range of min_gb and max_gb
    with open(zfs_arc_max, "r") as f:
        old_limit_b = int(f.readline())
        
    limit_gb = round(old_limit_b / 1024/1024/1024, 2)
    if limit_gb < cfg.min_gb:
        limit_gb = cfg.min_gb
    elif limit_gb > cfg.max_gb:
        limit_gb = cfg.max_gb
    
    total = get_ram_total()
    used = get_ram_used()
    percent = round(100*used/total, 2)
    
    adjust(percent, limit_gb, "before initial settings")

    return limit_gb


def run():
    limit_gb = limit_init()

    # time to sleep after every action (including after the idle sleep)
    sleep_time_action = 3
    # time to sleep after deciding no action is needed
    sleep_time_idle = 10

    primarycache = None

    while True:
        try:
            time.sleep(sleep_time_action)
            
            total = get_ram_total()
            used = get_ram_used()
            
            percent = round(100*used/total, 2)
            
            if percent > cfg.max_panic_percent:
                if primarycache == "metadata":
                    primarycache = "none"
                else:
                    primarycache = "metadata"

                print("%2.2f %% exceeds max_panic_percent! dropping cache and setting primarycache=%s" % (percent, primarycache))
                
                drop_caches()
                set_primarycache(primarycache)
                restart_crashplan()
                # we assume limit_gb is already cfg.min_gb, so we don't set it, and if it's not, then the next loop will set it anyway
                
            elif percent > cfg.max_percent:
                if limit_gb > cfg.min_gb:
                    limit_gb -= 1
                    if limit_gb < cfg.min_gb:
                        limit_gb = cfg.min_gb
                    adjust(percent, limit_gb, "> max")
                else:
                    print("%2.2f %% is > max; but limit_gb = %s is too low already; skipping" % (percent, limit_gb))
                    time.sleep(sleep_time_idle)
            elif percent < cfg.min_percent:
                if primarycache != "all":
                    print("%2.2f %% is < min; setting primarycache=all" % (percent))
                    set_primarycache("all")
                    primarycache = "all"
                    
                if limit_gb < cfg.max_gb:
                    limit_gb += 1
                    if limit_gb > cfg.max_gb:
                        limit_gb = cfg.max_gb
                    adjust(percent, limit_gb, "< min")
                else:
                    print("%2.2f %% is < min; but limit_gb = %s is too high already; no action" % (percent, limit_gb))
                    time.sleep(sleep_time_idle)
            elif percent > cfg.max_good_percent:
                # here we don't add one over and over, instead we just set it to the calculated good low.
                # that keeps the limits from flapping less, and is the reason we have this good range rather than a specific target we expect we can stay at
                limit_gb = cfg.min_good_gb
                adjust(percent, limit_gb, "> good")

                time.sleep(sleep_time_idle)
            elif percent < cfg.min_good_percent:
                # same explanation as "> good" comment above
                limit_gb = cfg.max_good_gb
                adjust(percent, limit_gb, "< good")

                time.sleep(sleep_time_idle)
            else:
                # the relative place (0.00 to 1.00) where percent is between min_good_percent and max_good_percent 
                relative_goodness = (percent - cfg.min_good_percent) / (cfg.max_good_percent - cfg.min_good_percent)
                # the relative place where the limit should be relative to the min_good_gb and max_good_gb
                limit_gb = cfg.min_good_gb + ( (1 - relative_goodness) * (cfg.max_good_gb - cfg.min_good_gb) )
                limit_gb = round(limit_gb, 2)

                adjust(percent, limit_gb, "good: relative_goodness = %s" % round(relative_goodness,6))

                time.sleep(sleep_time_idle)
        except KeyboardInterrupt:
            raise
        except:
            s = format_exception()
            logger.error("Something went wrong... sleeping 30s and continuing:\n%s" % s)
            time.sleep(30)


if __name__ == "__main__":
    global cfg
    
    parser = argparse.ArgumentParser(description='Replicate zfs datasets/pools to other pools in local or remote hosts.')
    parser.add_argument('-d', '--debug', action='store_const', const=True,
                    help='enable debug level logging')
    parser.add_argument('-v', '--verbose', action='store_const', const=True, default=False,
                    help='verbose mode')
    parser.add_argument('-q', '--quiet', action='store_const', const=True, default=False,
                    help='quiet mode')
    
    parser.add_argument('-n', '--dry-run', action='store_const', const=True, default=False,
                    help='dry run mode, which prints out commands instead of running them: snapshot, send, recv')
    
    # These are the limits for the actual reported used RAM
    # I have a NAS machine with 64GB RAM and 59T/196T used where the default gets me 92.77% RAM used, 
    # but on another busier machine with 192GB RAM and 94T/108T used, the default panics often, and -g 75 keeps it around 88% RAM used
    parser.add_argument('-m', '--min-percent', action='store', type=float, default=80.0,
                    help='minimum used memory percentage before trying to adjust to a good range')
    parser.add_argument('-M', '--max-percent', action='store', type=float, default=94.0,
                    help='maximum used memory percentage before trying to adjust to a good range')
    parser.add_argument('-g', '--min-good-percent', action='store', type=float, default=89.0,
                    help='minimum used memory percentage that defines the good range to stay in. If this script panics often on your system, you probably have to significantly lower this setting.')
    parser.add_argument('-G', '--max-good-percent', action='store', type=float, default=93.0,
                    help='maximum used memory percentage that defines the good range to stay in')
    
    # These are the limits of what goes in the zfs tunables, which might be higher or way lower than actual used RAM
    parser.add_argument('--min-gb', action='store', type=float, default=None,
                    help='tunable setting you should probably not touch, and is normally calculated from min-percent')
    parser.add_argument('--max-gb', action='store', type=float, default=None,
                    help='tunable setting you should probably not touch, and is normally calculated from max-percent')
    parser.add_argument('--min-good-gb', action='store', type=float, default=None,
                    help='tunable setting you should probably not touch, and is normally calculated from min-good-percent')
    parser.add_argument('--max-good-gb', action='store', type=float, default=None,
                    help='tunable setting you should probably not touch, and is normally calculated from max-good-percent')
    
    parser.add_argument('-P', '--max-panic-percent', action='store', type=float, default=97.0,
                    help='the point where we panic and immediately do everything to lower meomry')
    
    parser.add_argument('pools', action='store', nargs="*",
                    help='list of pools, used only for setting primarycache for panic mode (optional, default is all pools)')
    
    cfg = parser.parse_args()

    logging_init()
    auto_limits()
    
    # check that the tunable files actually exist and quit ... in case they change them in the future
    fail = False
    if not os.path.exists(zfs_arc_meta_limit):
        print("ERROR: file %s does not exist" % zfs_arc_meta_limit)
        fail = True
    if not os.path.exists(zfs_arc_max):
        print("ERROR: file %s does not exist" % zfs_arc_max)
        fail = True
    if fail:
        exit(1)
    
    got_lock = False
    lockFile = "/var/run/zfsonlinux_memorymanagement.lock"
    try:
        with open(lockFile, "wb") as f:
            try:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                got_lock = True
            except: # python3.4.x has BlockingIOError here, but python 3.2.x has IOError here... so just don't use those class names
                if cfg.verbose:
                    s = format_exception()
                    logger.error(s)
                    
                logger.error("Could not obtain lock; another process already running? quitting")
                exit(1)
            if got_lock:
                run()
    except KeyboardInterrupt:
        raise
    finally:
        if got_lock:
            os.remove(lockFile)
