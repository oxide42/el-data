import signal
import sys
import gc
import time
import schedule
import threading

import core.service

_terminated = False
_jobs_thread = None

def refresh_data():
    print("Running job 'refresh_data' ...")
    import os, gc
    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
    try:
        core.service.refresh_data()
    except:
        print("")
    gc.collect()
    print("Job 'refresh_data' done.")

def jobs():
    print("Scheduling jobs ...")
    schedule.every().day.at('00:45').do(refresh_data)
    print("Done.")
    while not _terminated:
        schedule.run_pending()
        time.sleep(2)

def signal_handler(sig, frame):
    end_jobs()
    sys.exit(0)

def start_jobs():
    global _jobs_thread

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)

    _jobs_thread = threading.Thread(target=jobs)
    _jobs_thread.start()

def end_jobs():
    global _terminated
    _terminated = True
