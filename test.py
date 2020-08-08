import sched
import time

event_schedule = sched.scheduler(time.time, time.sleep)

def do_something():
    print("Hello, World!")
    event_schedule.enter(30, 1, do_something )

event_schedule.enter(1, 1, do_something)
event_schedule.run()
