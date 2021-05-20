from time import sleep
from rethinkdb import RethinkDB
import random
import threading
import time

from rethinkdb.ast import Date

r = RethinkDB()

r.connect('localhost', 49154).repl()
current_velocity = 1
current_altitude = 1
start_time = time.time()

for i in range(1000):
    
    accel = random.uniform(5.0, 10.0)    
    current_velocity = current_velocity + accel
    time_elapsed = (time.time() - start_time);
    current_altitude = current_altitude + (current_velocity * time_elapsed)
    start_time = time.time()
    r.db('test').table('Data').insert([{"name":"Velocity", "value": current_velocity}]).run()
    r.db('test').table('Data').insert([{"name":"Acceleration", "value": accel}]).run()
    r.db('test').table('Data').insert([{"name":"Altitude", "value": current_altitude}]).run()
    sleep(0.1)