import time
import random

n_sensors = 100
n_messages_per_sensor = 1200
initial_timestamp = time.time()
message_interval_ms = 100

bad_situations = []
for i in range(n_sensors):
	if random.randint(1,100) < 20:
		bad_situations.append(i)

sensor_initial_values = []
for i in range(n_sensors):
	sensor_initial_values.append(float(random.randint(150, 250))/10.)

for i in range(n_messages_per_sensor * n_sensors):
	
	prob = random.randint(1, 100) 
	if prob < 10:
		if i % n_sensors in bad_situations:
			value = float(random.randint(-20, 100)/10.)
		else:
			value = float(random.randint(-1, 1)/10.)
	elif prob > 98:
		value = -1.0

		sensor_initial_values[i % n_sensors] = min(250., sensor_initial_values[i % n_sensors] + value)
	print('%d %d %.02f' % (i % n_sensors, (i // n_sensors) * message_interval_ms + initial_timestamp, sensor_initial_values[i % n_sensors]))
