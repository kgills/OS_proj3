#!/usr/bin/python


# sudo pkill -9 -f KooToueg


import sys, subprocess
print 'Opening config file:', str(sys.argv[1])
config_file = open(sys.argv[1], 'r')

# Edit this for your username
user="khg140030"

n = -1
cr_n = -1
minInst = -1
minSend = -1
messages = -1
n_m = 0
n_n = 0
n_cr = 0
machines = []
neighbors = []
crList = []

# get each line
for line in config_file:
	
	# Split the line by white space
	line_split = line.split()

	# Remove all of the stuff after the #
	if '#' in line_split:
		line_split = line_split[:line_split.index("#")]

	for word in line_split:
		if '#' in word:
			break

		# First number will be n
		if n == -1:
			n = int(word)
			continue

		if cr_n == -1:
			cr_n = int(word)
			continue

		if minInst == -1:
			minInst = int(word)
			continue

		if minSend == -1:
			minSend = int(word)
			continue

		if messages == -1:
			messages = int(word)
			continue

		# Get the machines
		if ((n_m < n) and (n != -1)):
			n_m = n_m+1
			machines.append(line_split)
			break

		# Get the cr iterations

for machine in machines:
	command = ["ssh","-o","StrictHostKeyChecking=no",user+"@"+machine[1],"killall","-u",user,"&"]
	print " ".join(command)
	# p = subprocess.Popen(command)
	# p.wait()
