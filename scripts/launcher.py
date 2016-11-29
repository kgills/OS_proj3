#!/usr/bin/python

import sys, subprocess

print 'Opening config file:', str(sys.argv[1])
config_file = open(sys.argv[1], 'r')

# Edit this to the path where the executables are
path="~/Workspace/OS_proj3/"

# Edit this for your username
user="mbs140230"

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

		# Get the neighbors
		if ((n_n < n) and (n != -1)):
			n_n = n_n+1
			neighbors.append(line_split)
			break

		if ((n_cr < cr_n) and (cr_n != -1)):
			n_cr = n_cr+1
			crList.append(line_split)
			break

		# Get the cr iterations

# Remove all of the extra characters
i = 0
j = 0
for neighbor in neighbors:
	j = 0
	for word in neighbor:
		word = word.replace(")", "")
		word = word.replace("(", "")
		word = word.replace(",", "")
		neighbors[i][j] = word
		j=j+1
	i=i+1

print crList
i = 0
j = 0
for cr in crList:
	j = 0
	for word in cr:
		word = word.replace(")", "")
		word = word.replace("(", "")
		word = word.replace(",", " ")
		crList[i][j] = word
		j=j+1
	crList[i] = crList[i][0].split(" ")
	i=i+1

# Build and execute the commands

print "n "+str(n)
print "cr_n "+str(cr_n)
print "minInst "+str(minInst)
print "minSend "+str(minSend)
print "messages "+str(messages)
print "n_m "+str(n_m)
print "n_n "+str(n_n)
print machines
print neighbors
print crList

i = 0
for machine in machines:

	command = ["java","-cp",path,"KooToueg",str(n), str(i), str(cr_n), str(minInst), str(minSend), str(messages)]

	for machine2 in machines:
		command = command + machine2[1:]

	command = command +[str(len(neighbors[i]) - 1)]+neighbors[i][1:]

	for cr in crList:
		command = command+[cr[0]]

	for cr in crList:
		command = command+[cr[1]]


	command = ["ssh","-o","StrictHostKeyChecking=no",user+"@"+machine[1]]+command
	i = i+1

	print " ".join(command)
	p=subprocess.Popen(command)

p.wait()
