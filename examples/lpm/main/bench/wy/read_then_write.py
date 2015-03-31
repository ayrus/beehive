import sys
from string import ascii_uppercase
from random import randint, choice

WRITE_INTERVAL = 2000
WRITES_PER_FILES = 20000

def generatePUT(IP):
    prefix_length = randint(1, 32)
    priority = randint(0, 10)
    random_name = ''.join(choice(ascii_uppercase) for i in range(5))
    return ("PUT " + IP + " " + str(prefix_length) + " " + str(priority) + " " + random_name)

def generateGET(IP):
    return ("GET " + IP)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Invalid number of parameters. Pass the ip file name."
        exit(1)
    ipFile= open(sys.argv[1], "r")
    i = 0
    for line in ipFile:
        if (i  % WRITES_PER_FILES) == 0:
            currFile = open("ip" + str(i / WRITES_PER_FILES) + ".txt", "w")
        IP = line.strip("\n")
        if (i > 0) and (i % WRITE_INTERVAL == 0):
            currFile.write(generateGET(IP))
        else:
            currFile.write(generatePUT(IP))
        currFile.write("\n")
        i += 1
