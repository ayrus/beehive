import random
import sys

def nextGet(filename, prob):
    if not hasattr(nextGet, 'fp'):
        nextGet.fp = open(filename, "r")
    while True:
        next = nextGet.fp.readline().strip()
        if not next:
            nextGet.fp.close()
            nextGet.fp = open(filename, "r")
            continue   
        if random.random() <= prob:
            return 'GET ' + next

def nextPut(filename, prob):
    if not hasattr(nextPut, 'fp'):
        nextPut.fp = open(filename, "r")
    while True:
        next = nextPut.fp.readline().strip()
        if not next:
            nextPut.fp.close()
            nextPut.fp = open(filename, "r")
            continue   
        if random.random() <= prob:
            l = random.randint(1, 32)
            p = random.randint(1, 10)
            name = 'Name'
            return 'PUT ' + next + " " + str(l) + " " + str(p) + " " + name

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Invalid number of parameters. Pass the ip file name."
        exit(1)
    for i in xrange(20):
        print nextGet(sys.argv[1], 1)
        print nextPut(sys.argv[1], 0.5)
