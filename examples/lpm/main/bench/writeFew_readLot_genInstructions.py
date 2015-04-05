from random import randint, choice
from string import ascii_uppercase

WRITE_RANDOM_FACTOR = 10
inserts = 0

print "Writing PUTS..."
with open('out.out', 'w') as wf:
  with open('dest_ips_100.txt') as f:
    for line in f:
      ip = line.strip("\n")
      r = randint(0, WRITE_RANDOM_FACTOR)
      if r is 3:
        inserts = inserts + 1
        prefix_length = randint(1, 32)
        priority = randint(0, 10)
        random_name = ''.join(choice(ascii_uppercase) for i in range(5))

        wf.write("PUT %s %s %s %s\n" % (ip, prefix_length, priority, random_name))

print "Total inserts: " + str(inserts)
print
print "Writing GETS..."

with open('out.out', 'a') as wf:
  with open('dest_ips_100.txt') as f:
    for line in f:
      ip = line.strip("\n")
      wf.write("GET %s\n" % (ip))
