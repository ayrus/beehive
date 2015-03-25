import fileinput
from random import randint
if __name__ == '__main__':
    
    print 'writing to file PUT.txt'
    newFile = open('PUT.txt', 'w')
    j = 0
    written = 1
    for line in fileinput.input(['dest_ips.txt']):
        if j%5==0:
            print line
            ip = line.strip('\n')
            priority = randint(0,10)
            prefLen = randint(1, 32)
            name = 'IP:{}'.format(written)
            newFile.write('PUT {0:} {1:} {2:} {3:}'.format(ip, prefLen, priority, name))
            newFile.write('\n')
            written += 1
        j+= 1
    newFile.close()
    
    print 'Written ' + str(written - 1) + ' number of PUTs'       
    