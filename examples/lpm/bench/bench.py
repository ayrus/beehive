import json, sys
from matplotlib import pyplot as plt

if __name__ == '__main__':
    putPoints = []
    getPoints = []


    json_data=open(sys.argv[1])
    data = json.load(json_data)
    num_put = 0
    num_get = 0
    totalPut = 0
    totalGet = 0
    for d in data:
        if (d['method'] == "PUT"):
            totalPut += d['dur']
            num_put += 1
            putPoints.append(d['dur'])
        if (d['method'] == "GET"):
            totalGet += d['dur']
            num_get += 1
            getPoints.append(d['dur'])
    averagePut = float(totalPut / num_put) / 10e9
    averageGet = float(totalGet / num_get) / 10e9

    #plt.figure(0)
    plt.scatter(xrange(num_put), putPoints, color='red')
    #plt.scatter(xrange(num_get), getPoints, color='blue')

    plt.xlabel('Request Number')
    plt.ylabel('Latency (nanoseconds)')
    plt.title(sys.argv[1])

    plt.show()

    print "Average PUT: %.10f" % averagePut
    print "Average GET: %.10f" % averageGet
