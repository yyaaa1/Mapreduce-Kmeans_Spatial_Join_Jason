import random

random.seed(2000)

ptsfile = 'pts.txt'
with open(ptsfile, "w") as f:
    for _ in range(0,15000000):
        f.write(str(random.randint(1,10000))+','+str(random.randint(1,10000))+'\n')



sqrsfile = 'sqrs.txt'
with open(sqrsfile, "w") as f2:
    for i in range(1,1000001):
        x = random.randint(1,9995)
        y = random.randint(1,9980)
        w = random.randint(1,5)
        h = random.randint(1,20)
        f2.write('r' + str(i)+','+str(x)+','+str(y)+','+str(x+w)+','+str(y+h)+'\n')

