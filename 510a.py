import sys

n = int(sys.stdin.readline().strip('\n'))

p = [[100000000 for i in range(8)] for i in range(n+1)]
l = [[0 for i in range(2)] for i in range(n)]
for i in range(n):
    line = sys.stdin.readline().strip('\n')
    c, v = list(map(str, line.split()))
    c=int(c)
    b = 0
    if(v.count("A")>0):
        b |= 1
    if(v.count("B")>0):
        b |= 2
    if(v.count("C")>0):
        b |= 4
    l[i]=(c, b)

p[0][0] = 0

for i in range(n):
    for j in range(8):
        p[i+1][j] = p[i][j]
    for j in range(8):
        
        if(p[i][j]+l[i][0]<p[i+1][j|l[i][1]]):
            p[i+1][j|l[i][1]] = p[i][j]+l[i][0]

if(p[n][7] == 100000000):
    print(-1)
else:
    print(p[n][7])
