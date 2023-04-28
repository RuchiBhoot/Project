import pyspark

from pyspark import SparkContext
sc = SparkContext.getOrCreate()

graph = "ORKUT"

worker1 = "triangle/edge/"+graph+"/BTH/WorkerData_BTH_0.txt"
worker2 = "triangle/edge/"+graph+"/BTH/WorkerData_BTH_1.txt"
worker3 = "triangle/edge/"+graph+"/BTH/WorkerData_BTH_2.txt"
worker4 = "triangle/edge/"+graph+"/BTH/WorkerData_BTH_3.txt"
worker5 = "triangle/edge/"+graph+"/BTH/WorkerData_BTH_4.txt"
worker6 = "triangle/edge/"+graph+"/BTH/WorkerData_BTH_5.txt"
worker7 = "triangle/edge/"+graph+"/BTH/WorkerData_BTH_6.txt"
worker8 = "triangle/edge/"+graph+"/BTH/WorkerData_BTH_7.txt"

w1rdd = sc.textFile(worker1)
w1rdd = w1rdd.map(lambda x: x.split(',')).map(lambda x: [int(x[1]),[int(i) for i in x[3:] if int(i)%15==0]]).filter(lambda x : x[0]%15==0 and len(x[1])>0) 
w2rdd = sc.textFile(worker2)
w2rdd = w2rdd.map(lambda x: x.split(',')).map(lambda x: [int(x[1]),[int(i) for i in x[3:] if int(i)%15==0]]).filter(lambda x : x[0]%15==0 and len(x[1])>0)
w3rdd = sc.textFile(worker3)
w3rdd = w3rdd.map(lambda x: x.split(',')).map(lambda x: [int(x[1]),[int(i) for i in x[3:] if int(i)%15==0]]).filter(lambda x : x[0]%15==0 and len(x[1])>0)
w4rdd = sc.textFile(worker4)
w4rdd = w4rdd.map(lambda x: x.split(',')).map(lambda x: [int(x[1]),[int(i) for i in x[3:] if int(i)%15==0]]).filter(lambda x : x[0]%15==0 and len(x[1])>0)
w5rdd = sc.textFile(worker5)
w5rdd = w5rdd.map(lambda x: x.split(',')).map(lambda x: [int(x[1]),[int(i) for i in x[3:] if int(i)%15==0]]).filter(lambda x : x[0]%15==0 and len(x[1])>0)
w6rdd = sc.textFile(worker6)
w6rdd = w6rdd.map(lambda x: x.split(',')).map(lambda x: [int(x[1]),[int(i) for i in x[3:] if int(i)%15==0]]).filter(lambda x : x[0]%15==0 and len(x[1])>0)
w7rdd = sc.textFile(worker7)
w7rdd = w7rdd.map(lambda x: x.split(',')).map(lambda x: [int(x[1]),[int(i) for i in x[3:] if int(i)%15==0]]).filter(lambda x : x[0]%15==0 and len(x[1])>0)
w8rdd = sc.textFile(worker8)
w8rdd = w8rdd.map(lambda x: x.split(',')).map(lambda x: [int(x[1]),[int(i) for i in x[3:] if int(i)%15==0]]).filter(lambda x : x[0]%15==0 and len(x[1])>0)
print("here 1")
#print(w1rdd.count())

d1 = w1rdd.reduceByKey(lambda x,y :x+y)
d2 = w2rdd.reduceByKey(lambda x,y :x+y)
d3 = w3rdd.reduceByKey(lambda x,y :x+y)
d4 = w4rdd.reduceByKey(lambda x,y :x+y)
d5 = w5rdd.reduceByKey(lambda x,y :x+y)
d6 = w6rdd.reduceByKey(lambda x,y :x+y)
d7 = w7rdd.reduceByKey(lambda x,y :x+y)
d8 = w8rdd.reduceByKey(lambda x,y :x+y)
print("here 6")
def joinfunc(a,b):
        r = []
        if(a is not None):
                r=r+a
        if(b is not None):
                r=r+b
        return r

u1 = w1rdd.map(lambda x: x[0]).distinct().map(lambda x: (x,[x+0.0]))
u2 = w2rdd.map(lambda x: x[0]).distinct().map(lambda x: (x,[x+0.1]))
u3 = w3rdd.map(lambda x: x[0]).distinct().map(lambda x: (x,[x+0.2]))
u4 = w4rdd.map(lambda x: x[0]).distinct().map(lambda x: (x,[x+0.3]))
u5 = w5rdd.map(lambda x: x[0]).distinct().map(lambda x: (x,[x+0.4]))
u6 = w6rdd.map(lambda x: x[0]).distinct().map(lambda x: (x,[x+0.5]))
u7 = w7rdd.map(lambda x: x[0]).distinct().map(lambda x: (x,[x+0.6]))
u8 = w8rdd.map(lambda x: x[0]).distinct().map(lambda x: (x,[x+0.7]))

print("here 2")
#print(u1.collect())
#print(u2.collect())
#print(u3.collect())
j = u1.fullOuterJoin(u2)
j=j.map(lambda x : (x[0],joinfunc(x[1][0],x[1][1])))
j = j.fullOuterJoin(u3)
j=j.map(lambda x : (x[0],joinfunc(x[1][0],x[1][1])))
j = j.fullOuterJoin(u4)
j=j.map(lambda x : (x[0],joinfunc(x[1][0],x[1][1])))
j = j.fullOuterJoin(u5)
j=j.map(lambda x : (x[0],joinfunc(x[1][0],x[1][1])))
j = j.fullOuterJoin(u6)
j=j.map(lambda x : (x[0],joinfunc(x[1][0],x[1][1])))
j = j.fullOuterJoin(u7)
j=j.map(lambda x : (x[0],joinfunc(x[1][0],x[1][1])))
j = j.fullOuterJoin(u8)
j=j.map(lambda x : (x[0],joinfunc(x[1][0],x[1][1])))
#print(j.collect())
print("here 3")

w1j = d1.leftOuterJoin(j)
#print(w1j.collect())

w2j = d2.leftOuterJoin(j)
#print(w2j.collect())

w3j = d3.leftOuterJoin(j)
w4j = d4.leftOuterJoin(j)
w5j = d5.leftOuterJoin(j)
w6j = d6.leftOuterJoin(j)
w7j = d7.leftOuterJoin(j)
w8j = d8.leftOuterJoin(j)
#print(w3j.collect())
print("here 4")
w1j = w1j.map(lambda x:[x[0]+0.0,[0],[[i+0.0,0] for i in x[1][0]]+[[i,1] for i in x[1][1]]]).map(lambda x : str(x).replace(" ",""))
#res1 = w1j.collect()

w2j = w2j.map(lambda x:[x[0]+0.1,[0],[[i+0.1,0] for i in x[1][0]]+[[i,1] for i in x[1][1]]]).map(lambda x : str(x).replace(" ",""))
#res2 = w2j.collect()

w3j = w3j.map(lambda x:[x[0]+0.2,[0],[[i+0.2,0] for i in x[1][0]]+[[i,1] for i in x[1][1]]]).map(lambda x : str(x).replace(" ",""))
#res3 = w3j.collect()

w4j = w4j.map(lambda x:[x[0]+0.3,[0],[[i+0.3,0] for i in x[1][0]]+[[i,1] for i in x[1][1]]]).map(lambda x : str(x).replace(" ",""))
#res4 = w4j.collect()

w5j = w5j.map(lambda x:[x[0]+0.4,[0],[[i+0.4,0] for i in x[1][0]]+[[i,1] for i in x[1][1]]]).map(lambda x : str(x).replace(" ",""))
#res5 = w5j.collect()

w6j = w6j.map(lambda x:[x[0]+0.5,[0],[[i+0.5,0] for i in x[1][0]]+[[i,1] for i in x[1][1]]]).map(lambda x : str(x).replace(" ",""))
#res6 = w6j.collect()

w7j = w7j.map(lambda x:[x[0]+0.6,[0],[[i+0.6,0] for i in x[1][0]]+[[i,1] for i in x[1][1]]]).map(lambda x : str(x).replace(" ",""))
#res7 = w7j.collect()

w8j = w8j.map(lambda x:[x[0]+0.7,[0],[[i+0.7,0] for i in x[1][0]]+[[i,1] for i in x[1][1]]]).map(lambda x : str(x).replace(" ",""))
#res8 = w8j.collect()

res = w1j.union(w2j).union(w3j).union(w4j).union(w5j).union(w6j).union(w7j).union(w8j)
print("here 9")
#print(w1j.count())
res.coalesce(1).saveAsTextFile(graph + "/BTHsmallinp.txt")
print("completed")
'''
res = res1+res2+res3+res4+res5+res6+res7+res8
print("here 5")
fileout = open("/data/hadoop/edge_centre/ORKUT/dbh/input.txt","w")
for i in res[:-1]:
        s = str(i).replace(" ","")
        print(s)
        fileout.write(s)
        fileout.write("\n")
s = str(res[-1]).replace(" ","")
fileout.write(s)
fileout.close()
'''
