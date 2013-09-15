# coding: utf-8

import networkx
get_ipython().magic(u'pinfo networkx.read_edgelist')
get_ipython().magic(u'pinfo networkx.read_edgelist')
g = networkx.read_edgelist('46af3069-17e1-471e-9e6a-20d157039613_000000.gz', delimiter='\x1')
g = networkx.read_edgelist('46af3069-17e1-471e-9e6a-20d157039613_000000.gz', delimiter='\0x1')
g = networkx.read_edgelist('46af3069-17e1-471e-9e6a-20d157039613_000000', delimiter='\0x1')
g
g.size()
len(g)
g.nodes()
f = file('46af3069-17e1-471e-9e6a-20d157039613_000000')
f.readline()
g = networkx.read_edgelist('46af3069-17e1-471e-9e6a-20d157039613_000000', delimiter='\x01')
g
g.size()
cc = networkx.connected_components(g)
len(cc)
cc[0]
cc[1]
cc[2]
cc[3]
cc[4]
cc[5]
cc[10]
cc[15]
cc[100]
cc[500]
cc[700]
cc[200]
cc[20]
cc[10]
cc[5]
cc[0]
g.remove_node('81231231233')
g.remove_node('81231231232')
g.remove_node('81231231231')
cc[5]
g.remove_node('89999999999')
g.remove_node('84545454545')
cc = networkx.connected_components(g)
cc[5]
cc[4]
cc[0]
[i for i in cc if 'puritanne@gmail.com' in i]
g.remove_node('80939293929')
g.remove_node('89276878768')
cc = networkx.connected_components(g)
[i for i in cc if 'puritanne@gmail.com' in i]
[i for i in cc if '89263215767' in i]
g.add_edge('89263215767', 'puritanne@gmail.com')
cc = networkx.connected_components(g)
[i for i in cc if '89263215767' in i]
len(cc)
import csv
w = csv.writer(file('sessions.csv', 'w+'))
for i in cc:
    for t in i:
        w.writerow([i[0], t])
        
get_ipython().system(u'less sessions.csv')
get_ipython().magic(u'hist')
get_ipython().magic(u'hist ?')
get_ipython().magic(u'pinfo %hist')
get_ipython().magic(u'pinfo %save')
