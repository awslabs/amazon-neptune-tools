import json
import csv
import pandas as pd

nodes = []
edges = []

def parseNode(dict):
    node = {}
    node["~id"] = dict["id"]
    node["~label"] = dict["label"]

    for key, value in dict["properties"].items():
        node[key] = value[0]["value"]

    return node

def parseEdge(dict):
    edge = {}
    edge["~id"] = dict["id"]
    edge["~label"] = dict["label"]
    edge["~from"] = dict["outV"]
    edge["~to"] = dict["inV"]

    return edge

#parse JSON start
def parseJSON():
    # change the input file name to local file 
    filename = 'data/cosmos-dump.json'
    with open(filename, 'r') as f:
        data = json.load(f)

        for dataItem in data:
            for nodedictionaryItem in dataItem.values():
                #top level nodes parsing
                nodes.append(parseNode(dict=nodedictionaryItem["key"]))

                for edgedictionaryItem in nodedictionaryItem["value"].values():
                    # edge parsing
                    edges.append(parseEdge(dict=edgedictionaryItem["key"]))

                    for nodedictionaryItemLevel2 in edgedictionaryItem["value"].values():
                       #inner level nodes parsing
                        nodes.append(parseNode(dict=nodedictionaryItemLevel2["key"]))

    print(f'total nodes found: {len(nodes)}')
    print(f'total nodes found: {len(edges)}')

    # remove duplicates nodes and save to csv
    dfnodes = pd.json_normalize(nodes)
    dfnodes = dfnodes.drop_duplicates()
    dfnodes.to_csv('./data/output/nodes.csv', index=False)

    # remove duplicates edges and save to csv
    dfedges = pd.json_normalize(edges)
    dfedges = dfedges.drop_duplicates()
    dfedges.to_csv('./data/output/edges.csv', index=False)

    print(f'total distinct nodes found: {len(dfnodes.index)}')
    print(f'total distinct edges found: {len(dfedges.index)}')


#parse JSON end

parseJSON()
