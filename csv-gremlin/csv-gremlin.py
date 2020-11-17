# Copyright 2020 Amazon.com, Inc. or its affiliates.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file.
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions
# and limitations under the License.

'''
@author:     krlawrence
@copyright:  Amazon.com, Inc. or its affiliates
@license:    Apache2
@contact:    @krlawrence
@deffield    created:  2020-11-17

Overview

This file contains the definition for a NeptuneCSVReader class. Its purpose is
to provide a tool able to read CSV files that use the Amazon Neptune formatting
rules and generate Gremlin steps from that data.  Those Gremlin steps can then
be used to load the data into any TinkerPop compliant graph that allows for user
defined Vertex and Edge IDs. 

The tool can detect and handle both the vertex and edge CSV file formats. It
recognizes the Neptune type specifiers, such as 'age:Int' and defaults to String
if none is provided in a column header.  It also handles sparse rows ',,,' etc.

The tool also allows you to specify the batch size for vertices and edges. The
default is set to 10 for each currently. Batching allows multiple vertices or
edges, along with their properties, to be added in a single Gremlin query.

Gremlin steps that represent the data in the CSV are written to 'stdout'. 

Current Limitations

Currently the tool does not support the cardinality column header such as
'age:Int(single)'. Likewise lists of values declared using the '[]' column
header modifier are not supported.     

In this initial version none of the special column headers are allowed to
be omitted. Those being (~id, ~label, ~from, ~to).
'''
import csv
import sys 

class NeptuneCSVReader:
    VERSION = 0.1
    VERSION_DATE = '2020-11-07'
    INTEGERS = ('BYTE','SHORT','INT','LONG')
    FLOATS = ('FLOAT','DOUBLE')

    def __init__(self, vbatch=1, ebatch=1):
        self.vertex_batch_size = vbatch
        self.edge_batch_size = ebatch

    def get_batch_sizes(self):
        return {'vbatch': self.vertex_batch_size,
                'ebatch': self.edge_batch_size}
        
    def set_batch_sizes(self, vbatch=1, ebatch=1):
        self.vertex_batch_size = vbatch
        self.edge_batch_size = ebatch

    def process_vertices(self,reader):
        count = 0
        batch = "g"
        for row in reader:
            batch += self.process_vertex_row(row)
            count += 1
            if count == self.vertex_batch_size:
                count = 0
                print(batch)
                batch = 'g'
        if batch != 'g':        
            print(batch)

    def process_edges(self,reader):
        count = 0
        batch = 'g'
        for row in reader:
            batch += self.process_edge_row(row)
            count += 1
            if count == self.edge_batch_size:
                count = 0
                print(batch)
                batch = 'g'
        if batch != 'g':        
            print(batch)

    def process_property(self,row,key):
        kt = key.split(':')
        if len(kt) > 1:
            if kt[1].upper() in self.INTEGERS:
                value = int(row[key])
            elif kt[1].upper() in self.FLOATS:
                value = float(row[key])
            elif kt[1].upper() == 'DATE':
                value = f'datetime(\'{row[key]}\')'
            else:
                value = f'\'{row[key]}\''
        else:
            value = f'\'{row[key]}\''
        return f'.property(\'{kt[0]}\',{value})'  

    def process_edge_row(self,r):
        properties = ''
        for k in r:
            if r[k] == '':
                pass
            elif k == '~id':
                eid = r['~id']
            elif k == '~label':    
               elabel = r['~label']
            elif k == '~from':    
               efrom = r['~from']
            elif k == '~to':    
               eto = r['~to']
            else:
                properties += self.process_property(r,k)
       
        edge = f'.addE(\'{elabel}\').property(id,\'{eid}\')' 
        edge += f'.from(\'{efrom}\').to(\'{eto}\')' 
        edge += properties        
        return edge


    def process_vertex_row(self,r):
        properties = ''
        for k in r:
            if r[k] == '':
                pass
            elif k == '~id':
                vid = r['~id']
            elif k == '~label':    
               vlabel = r['~label']
            else:
                properties += self.process_property(r,k)
       
        vertex = f'.addV(\'{vlabel}\').property(id,\'{vid}\')' + properties        
        return vertex
        
    def process_csv_file(self,fname):
        with open(fname, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            
            if '~from' in reader.fieldnames:
                self.process_edges(reader)
            else:
                self.process_vertices(reader)
            csvfile.close()

if __name__ == '__main__':
    ncsv = NeptuneCSVReader(vbatch=10,ebatch=10)
    if len(sys.argv) > 1:
        ncsv.process_csv_file(sys.argv[1])
    else:
        print(f"\ncsv-gremlin: version {ncsv.VERSION}, {ncsv.VERSION_DATE}")
        print("\nUsage: csv-gremlin <name_of_csv_file>")
