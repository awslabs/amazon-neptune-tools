#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2018 Amazon.com, Inc. or its affiliates. 
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
@author:     beebs-systap
@copyright:  Amazon.com, Inc. or its affiliates 
@license:    Apache2
@contact:    @beebs-systap
@deffield    created:  2018-01-30

Heavily modified from https://github.com/hadim/pygraphml/blob/master/pygraphml/graphml_parser.py
licensed under BSD 3 Part: https://github.com/hadim/pygraphml/blob/master/LICENSE

'''

__all__ = []
__version__ = 0.1
__date__ = '2018-01-31'
__updated__ = '2018-01-31'

import sys
import os

from optparse import OptionParser
import csv

import xml.etree.ElementTree as etree

class GraphML2CSV:

   @staticmethod
   def fixtag(ns, tag):
	return '{' + ns + '}' + tag

   @staticmethod
   def graphml_tag(tag):
   	graphml_ns = 'http://graphml.graphdrawing.org/xmlns'
	return GraphML2CSV.fixtag(graphml_ns, tag)

   def graphml_to_csv(self, fname):

       outfname_prefix = os.path.splitext(fname)[0]
    
       with open( fname, 'r' ) as f:

	   #Initialize headers and dictionary
           vtx_header = []
           vtx_dict = {}
           edge_header = []
           edge_dict = {}
    
           #Add the Neptune CSV Edge Headers
    
           vtx_header.append("~id")
           vtx_header.append("~label")

           #Add the Neptune CSV Edge Headers
    
           edge_header.append("~id")
           edge_header.append("~from")
           edge_header.append("~to")
           edge_header.append("~label")
       

           with open(outfname_prefix+'-nodes.csv', 'w') as node_csvfile, open(outfname_prefix+'-edges.csv', 'w') as edge_csvfile:

	      #Initialize these after we've read the header.
              node_writer = None
              edge_writer = None
 
    	      edge_cnt = 0
              edge_attr_cnt = 0
        

	      node_cnt = 0
              node_attr_cnt = 0

	      for event, elem in etree.iterparse(f, events=('start', 'end')):

                 if event == 'start':

       	            #Extract the node and edge CSV headers
		    #Write the header for the CSV files when we see the graph element
                    if elem.tag == GraphML2CSV.graphml_tag('graph'):
               	       node_writer = csv.DictWriter(node_csvfile, fieldnames=vtx_header, restval='')
                       node_writer.writeheader()
               	       edge_writer = csv.DictWriter(edge_csvfile, fieldnames=edge_header, restval='')
               	       edge_writer.writeheader()
	
                 if event == 'end':

                    if elem.tag == GraphML2CSV.graphml_tag('key'):
	
                       #Assume the labelV is the vertex label, if specified
	               if elem.attrib['for'] == 'node' and elem.attrib['id'] != 'labelV':
	                  vtx_dict[elem.attrib['id']] = elem.attrib['id']+":"+elem.attrib['attr.type']
	                  vtx_header.append(elem.attrib['id']+":"+elem.attrib['attr.type'])
	    
                          #Assume the labelE is the edge label, if specified
	               elif elem.attrib['for'] == 'edge' and elem.attrib['id'] != 'labelE':
	               	  edge_dict[elem.attrib['id']] = elem.attrib['id']+":"+elem.attrib['attr.type']
	                  edge_header.append(elem.attrib['id']+":"+elem.attrib['attr.type'])

                       elem.clear()

                    if elem.tag == GraphML2CSV.graphml_tag('node'):
		       node_cnt+=1
		       node_d = {}
                       node_d ["~id"] = elem.attrib['id']
	
                       for data in elem:
                          att_val = data.attrib.get('key').encode('utf-8').strip()

                          if att_val == "labelV":
                             node_d["~label"] = data.text.encode('utf-8').strip()
			  else:
                             node_d[vtx_dict[att_val]] = data.text.encode('utf-8').strip()
                          node_attr_cnt+=1

                       node_writer.writerow(node_d)
                       elem.clear()
	
                    if elem.tag == GraphML2CSV.graphml_tag('edge'):
                        edge_cnt+=1
                        edge_d = {}
    
                        id=elem.attrib['id']
                        source = elem.attrib['source']
                        dest = elem.attrib['target']
                        # Neptune CSV header values        
                        # source/target attributes refer to IDs: http://graphml.graphdrawing.org/xmlns/1.1/graphml-structure.xsd
        
                        edge_d["~id"] = id
                        edge_d["~from"] = source
                        edge_d["~to"] = dest


                        for data in elem:
                           att_val = data.attrib.get('key').encode('utf-8').strip()

                           if att_val == "labelE":
                              edge_d["~label"] = data.text.encode('utf-8').strip()
                           else:
                              edge_d[edge_dict[att_val]] = data.text.encode('utf-8').strip()
                           edge_attr_cnt+=1

                        edge_writer.writerow(edge_d)
                        elem.clear()
    
       sys.stderr.write("Wrote %d nodes and %d attributes to %s.\n" % ( node_cnt , node_attr_cnt , outfname_prefix+'-nodes.csv'))
       sys.stderr.write("Wrote %d edges and %d attributes to %s.\n" % ( edge_cnt , edge_attr_cnt , outfname_prefix+'-edges.csv') )
    
       return

def main(argv=None):
    '''Command line options.'''

    program_name = os.path.basename(sys.argv[0])
    program_version = "v0.1"
    program_build_date = "%s" % __updated__

    program_version_string = '%%prog %s (%s)' % (program_version, program_build_date)
    program_longdesc = ( "A utility python script to convert GraphML files into the Amazon Neptune CSV format " 
         "for bulk ingestion. See "
	 "https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html."
	 )
    program_license = "Copyright 2018 Amazon.com, Inc. or its affiliates.	\
                Licensed under the Apache License 2.0\nhttp://aws.amazon.com/apache2.0/"


    if argv is None:
        argv = sys.argv[1:]
    try:
        # setup option parser
        parser = OptionParser(version=program_version_string, epilog=program_longdesc, description=program_license)
        parser.add_option("-i", "--in", dest="infile", help="set input path [default: %default]", metavar="FILE")
        parser.add_option("-v", "--verbose", dest="verbose", action="count", help="set verbosity level [default: %default]")

        # process options
        (opts, args) = parser.parse_args(argv)
    
        if opts.verbose > 0:
            sys.stderr.write("verbosity level = %d" % opts.verbose)

        if opts.infile:
            sys.stderr.write("infile = %s\n" % opts.infile)
            infile = opts.infile
        else:
            sys.stderr.write("graphml input file is required.\n")
            return 2

        # MAIN BODY #

        sys.stderr.write('Processing %s\n' % opts.infile)
	xformer = GraphML2CSV()
        xformer.graphml_to_csv(opts.infile)

    except Exception, e:
        sys.stderr.write(repr(e))
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2

if __name__ == '__main__':
    sys.exit(main())
cls
