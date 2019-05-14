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
        if tag.startswith('{'+graphml_ns+'}'):
            return tag
        else:
            return GraphML2CSV.fixtag(graphml_ns, tag)

    @staticmethod
    def py_compat_str(encoding, data):
        if sys.hexversion >= 0x3000000:
            return data.encode(encoding).decode('utf-8')
        else:
            return data.encode(encoding)

    def graphml_to_csv(self, fname, delimiter, encoding):

        outfname_prefix = os.path.splitext(fname)[0]

        with open(fname, 'r') as f:

            # Initialize headers and dictionary
            vtx_header = []
            vtx_dict = {}
            edge_header = []
            edge_dict = {}

            # Add the Neptune CSV Edge Headers

            vtx_header.append("~id")
            vtx_header.append("~label")

            # Add the Neptune CSV Edge Headers

            edge_header.append("~id")
            edge_header.append("~from")
            edge_header.append("~to")
            edge_header.append("~label")

            with open(outfname_prefix+'-nodes.csv', 'w') as node_csvfile, open(outfname_prefix+'-edges.csv', 'w') as edge_csvfile:

                # Initialize these after we've read the header.
                node_writer = None
                edge_writer = None

                edge_cnt = 0
                edge_attr_cnt = 0

                node_cnt = 0
                node_attr_cnt = 0

                for event, elem in etree.iterparse(f, events=('start', 'end')):

                    if event == 'start':

                        # Extract the node and edge CSV headers
                        # Write the header for the CSV files when we see the graph element
                        if GraphML2CSV.graphml_tag(elem.tag) == GraphML2CSV.graphml_tag('graph'):
                            node_writer = csv.DictWriter(
                                node_csvfile, fieldnames=vtx_header, restval='', delimiter=delimiter)
                            node_writer.writeheader()
                            edge_writer = csv.DictWriter(
                                edge_csvfile, fieldnames=edge_header, restval='', delimiter=delimiter)
                            edge_writer.writeheader()

                    if event == 'end':

                        if GraphML2CSV.graphml_tag(elem.tag) == GraphML2CSV.graphml_tag('key'):

                            # Assume the labelV is the vertex label, if specified
                            if elem.attrib['id'] != 'labelV' and elem.attrib['id'] != 'labelE':
                                if not 'for' in elem.attrib or elem.attrib['for'] == 'node':
                                    vtx_dict[elem.attrib['id']] = elem.attrib['id'] + \
                                        ":"+elem.attrib['attr.type']
                                    vtx_header.append(
                                        elem.attrib['id']+":"+elem.attrib['attr.type'])

                                if not 'for' in elem.attrib or elem.attrib['for'] == 'edge':
                                    edge_dict[elem.attrib['id']] = elem.attrib['id'] + \
                                        ":"+elem.attrib['attr.type']
                                    edge_header.append(
                                        elem.attrib['id']+":"+elem.attrib['attr.type'])

                            elem.clear()

                        if GraphML2CSV.graphml_tag(elem.tag) == GraphML2CSV.graphml_tag('node'):

                            node_cnt += 1
                            node_d = {}

                            if 'id' in elem.attrib:
                                node_d["~id"] = elem.attrib['id']
                            else:
                                # If the optional ID is not present, use the node count
                                node_d["~id"] = node_cnt

                            has_label = None

                            for data in elem:
                                att_val = GraphML2CSV.py_compat_str(encoding,
                                                                    data.attrib.get('key'))

                                if att_val == "labelV":
                                    node_d["~label"] = GraphML2CSV.py_compat_str(encoding,
                                                                                 data.text)
                                    has_label = True
                                else:
                                    node_d[vtx_dict[att_val]] = GraphML2CSV.py_compat_str(encoding,
                                                                                          data.text)
                                node_attr_cnt += 1

                            if not has_label:
                                # Use node as the label if it is unspecified
                                node_d["~label"] = "node"

                            node_writer.writerow(node_d)
                            elem.clear()

                        if GraphML2CSV.graphml_tag(elem.tag) == GraphML2CSV.graphml_tag('edge'):
                            edge_cnt += 1
                            edge_d = {}
                            has_label = None

                            source = elem.attrib['source']
                            dest = elem.attrib['target']
                            # Neptune CSV header values
                            # source/target attributes refer to IDs: http://graphml.graphdrawing.org/xmlns/1.1/graphml-structure.xsd

                            id = source + '_' + dest  # If the optional ID is not present, use the source_dest
                            if 'id' in elem.attrib:
                                id = elem.attrib['id']

                            edge_d["~id"] = id
                            edge_d["~from"] = source
                            edge_d["~to"] = dest

                            for data in elem:
                                att_val = GraphML2CSV.py_compat_str(encoding,
                                                                    data.attrib.get('key'))

                                if att_val == "labelE":
                                    edge_d["~label"] = GraphML2CSV.py_compat_str(encoding,
                                                                                 data.text)
                                    has_label = True
                                else:
                                    edge_d[edge_dict[att_val]] = GraphML2CSV.py_compat_str(encoding,
                                                                                           data.text)
                                edge_attr_cnt += 1

                            if not has_label:
                                # Use edge as the label if it is unspecified
                                edge_d["~label"] = "edge"

                            edge_writer.writerow(edge_d)
                            elem.clear()

        sys.stderr.write("Wrote %d nodes and %d attributes to %s.\n" % (
            node_cnt, node_attr_cnt, outfname_prefix+'-nodes.csv'))
        sys.stderr.write("Wrote %d edges and %d attributes to %s.\n" % (
            edge_cnt, edge_attr_cnt, outfname_prefix+'-edges.csv'))

        return


def main(argv=None):
    '''Command line options.'''

    program_name = os.path.basename(sys.argv[0])
    program_version = "v0.1"
    program_build_date = "%s" % __updated__

    program_version_string = '%%prog %s (%s)' % (
        program_version, program_build_date)
    program_longdesc = ("A utility python script to convert GraphML files into the Amazon Neptune CSV format "
                        "for bulk ingestion. See "
                        "https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html."
                        )
    program_license = "Copyright 2018 Amazon.com, Inc. or its affiliates.	\
                Licensed under the Apache License 2.0\nhttp://aws.amazon.com/apache2.0/"

    if argv is None:
        argv = sys.argv[1:]
    try:
        # setup option parser
        parser = OptionParser(version=program_version_string,
                              epilog=program_longdesc, description=program_license)
        parser.add_option("-i", "--in", dest="infile",
                          help="set input path [default: %default]", metavar="FILE")

        parser.add_option("-d", "--delimiter", dest="delimiter", default=",",
                          help="Set the output file delimiter [default: %default]")

        parser.add_option("-e", "--encoding", dest="encoding", default="utf-8",
                          help="Set the input file encoding [default: %default]")

        # process options
        (opts, args) = parser.parse_args(argv)

        if opts.infile:
            sys.stderr.write("infile = %s\n" % opts.infile)
            infile = opts.infile
        else:
            sys.stderr.write("graphml input file is required.\n")
            parser.print_help()
            return int(2)

        # MAIN BODY #

        sys.stderr.write('Processing %s\n' % opts.infile)
        xformer = GraphML2CSV()
        xformer.graphml_to_csv(opts.infile, opts.delimiter, opts.encoding)
        return 0

    except Exception as e:
        sys.stderr.write(repr(e))
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2


if __name__ == '__main__':
    sys.exit(main())
cls
