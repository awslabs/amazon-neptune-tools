#!/usr/local/bin/python3
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
@deffield    lastUpdated:  2022-06-30

Overview
--------
This file contains the definition for a NeptuneCSVReader class. Its purpose is to
provide a tool able to read CSV files that use the Amazon Neptune formatting rules
and generate Gremlin steps from that data.  Those Gremlin steps can then be used to
load the data into any TinkerPop compliant graph that allows for user defined Vertex
and Edge IDs. 

The tool can detect and handle both the vertex and edge CSV file formats. It
recognizes the Neptune type specifiers, such as 'age:Int' and defaults to String if
none is provided in a column header.  It also handles sparse rows ',,,' etc.

The tool also allows you to specify the batch size for vertices and edges. The
default is set to 10 for each currently. Batching allows multiple vertices or edges,
along with their properties, to be added in a single Gremlin query. Many other
options are provided. The tool also tries to detect and report any errors in the CSV
files it processes.

Gremlin steps that represent the data in the CSV are written to 'stdout'. Errors and
statistics are written to 'stderr' so that Gremlin output can be redirected to a file
without also capturing other information.

None of the special column headers are allowed to be omitted except for ~label in the
case of vertices.  In that case a default label "vertex" will be used. The special
headers are ~id, ~label, ~from and ~to.

For files containing vertex data, the same ID may appear on multiple rows allowing
properties containing sets of values to be built up on multiple rows if desired.

Current Limitations
-------------------
Currently the tool does not support the cardinality column header such as
'age:Int(single)'. However, lists of values declared using the '[]' column
header modifier are supported.     
'''

import csv
import sys
import argparse
import datetime
import dateutil.parser as dparser

class NeptuneCSVReader:
    VERSION = 0.19
    VERSION_DATE = '2022-06-30'
    INTEGERS = ('BYTE','SHORT','INT','LONG')
    FLOATS = ('FLOAT','DOUBLE')
    BOOLS = ('BOOL','BOOLEAN')
    VERTEX = 1
    EDGE = 2

    def __init__(self, vbatch=1, ebatch=1, java_dates=False, max_rows=sys.maxsize,
                 assume_utc=False, stop_on_error=True, silent_mode=False,
                 escape_dollar=False, show_summary=True, double_suffix=False,
                 skip_spaces=False):
        
        self.vertex_batch_size = vbatch
        self.edge_batch_size = ebatch
        self.use_java_date = java_dates
        self.row_limit = max_rows
        self.assume_utc = assume_utc
        self.stop_on_error = stop_on_error
        self.silent_mode = silent_mode
        self.escape_dollar = escape_dollar
        self.double_suffix = double_suffix
        self.show_summary = show_summary
        self.mode = self.VERTEX
        self.current_row = 1
        self.id_store = {}
        self.id_count = 0
        self.duplicate_id_count = 0
        self.errors = 0
        self.vertex_count = 0
        self.edge_count = 0
        self.property_count = 0
        self.verbose_summary = False
        self.skip_spaces = skip_spaces

    def get_batch_sizes(self):
        return {'vbatch': self.vertex_batch_size,
                'ebatch': self.edge_batch_size}
        
    def set_batch_sizes(self, vbatch=1, ebatch=1):
        self.vertex_batch_size = vbatch
        self.edge_batch_size = ebatch

    def set_java_dates(self,f:bool):
        self.use_java_date = f
    
    def get_java_dates(self):
        return self.use_java_date

    def set_max_rows(self,r):
        self.row_limit = r

    def get_max_rows(self):
        return self.row_limit

    def set_assume_utc(self,utc:bool):
        self.assume_utc = utc

    def get_assume_utc(self):
        return self.assume_utc

    def set_stop_on_error(self,stop:bool):
        self.stop_on_error = stop

    def get_stop_on_error(self):
        return self.stop_on_error

    def set_silent_mode(self,silent:bool):
        self.silent_mode = silent

    def get_silent_mode(self):
        return self.silent_mode

    def set_escape_dollar(self,dollar:bool):
        self.escape_dollar = dollar

    def get_escape_dollar(self):
        return self.escape_dollar

    def set_show_summary(self, summary:bool):
        self.show_summary = summary

    def get_show_summary(self):
        return self.show_summary

    def set_double_suffix(self,suffix:bool):
        self.double_suffix = suffix

    def get_double_suffix(self):
        return self.double_suffix

    def set_skip_spaces(self,skip:bool):
        self.skip_spaces = skip

    def get_skip_spaces(self):
        return self.skip_spaces

    def escape(self,string):
        escaped = string.replace('"','\\"')
        return escaped

    def print_normal(self,msg):
        if not self.silent_mode:
            print(msg)

    def print_error(self,msg):
        txt = f'Row {self.current_row}: {msg}'
        print(txt, file=sys.stderr)
        self.errors += 1
        if self.stop_on_error:
            sys.exit(1)
    
    # The Amazon Neptune CSV format documentation states that any value other than
    # 'true' (in any combination of upper and lowercase letters) in a column tagged 
    # as containing a Boolean value should be treated as 'false'
    def process_boolean(self,val):
        if val.upper() == 'TRUE':
            result = 'true'
        else:
            result = 'false'
        return result

    # If use_java_date is not set, the date string from the CSV file is wrapped
    # as-is inside a datetime(). If use_java_date is set, the ISO date string
    # is converted into a datetime and the delta from 1970 is calculated using
    # epoch time. As Python cannot subtract a TZ aware date and a naiive date,
    # if no TZ offset is present in the CSV date, it is treated as being in
    # local TZ when this code is run. However, if assume_utc is set, the date
    # will be treated as UTC instead of local time. If date_string is not a valid
    # ISO 8601 date, isoparse will throw an exception. That exception needs to be
    # handled by methods that call process_date.

    def process_date(self,date_string):
        """Return an ISO 8601 date appropriately converted and wrapped"""
        if date_string is None:
            return None

        if self.use_java_date:
            epoch = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)
            date =  dparser.isoparse(date_string)
            if date.tzinfo is None:
                if self.assume_utc:
                    date =  date.replace(tzinfo=datetime.timezone.utc)
                else:
                    date =  date.replace(tzinfo=datetime.datetime.now().astimezone().tzinfo)
            delta = int((date - epoch).total_seconds() * 1000)
            val = f'new Date({delta})'
        else:
            val = f'datetime(\'{date_string}\')'
        return val

    # Start processing each row assuming they represent vertices.
    def process_vertices(self,reader):
        count = 0
        rows_processed = 0
        self.mode = self.VERTEX
        batch = "g"
        for row in reader:
            self.current_row += 1
            batch += self.process_vertex_row(row)
            count += 1
            if count == self.vertex_batch_size:
                count = 0
                if batch != 'g':
                    self.print_normal(batch)
                batch = 'g'
            rows_processed += 1
            if rows_processed == self.row_limit:
                break
        if batch != 'g':        
            self.print_normal(batch)

    # Start processing each row assuming they represent edges.
    def process_edges(self,reader):
        count = 0
        rows_processed = 0
        self.mode = self.EDGE
        batch = 'g'
        errors = False

        if not '~label' in reader.fieldnames:
            self.print_error('For edges, the header row must include a ~label column')
            errors = True

        if not '~from' in reader.fieldnames or not '~to' in reader.fieldnames:
            self.print_error('For edges, the header row must include both ~from and ~to columns')
            errors = True

        if errors:
            sys.exit(1)

        for row in reader:
            self.current_row += 1
            batch += self.process_edge_row(row)
            count += 1
            if count == self.edge_batch_size:
                count = 0
                if batch != 'g':
                    self.print_normal(batch)
                batch = 'g'
            rows_processed += 1
            if rows_processed == self.row_limit:
                break
        if batch != 'g':        
            self.print_normal(batch)

    # Process properties taking into account any type information from the CSV
    # header row. The header may optionally define a type and also a Set '[]'
    # cardinality. Set cardinality is only supported by vertices. If no type
    # information is present the values will be treated as strings.  Examples
    # include:
    #  name                  value
    #  name:String           value:Int
    #  names:String[]        values:Int[]
    #
    # This method attempts to catch errors in the CSV file such as invalid dates
    # and numbers, and missing required values for a column. When print_error is
    # called it will check to see if processing is allowed to continue. If it is
    # not it will exit. An exception is made in the case of an edge file that is
    # attempting to specify set cardinality properties. If that is detected, 
    # processing is stopped immediately.
    def process_property(self,row,key):
        cardinality = ''
        result = ''
        if key is None:
            self.print_error('Unexpected additional column(s) with no header.')
            return ''

        kt = key.split(':')
        if len(kt) > 1:
            if kt[1].endswith('[]'):
                if self.mode == self.EDGE:
                    self.print_error('Only vertices can have Set cardinality properties')
                    sys.exit(1)
                else:
                    kt[1] = kt[1][0:-2]
                    cardinality = 'set,'
                    members = row[key].split(';')
            else:
                members = [row[key]]

            try:
                if kt[1].upper() in self.INTEGERS:
                    values = [int(x) for x in members]
                elif kt[1].upper() in self.FLOATS:
                    values = [float(x) for x in members]
                elif kt[1].upper() in self.BOOLS:
                    values = [self.process_boolean(x) for x in members]
                elif kt[1].upper() == 'DATE':
                    values = [self.process_date(x) for x in members]
                else:
                    values = [f'"{self.escape(x)}"' for x in members] 

            except TypeError as te:
                result = ''
                msg = f'For column [{kt[0]}] {str(te)}'
                self.print_error(msg)
            except ValueError as ve:
                result = ''
                msg = f'For column [{kt[0]}] {str(ve)}'
                self.print_error(msg)
            except Exception as ex:
                result = ''
                msg = f'For column [{kt[0]}] {str(ex)}'
                self.print_error(msg)
            else:    
                if None in values:
                    msg = f'For column [{kt[0]}] a value is required'
                    self.print_error(msg)
                    result = ''
                else:
                    for p in values:
                        self.property_count += 1
                        if type(p) is float and self.double_suffix:
                            pv = f'{p}d'
                        else:
                            pv = f'{p}'
                        result += f'.property({cardinality}\"{kt[0]}\",{pv})'
        else:
            if row[key] is None:
                result = ''
                msg = f'For column [{kt[0]}] a value is required.'
                self.print_error(msg)
            else:
                self.property_count += 1
                result = f'.property("{kt[0]}","{self.escape(row[key])}")'
        return result

    # Process a row from a file of edge data. A check is made that a value for each 
    # of the required column headers is provided. The header row itself will have 
    # already been validated before we get here by process_edges.  Any dollar
    # signs ($) are replaced with their escaped version as in Groovy Strings the $ has
    # a special meaning and causes the compiler to attempt interpolation. This becomes
    # an issue once the Gremlin scripts that this tool generates are executed in some cases.
    def process_edge_row(self,r):
        properties = ''
        error = False
        seen = 0
        for k in r:
            if r[k] == '':
                pass
            elif k == '~id':
                eid = r['~id']
                seen |= 0x1
            elif k == '~label':    
               elabel = r['~label']
               seen |= 0x2
            elif k == '~from':    
               efrom = r['~from']
               seen |= 0x4
            elif k == '~to':    
               eto = r['~to']
               seen |= 0x8
            else:
                properties += self.process_property(r,k)
       
        if seen != 0xF:
            self.print_error('For edge data, values must be provided for ~id,~label,~from and ~to')
            edge = ''
        else:
            if eid in self.id_store:
                self.duplicate_id_count += 1
                self.id_store[eid] += 1
                error = True
                edge = ''
                self.print_error(f'Duplicate edge ID {eid}')
            else:
                self.edge_count += 1
                self.id_count += 1
                self.id_store[eid] = 1
            
            if not error:
                edge = f'.addE(\"{elabel}\").property(id,\"{eid}\")' 
                edge += f'.from(V(\"{efrom}\")).to(V(\"{eto}\"))' 
                edge += properties 
                if self.escape_dollar:
                    edge = edge.replace("$","\\$")
        return edge

    # Process one row of a CSV file that has been determined to contain vertex data.
    # A check is made that a value has been seen (provided) for ~id. Any dollar
    # signs ($) are replaced with their escaped version as in Groovy Strings the $ has
    # a special meaning and causes the compiler to attempt interpolation. This becomes
    # an issue once the Gremlin scripts that this tool generates are executed in some cases.
    def process_vertex_row(self,r):
        properties = ''
        seen = 0
        vlabel = 'vertex'
        for k in r:
            if r[k] == '':
                pass
            elif k == '~id':
                vid = r['~id']
                seen = 1
            elif k == '~label':    
               vlabel = r['~label']
            else:
                properties += self.process_property(r,k)
       
        if seen != 1:
            self.print_error('Values must be provided for ~id')
            vertex = ''
        else:
            if vid in self.id_store:
                self.duplicate_id_count += 1
                self.id_store[vid] += 1
                prefix = f'.V("{vid}")'
                properties = properties.replace('property("','property(set,"')
            else:
                self.vertex_count += 1
                self.id_count += 1
                self.id_store[vid] = 1
                prefix = f'.addV(\"{vlabel}\").property(id,\"{vid}\")'        

            vertex = prefix + properties

            if self.escape_dollar:
                vertex = vertex.replace("$","\\$")
        return vertex
        
    # Start processing the file and try to detect if the data describes
    # edges or vertices. If the file contains edges and one of ~from or
    # ~to is missing, that will be detected later.
    def process_csv_file(self,fname):
        """Appropriately process the CSV file as either vertices or edges"""
        self.current_row = 1
        self.id_store = {}
        self.id_count = 0
        self.duplicate_id_count = 0
        self.errors = 0
        self.vertex_count = 0
        self.edge_count = 0
        self.property_count = 0
        try:
            with open(fname, newline='') as csvfile:
                reader = csv.DictReader(csvfile, skipinitialspace=self.skip_spaces, escapechar="\\")

                if not '~id' in reader.fieldnames:
                    self.print_error('The header row must include an ~id column')
                    sys.exit(1)
                
                if '~from' in reader.fieldnames or '~to' in reader.fieldnames:
                    self.process_edges(reader)
                else:
                    self.process_vertices(reader)
                csvfile.close()
        except Exception as ex:
            print(f'Unable to load the CSV file: {str(ex)}')
        
        if self.show_summary:
            if self.verbose_summary:
                print(self.id_store, file=sys.stderr)
                
            print('\nProcessing Summary',file=sys.stderr)
            print('------------------',file=sys.stderr)
            stats =  f'Rows={self.current_row}, IDs={self.id_count}, '
            stats += f'Duplicate IDs={self.duplicate_id_count}, '
            stats += f'Vertices={self.vertex_count}, '
            stats += f'Edges={self.edge_count}, '
            stats += f'Properties={self.property_count}, '
            stats += f'Errors={self.errors}'
            print(stats, file=sys.stderr)

if __name__ == '__main__':
    ncsv = NeptuneCSVReader()
    parser = argparse.ArgumentParser()
    parser.add_argument('csvfile', help='The name of the CSV file to process')
    parser.add_argument('-v','--version', action='version', 
                        help='Display version information', 
                        version=f"\ncsv-gremlin: version {ncsv.VERSION}, {ncsv.VERSION_DATE}")
    parser.add_argument('-vb', type=int, default=10,
                        help='Set the vertex batch size to use (default %(default)s)')
    parser.add_argument('-eb', type=int, default=10,
                        help='Set the edge batch size to use (default %(default)s)')
    parser.add_argument('-java_dates', action='store_true',
                        help='Use Java style "new Date()" instead of "datetime()".\
                              This option can also be used to force date validation.')
    parser.add_argument('-assume_utc', action='store_true',
                        help='If date fields do not contain timezone information, assume they are in UTC.\
                              By default local time is assumed otherwise. This option only applies if\
                              java_dates is also specified.')
    parser.add_argument('-rows', type=int,
                        help='Specify the maximum number of rows to process. By default the whole file is processed')
    parser.add_argument('-all_errors', action='store_true',
                        help='Show all errors. By default processing stops after any error in the CSV is encountered.')
    parser.add_argument('-silent', action='store_true',
                        help='Enable silent mode. Only errors are reported. No Gremlin is generated.')
    parser.add_argument('-no_summary', action='store_true',
                        help='Do not show a summary report after processing.')
    parser.add_argument('-double_suffix', action='store_true',
                        help='Suffix all floats and doubles with a "d" such as 12.34d. This is helpful\
                        when using the Gremlin Console or Groovy scripts as it will prevent\
                        floats and doubles automatically being created as BigDecimal objects.')
    parser.add_argument('-skip_spaces', action='store_true',
                        help='Skip any leading spaces in each column.\
                        By defaut this setting is False and any leading spaces\
                        will be considered part of the column header or data value.\
                        This setting does not apply to values enclosed in quotes\
                        such as "  abcd".',
                        default=False)
    parser.add_argument('-escape_dollar', action='store_true',
                        help='For any dollar signs found convert them to an escaped\
                        form \$. This is needed if you are going to load the\
                        generated Gremlin using a Groovy processor such as used by\
                        the Gremlin Console. In Groovy strings, the $ sign is used\
                        for interpolation')

    args = parser.parse_args()
    ncsv.set_batch_sizes(vbatch=args.vb, ebatch=args.eb)
    ncsv.set_java_dates(args.java_dates)
    if args.rows is not None:
        ncsv.set_max_rows(args.rows)
    ncsv.set_assume_utc(args.assume_utc)
    ncsv.set_stop_on_error(not(args.all_errors))
    ncsv.set_silent_mode(args.silent)
    ncsv.set_escape_dollar(args.escape_dollar)
    ncsv.set_double_suffix(args.double_suffix)
    ncsv.set_show_summary(not args.no_summary)
    ncsv.set_skip_spaces(args.skip_spaces)
    ncsv.process_csv_file(args.csvfile)
