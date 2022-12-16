# Copyright Amazon.com, Inc. or its affiliates.
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
import re
import datetime

class DateTimeFormatter:
    
    def format(self, v):
        if type(v) is datetime.datetime:
            return v
        elif isinstance(v, str):
            return datetime.datetime.fromisoformat(v.replace('Z', '+00:00'))
        elif isinstance(v, int) or isinstance(v, float):
            return datetime.datetime.utcfromtimestamp(v).replace(tzinfo=datetime.timezone.utc)
        else:
            raise Exception('Unable to parse value into datetime: {}'.format(v))

class Separator:

    MULTI_VALUED_FIELD_DELIMITER = ';'
    MULTI_VALUED_FIELD_DELIMITER_REGEX = '(?<!\\\\)' + MULTI_VALUED_FIELD_DELIMITER
    MULTI_VALUED_FIELD_ESCAPED_DELIMITER = '\\' + MULTI_VALUED_FIELD_DELIMITER
    
    def split(self, s):
        values = re.split(Separator.MULTI_VALUED_FIELD_DELIMITER_REGEX, s)
        return [v.replace(Separator.MULTI_VALUED_FIELD_ESCAPED_DELIMITER, Separator.MULTI_VALUED_FIELD_DELIMITER) for v in values]
        
class TokenMappings:
    
    def __init__(self, id_token='~id', label_token='~label', from_token='~from', to_token='~to'):
        self.id_token = id_token
        self.label_token = label_token
        self.from_token = from_token
        self.to_token = to_token
        
    def is_id_token(self, s):
        return s == self.id_token
        
    def is_label_token(self, s):
        return s == self.label_token
        
    def is_from_token(self, s):
        return s == self.from_token
        
    def is_to_token(self, s):
        return s == self.to_token
        
    def is_token(self, s):
        return self.is_id_token(s) or self.is_label_token(s) or self.is_from_token(s) or self.is_to_token(s)
        
        

class Mapping:
    
    def __init__(self, key, name, datatype='string', cardinality='set', is_multi_valued=False, converter=lambda x:x, separator=None, token_mappings=None):
        self.key = key
        self.name = name
        self.datatype = datatype
        self.cardinality = cardinality
        self.is_multi_valued = is_multi_valued
        self.converter = converter
        self.separator = separator
        self.token_mappings = token_mappings
        
        if self.is_multi_valued and not self.separator:
            raise Exception('No separator specified for multi-valued property: {}'.format(self.key))
            
    def is_id_token(self):
        return self.token_mappings.is_id_token(self.key) if self.token_mappings else False
        
    def is_label_token(self):
        return self.token_mappings.is_label_token(self.key) if self.token_mappings else False
        
    def is_from_token(self):
        return self.token_mappings.is_from_token(self.key) if self.token_mappings else False
        
    def is_to_token(self):
        return self.token_mappings.is_to_token(self.key) if self.token_mappings else False
        
    def is_token(self):
        return True if self.token_mappings else False
        
    def convert(self, v):
        if self.token_mappings:
            raise Exception('Invalid operation for token mapping')
        return self.converter(v)
        
class Mappings:
    
    def __init__(self, mappings={}, token_mappings=TokenMappings(), separator=Separator(), datetime_formatter=DateTimeFormatter()):
        self.mappings = mappings
        self.token_mappings = token_mappings
        self.separator = separator
        self.datetime_formatter = datetime_formatter
        
    def add(self, mapping):
        if mapping.key in self.mappings:
            raise Exception('Mapping for {} already exists'.format(mapping.key))
        self.mappings[mapping.key] = mapping
        
    def mapping_for(self, key):
        if key in self.mappings:
            return self.mappings[key]
            
        kwargs = {}
        kwargs['key'] = key
        
        if self.token_mappings.is_token(key):
            kwargs['name'] = key
            kwargs['datatype'] = None
            kwargs['cardinality'] = None
            kwargs['token_mappings'] = self.token_mappings
            mapping = Mapping(**kwargs)
            self.add(mapping)
            return mapping

        parts = key.rsplit(':', 1)
        
        name = parts[0]
        metadata = parts[1] if len(parts) > 1 else None
        
        if metadata:
            metadata_match = re.search('([^\\[\\]\\(\\)]+)(\\((single|set)\\))?(\\[\\])?', metadata)
            
            datatype = metadata_match.group(1).lower() if metadata_match.group(1) else None
            
            if datatype not in ['string', 'bool', 'boolean', 'byte', 'short', 'int', 'long', 'float', 'double', 'date']:
                
                name = key
            
            else:
                
                kwargs['datatype'] = datatype
                
                if datatype == 'byte':
                    kwargs['converter'] = lambda x: int(x)
                elif datatype == 'short':
                    kwargs['converter'] = lambda x: int(x)
                elif datatype == 'int':
                    kwargs['converter'] = lambda x: int(x)
                elif datatype == 'long':
                    kwargs['converter'] = lambda x: int(x)
                elif datatype == 'float':
                    kwargs['converter'] = lambda x: float(x)
                elif datatype == 'double':
                    kwargs['converter'] = lambda x: float(x)
                elif datatype == 'date':
                    kwargs['converter'] = lambda x: self.datetime_formatter.format(x)
                elif datatype.startswith('bool'):
                    kwargs['converter'] = lambda x: x.lower() == 'true'
                    
                cardinality = 'set'
                    
                if metadata_match.group(3) and metadata_match.group(3).lower() == 'single':
                    cardinality = 'single'
                    kwargs['cardinality'] = cardinality
                    
                if metadata_match.group(3) and metadata_match.group(3).lower() == 'set':
                    kwargs['is_multi_valued'] = True
                    kwargs['separator'] = self.separator
                    
                if metadata_match.group(4) and metadata_match.group(4) == '[]':
                    kwargs['is_multi_valued'] = True
                    kwargs['separator'] = self.separator
                    if cardinality == 'single':
                        raise Exception('Invalid mapping: single cardinality multi-valued property')
                    
        if name.endswith('[]'):
            name = name[:-2]
            kwargs['is_multi_valued'] = True
            kwargs['separator'] = self.separator
        
        kwargs['name'] = name        
        
        mapping = Mapping(**kwargs)
      
        self.add(mapping)
        
        return mapping
        
    def get_id(self, row):
        return row[self.token_mappings.id_token] if self.token_mappings.id_token in row else None
        
    def get_label(self, row):
        return row[self.token_mappings.label_token] if self.token_mappings.label_token in row else None
        
    def get_from(self, row):
        return row[self.token_mappings.from_token] if self.token_mappings.from_token in row else None
        
    def get_to(self, row):
        return row[self.token_mappings.to_token] if self.token_mappings.to_token in row else None
        
import unittest

class TestMappings(unittest.TestCase):

    def test_simple_header(self):
        mappings = Mappings()
        mapping = mappings.mapping_for('email')
        self.assertEqual(mapping.key, 'email')
        self.assertEqual(mapping.name, 'email')
        self.assertEqual(mapping.datatype, 'string')
        self.assertEqual(mapping.cardinality, 'set')
        self.assertEqual(mapping.is_multi_valued, False)
        self.assertEqual(mapping.convert('x@y'), 'x@y')
        
    def test_header_with_type(self):
        mappings = Mappings()
        mapping = mappings.mapping_for('age:int')
        self.assertEqual(mapping.key, 'age:int')
        self.assertEqual(mapping.name, 'age')
        self.assertEqual(mapping.datatype, 'int')
        self.assertEqual(mapping.cardinality, 'set')
        self.assertEqual(mapping.is_multi_valued, False)
        self.assertEqual(mapping.convert('10'), 10)
        
    def test_header_with_type_and_cardinality(self):
        mappings = Mappings()
        mapping = mappings.mapping_for('age:int(single)')
        self.assertEqual(mapping.key, 'age:int(single)')
        self.assertEqual(mapping.name, 'age')
        self.assertEqual(mapping.datatype, 'int')
        self.assertEqual(mapping.cardinality, 'single')
        self.assertEqual(mapping.is_multi_valued, False)
        self.assertEqual(mapping.convert('12'), 12)
        
    def test_header_with_type_and_multi_value(self):
        mappings = Mappings()
        mapping = mappings.mapping_for('email:string[]')
        self.assertEqual(mapping.key, 'email:string[]')
        self.assertEqual(mapping.name, 'email')
        self.assertEqual(mapping.datatype, 'string')
        self.assertEqual(mapping.cardinality, 'set')
        self.assertEqual(mapping.is_multi_valued, True)
        self.assertEqual(mapping.convert('x@y;a@b'), ['x@y', 'a@b'])
        
    def test_header_with_type_and_cardinality_set(self):
        mappings = Mappings()
        mapping = mappings.mapping_for('email:string(set)')
        self.assertEqual(mapping.key, 'email:string(set)')
        self.assertEqual(mapping.name, 'email')
        self.assertEqual(mapping.datatype, 'string')
        self.assertEqual(mapping.cardinality, 'set')
        self.assertEqual(mapping.is_multi_valued, True)
        self.assertEqual(mapping.convert('x@y;a@b'), ['x@y', 'a@b'])
        
    def test_header_without_type_but_with_multi_value(self):
        mappings = Mappings()
        mapping = mappings.mapping_for('email[]')
        self.assertEqual(mapping.key, 'email[]')
        self.assertEqual(mapping.name, 'email')
        self.assertEqual(mapping.datatype, 'string')
        self.assertEqual(mapping.cardinality, 'set')
        self.assertEqual(mapping.is_multi_valued, True)
        self.assertEqual(mapping.convert('x@y;a@b'), ['x@y', 'a@b'])
        
    def test_header_with_type_and_cardinality_and_multi_value(self):
        mappings = Mappings()
        mapping = mappings.mapping_for('reading:float(set)[]')
        self.assertEqual(mapping.key, 'reading:float(set)[]')
        self.assertEqual(mapping.name, 'reading')
        self.assertEqual(mapping.datatype, 'float')
        self.assertEqual(mapping.cardinality, 'set')
        self.assertEqual(mapping.is_multi_valued, True)
        self.assertEqual(mapping.convert('12.0;10.1;5.34'), [12.0, 10.1, 5.34])
        
    def test_throws_exception_if_multi_valued_single(self):
        mappings = Mappings()
        try:
            mapping = mappings.mapping_for('reading:float(single)[]')
        except Exception as e:
            self.assertEqual(str(e), 'Invalid mapping: single cardinality multi-valued property')
            
    def test_allows_colons_in_name(self):
        mappings = Mappings()
        mapping = mappings.mapping_for('ns:age:int')
        self.assertEqual(mapping.key, 'ns:age:int')
        self.assertEqual(mapping.name, 'ns:age')
        self.assertEqual(mapping.datatype, 'int')
        self.assertEqual(mapping.cardinality, 'set')
        self.assertEqual(mapping.is_multi_valued, False)
        self.assertEqual(mapping.convert('10'), 10)
        
    def test_use_full_key_with_colons_as_name_if_unrecognized_datatype(self):
        mappings = Mappings()
        mapping = mappings.mapping_for('ns:age:unrecognized')
        self.assertEqual(mapping.key, 'ns:age:unrecognized')
        self.assertEqual(mapping.name, 'ns:age:unrecognized')
        self.assertEqual(mapping.datatype, 'string')
        self.assertEqual(mapping.cardinality, 'set')
        self.assertEqual(mapping.is_multi_valued, False)
        self.assertEqual(mapping.convert('10'), '10')
        
    def test_date_property(self):
        mappings = Mappings()
        mapping = mappings.mapping_for('created:date')
        self.assertEqual(mapping.key, 'created:date')
        self.assertEqual(mapping.name, 'created')
        self.assertEqual(mapping.datatype, 'date')
        self.assertEqual(mapping.cardinality, 'set')
        self.assertEqual(mapping.is_multi_valued, False)
        expected = datetime.datetime(2021, 6, 22, 12, 3, 52, tzinfo=datetime.timezone.utc)
        self.assertEqual(mapping.convert('2021-06-22T12:03:52Z'), expected)
        self.assertEqual(expected, expected)
        self.assertEqual(mapping.convert(expected.timestamp()), expected)
        
          
    def test_separator_replaces_escaped_chars(self):
        self.assertEqual(Separator().split('en\\;;fr'), ['en;', 'fr'])

if __name__ == '__main__':
    unittest.main()
