import os
import logging
import json
import uuid
import csv
import re
logger = logging.getLogger(__name__)

class BaseDef:
    nodes = {}
    edges = {}
    node_label_count = {}
    edge_label_count = {}
    gen_dup_file = False
    dup_nodes = []
    dup_edges = []

    def __init__(self):
        logger = logging.getLogger(__name__)
        self.data = {}

    def __str__(self):
        return json.dumps(self.data)
    
    def evaluate(self, row, expr):
        return eval(expr, {"row": row, "re": re, "uuid": uuid.uuid4})
    
    def get_indexed_value(self, p_list_value, index):
        result = p_list_value
        if type(p_list_value) == list :
            result = p_list_value[index] if index < len(p_list_value) else p_list_value[0]
        return result

    @classmethod
    def clean_stats(cls):
        cls.nodes = {}
        cls.edges = {}
        cls.node_label_count = {}
        cls.edge_label_count = {}
        cls.dup_nodes = []
        cls.dup_edges = []

    @classmethod
    def log_stats(cls):
        logger.info(f'Nodes: {cls.node_label_count}')
        logger.info(f'Edges: {cls.edge_label_count}')

    @classmethod
    def get_node_id(cls, label, uk):
        result = ""
        key = label + "-" + str(uk)
        if key not in cls.nodes:
            result = uk
        else:
            result = cls.nodes[key]
        return str(result)

    @classmethod
    def validate_nodes(cls, nodes):
        result = []
        for node in nodes:
            key = node.pop('~key')
            if key not in cls.nodes:
                cls.nodes[key] = node['~id']
                result.append(node)
                label = node['~label']
                cls.node_label_count[label] = cls.node_label_count[label] + 1 if label in cls.node_label_count else 1
            elif cls.gen_dup_file:
                # override possible new uuid with found uuid
                node['~id'] = cls.nodes[key]
                cls.dup_nodes.append(node)
            else:
                pass
        return result

    @classmethod
    def validate_edges(cls, edges):
        result = []
        for edge in edges:
            key = edge.pop('~key')
            if key not in cls.edges:
                cls.edges[key] = edge['~id']
                result.append(edge)
                label = edge['~label']
                cls.edge_label_count[label] = cls.edge_label_count[label] + 1 if label in cls.edge_label_count else 1
            elif cls.gen_dup_file:
                # override possible new uuid with found uuid
                edge['~id'] = cls.edges[key]
                cls.dup_edges.append(edge)
            else:
                pass
        return result

    @classmethod    
    def write_dup_files(cls):
        if cls.gen_dup_file:
            file = open('dup_nodes.csv', 'w', newline='', encoding='utf-8') 
            writer = csv.writer(file)
            for node in cls.dup_nodes:
                writer.writerow(node.values())
            logger.debug(f'Written file: {file.name}')
            file.close()
            file = open('dup_edges.csv', 'w', newline='', encoding='utf-8') 
            writer = csv.writer(file)
            for edge in cls.dup_edges:
                writer.writerow(edge.values())
            logger.debug(f'Written file: {file.name}')
            file.close()

class ConfigDef(BaseDef):
    def __init__(self, fileName, gen_dup_file, local_enc='utf-8'):
        super().__init__()
        self.conf_file_name = fileName
        BaseDef.gen_dup_file = gen_dup_file
        self.local_enc = local_enc
        self.output_files = []
        self.output_file_names = []
        self.node_writers = {}
        self.edge_writers = {}
        try:
            with open(self.conf_file_name, "r", encoding=self.local_enc) as json_file:
                self.data = json.load(json_file)
                json_file.close()
        except Exception as ex:
            raise Exception(f'Unable to load the JSON file: {self.conf_file_name} \nexception: {str(ex)}')

        self.source_folder = self.data['source_folder'] if 'source_folder' in self.data else '.source'
        self.data_folder = self.data['data_folder'] if 'data_folder' in self.data else '.data'
        self.file_names = self.data['fileNames']
        self.s3_bucket = self.data['s3_bucket'] if 's3_bucket' in self.data else ''
        self.s3_conf_folder = self.data['s3_conf_folder'] if 's3_conf_folder' in self.data else ''
        self.s3_source_folder = self.data['s3_source_folder'] if 's3_source_folder' in self.data else ''
        self.s3_data_folder = self.data['s3_data_folder'] if 's3_data_folder' in self.data else ''
        self.nodeDefs = []
        for dict_node in self.data['nodes']:
            self.nodeDefs.append(NodeDef(dict_node))
        self.edgeDefs = []
        for dict_edge in self.data['edges']:
            self.edgeDefs.append(EdgeDef(dict_edge))
        logger.debug(f'Loaded Configration from: {self.conf_file_name}')
        #make source and data folders
        try: 
            os.makedirs('./' + self.source_folder, exist_ok=True)
            os.makedirs('./' + self.data_folder, exist_ok=True)
            logger.debug(f'Making sure folders exist: {self.source_folder} {self.data_folder}')
        except Exception as ex:
            raise Exception(f'Unable to make folders: {self.source_folder} {self.data_folder} \nexception: {str(ex)}')
    
    def download_source_file(self, s3, file_name): 
        object_name = self.s3_source_folder + '/' + file_name
        download_file_name = self.source_folder + '/s3_' + file_name
        try:
            logger.debug(f'Downloading Source File: s3://{self.s3_bucket}/{object_name} to {download_file_name}')
            #s3.download_file(self.s3_bucket, object_name, download_file_name)
            s3.Bucket(self.s3_bucket).download_file(object_name, download_file_name)
            logger.debug(f'Downloaded Source File: s3://{self.s3_bucket}/{object_name} to {download_file_name}')
        except Exception as ex:
            raise Exception(f'Unable to download source file: {object_name} \nexception: {str(ex)}')
        return download_file_name

    def upload_data_files(self, s3): 
        try:
            for file_name in self.output_file_names:
                object_name = self.s3_data_folder + '/' + os.path.basename(file_name)
                #response = s3.upload_file(file_name, self.s3_bucket, object_name)
                s3.Bucket(self.s3_bucket).upload_file(file_name, object_name)
                logger.debug(f'Uploaded file: {file_name} to s3://{self.s3_bucket}/{object_name}')
        except Exception as ex:
            raise Exception(f'Unable to upload file: {object_name} to s3://{self.s3_bucket}\nexception: {str(ex)}')
            return False
        self.output_file_names = []
        return True  
 
    def delete_data_files(self, s3): 
        try:
            s3.Bucket(self.s3_bucket).objects.filter(Prefix=self.s3_data_folder).delete()
            logger.debug(f'Emptied the folder: s3://{self.s3_bucket}/{self.s3_data_folder}')
        except Exception as ex:
            raise Exception(f'Unable to empty the folder: s3://{self.s3_bucket}/{self.s3_data_folder}\nexception: {str(ex)}')
            return False
        return True

    def init_writers(self):
        self.output_files = []
        self.node_writers = {}
        self.edge_writers = {}
        #open files for node_writers and write_header
        for node_def in self.nodeDefs:
            node_file = open(self.data_folder + '/' + node_def.csv_file_name, 'w', newline='', encoding=self.local_enc) 
            self.output_files.append(node_file)
            node_writer = csv.DictWriter(node_file, fieldnames=node_def.header, restval='')
            node_writer.writeheader()
            self.node_writers[node_def.csv_file_name] = node_writer
        logger.debug(f'Initialized Node Files: {len(self.node_writers)}')
        
        #open files for edge_writers and write_header
        for edge_def in self.edgeDefs:
            edge_file = open(self.data_folder + '/' + edge_def.csv_file_name, 'w', newline='', encoding=self.local_enc) 
            self.output_files.append(edge_file)
            edge_writer = csv.DictWriter(edge_file, fieldnames=edge_def.header, restval='')
            edge_writer.writeheader()
            self.edge_writers[edge_def.csv_file_name] = edge_writer
        logger.debug(f'Initialized Edge Files: {len(self.edge_writers)}')

    def close_writers(self):
        self.output_file_names = []
        for file in self.output_files:
            logger.debug(f'Written file: {file.name}')
            file.close()
            self.output_file_names.append(file.name)
        self.output_files = []
        self.node_writers = {}
        self.edge_writers = {}

    def process_csv_to_csv(self, reader):
        index = 0
        for row in reader:
            for node_def in self.nodeDefs:
                nodes = self.validate_nodes(node_def.process_to_dict(row))
                for node in nodes: self.node_writers[node_def.csv_file_name].writerow(node)
            for edge_def in self.edgeDefs:
                edges = self.validate_edges(edge_def.process_to_dict(row, self.nodes))
                for edge in edges : self.edge_writers[edge_def.csv_file_name].writerow(edge)
            index += 1
            if index % 10000 == 0:
                logger.debug(f'Processed rows: {index}')
        logger.debug(f'Processed rows: {index}')
  
class NodeDef(BaseDef):
    def __init__(self, dict_node):
        self.data = dict_node
        self.csv_file_name = dict_node['csvFileName']
        #self.gremlin_file_name = dict_node['gremlinFileName']
        self.select = dict_node['select']
        self.id = dict_node['id']
        self.label = dict_node['label']
        self.unique_key = dict_node['uniqueKey'] if 'uniqueKey' in dict_node else dict_node['id']
        self.header = ['~id', '~label']
        self.properties = []
        for dict_prop in self.data['properties']:
            prop_def = PropertyDef(dict_prop)
            self.properties.append(prop_def)
            self.header.append(prop_def.header)
    
    def process_to_dict(self,row):
        nodes = []
        try:
            # process row select is true
            if self.evaluate(row, self.select):
                uniqueKey = self.evaluate(row, self.unique_key)
                if type(uniqueKey) == list :
                    index = 0
                    for key in uniqueKey:
                        node = {}
                        node['~id'] = self.get_indexed_value(self.evaluate(row, self.id), index)
                        node['~label'] = self.get_indexed_value(self.evaluate(row, self.label), index)
                        # ~key needs to be popped by the caller
                        node['~key'] = node['~label'] + '-' + key
                        for prop_def in self.properties:
                            node[prop_def.header] = self.get_indexed_value(prop_def.get_value(row), index)
                        nodes.append(node)
                        index += 1
                else :
                    node = {}
                    node['~id'] = self.evaluate(row, self.id)
                    node['~label'] = self.evaluate(row, self.label)
                    # ~key needs to be popped by the caller
                    node['~key'] = node['~label'] + '-' + str(uniqueKey)
                    for prop_def in self.properties:
                        node[prop_def.header] = prop_def.get_value(row)
                    nodes.append(node)
        except Exception as ex:
            raise Exception(f'Unable to process the node: {self.label} \nnodeDef: {self} \n\nrow: {row} \nexception: {str(ex)}')
        return nodes

class EdgeDef(BaseDef):
    def __init__(self, dict_edge):
        self.data = dict_edge
        self.csv_file_name = dict_edge['csvFileName']
        #self.gremlin_file_name = dict_edge['gremlinFileName']
        self.select = dict_edge['select']
        self.id = dict_edge['id']
        self.label = dict_edge['label']
        self.frm = dict_edge['from']
        self.to = dict_edge['to']
        self.frmLabel = dict_edge['fromLabel']
        self.toLabel = dict_edge['toLabel']
        self.header = ['~id', '~label', '~from', '~to']
        self.properties = []
        for dict_prop in self.data['properties']:
            prop_def = PropertyDef(dict_prop)
            self.properties.append(prop_def)
            self.header.append(prop_def.header)

    def process_to_dict(self, row, nodes):
        edges = []
        try: 
            # process row select is true
            if self.evaluate(row, self.select):
                to_node = self.evaluate(row, self.to)
                if type(to_node) == list :
                    label = self.evaluate(row, self.label)
                    from_node = self.evaluate(row, self.frm)
                    from_label = self.evaluate(row, self.frmLabel)
                    to_label = self.evaluate(row, self.toLabel)
                    index = 0
                    for key in to_node:
                        edge = {}
                        # id of each edge has to  be different and most probably a UUID
                        edge['~id'] = self.evaluate(row, self.id)
                        edge['~label'] = self.get_indexed_value(label, index)
                        edge['~from'] = self.get_node_id(self.get_indexed_value(from_label, index), self.get_indexed_value(from_node, index))
                        edge['~to'] = self.get_node_id(self.get_indexed_value(to_label, index), key)
                        edge['~key'] = edge['~label'] + '-' + edge['~from'] + '-' + edge['~to']
                        for prop_def in self.properties:
                            edge[prop_def.header] = self.get_indexed_value(prop_def.get_value(row), index)
                        edges.append(edge)
                else: 
                    edge = {}
                    edge['~id'] = self.evaluate(row, self.id)
                    edge['~label'] = self.evaluate(row, self.label)
                    edge['~from'] = self.get_node_id(self.evaluate(row, self.frmLabel), self.evaluate(row, self.frm))
                    edge['~to'] = self.get_node_id(self.evaluate(row, self.toLabel), to_node)
                    edge['~key'] = edge['~label'] + '-' + edge['~from'] + '-' + edge['~to']
                    for prop_def in self.properties:
                        edge[prop_def.header] = prop_def.get_value(row)
                    edges.append(edge)
        except Exception as ex:
            raise Exception(f'Unable to process the edge: {self.label} \nedgeDef: {self} \n\nrow: {row} \nexception: {str(ex)}')
        return edges

class PropertyDef(BaseDef):
    def __init__(self, dict_prop):
        self.data = dict_prop
        self.property = dict_prop['property']
        self.data_type = dict_prop['dataType'] if 'dataType' in dict_prop else 'String'
        self.is_multi_value = dict_prop['isMultiValue'] if 'isMultiValue' in dict_prop else False
        self.key = dict_prop['key'] if 'key' in dict_prop else None
        self.value = dict_prop['value'] if 'value' in dict_prop else None
        if self.data_type and self.is_multi_value :
            self.header = self.property + ':' + self.data_type + '[]'
        elif self.data_type :
            self.header = self.property + ':' + self.data_type
        else :
            self.header = self.property

    def get_value(self, row):
        if self.value :
            #TODO handle isMultiValue
            return self.evaluate(row, self.value)
        elif self.key :
            return row[self.key] if self.key in row else None
        else:
            return None