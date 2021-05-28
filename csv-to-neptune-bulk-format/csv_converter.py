import sys
import os
import logging
logger = logging.getLogger(__name__)

import argparse
import csv
import data_config

import boto3

import locale

__all__ = []
__version__ = 0.1
__date__ = '2021-01-21'
__updated__ = '2021-01-21'

class RawCSVConverter:
    def __init__(self, conf_file_names, gen_dup_file=False, use_s3=False, local_enc='utf-8'):
        self.gen_dup_file = gen_dup_file
        self.use_s3 = use_s3
        self.local_enc = local_enc
        self.conf_defs =[]
        try:
            data_config.BaseDef.log_stats()
            for conf_file_name in conf_file_names:
                self.conf_defs.append(data_config.ConfigDef(conf_file_name, self.gen_dup_file, self.local_enc))
        except Exception as ex:
            raise Exception(f'Unable to read configuration file {conf_file_name} \nexception: {str(ex)}')
        if self.use_s3 :
            try :
                #self.s3 = boto3.client('s3')
                self.s3 = boto3.resource('s3')
            except Exception as ex:
                raise Exception(f'Unable to connect to s3 \nexception: {str(ex)}')

    def convert_to_csv(self):
        for index1, conf_def in enumerate(self.conf_defs) :
            data_file_names = conf_def.file_names
            # initialize writers
            conf_def.init_writers()
            
            for index2, data_file_name in enumerate(data_file_names):
                try:
                    if self.use_s3 :
                        data_file_name = conf_def.download_source_file(self.s3, data_file_name)
                    else :
                        data_file_name = conf_def.source_folder + '/' + data_file_name
                    logger.info(f'Processing Data File:{index2}:{data_file_name}')
                    with open(data_file_name, newline='', encoding=self.local_enc) as csv_file:
                        reader = csv.DictReader(csv_file, escapechar="\\")
                        #process the file
                        try:
                            conf_def.process_csv_to_csv(reader)
                        except Exception as ex:
                            raise Exception(f'Unable to process the CSV file: {data_file_name} \nexception: {str(ex)}')
                        #close the file
                        csv_file.close()
                except Exception as ex:
                    logger.error(f'Unable to load the CSV file: {data_file_name} \nexception: {str(ex)}')

            # close files
            conf_def.close_writers()
            # delete current files and upload new files
            if self.use_s3 :
                if index1 == 0 : conf_def.delete_data_files(self.s3)
                conf_def.upload_data_files(self.s3)

        if self.gen_dup_file: data_config.BaseDef.write_dup_files()
        #log stats
        data_config.BaseDef.log_stats()
        data_config.BaseDef.clean_stats()
   
def main(argv=None):
    program_name = os.path.basename(sys.argv[0])
    program_version = "v0.1"
    program_build_date = "%s" % __updated__

    program_version_string = '%%prog %s (%s)' % (
        program_version, program_build_date)
    program_longdesc = ("A utility python script to convert CSV data file into the Amazon Neptune CSV format "
                        "for bulk ingestion. See "
                        "https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html."
                        )
    program_license = "Copyright 2018 Amazon.com, Inc. or its affiliates.	\
                Licensed under the Apache License 2.0\nhttp://aws.amazon.com/apache2.0/"
    system_enc = locale.getpreferredencoding()

    if argv is None:
        argv = sys.argv[1:]
    try:
        # setup argument parser
        parser = argparse.ArgumentParser( description=program_license, epilog=program_longdesc)
        parser.add_argument("conf_file_names", nargs='+',
                          help="Space separated, one or more Data Configuration File(s) (json)\nUse separate files if Node/Edges are different.", metavar="DATA_CONF_FILES")
        parser.add_argument("-e", "--enc", default='utf-8', dest='local_enc',
                          help="Optional: encoding for the source files 'utf-8' or 'cp1252'")
        parser.add_argument("--s3", dest='use_s3', action='store_true',
                          help="Use S3 as source and destination of files")
        parser.add_argument("--dup", dest='gen_dup_file', action='store_true',
                          help="Generate file for duplicates")
        parser.add_argument("-v", "--verbose", dest='verbose', action='store_true',
                          help="Emit Verbose logging")

        # process arguments
        args = parser.parse_args(argv)

        conf_file_names = args.conf_file_names
        log_level = logging.DEBUG if args.verbose else logging.INFO
        logging.basicConfig(format='%(asctime)s %(name)s:%(levelname)s:%(message)s', datefmt='%H:%M:%S')
        logging.getLogger(__name__).setLevel(log_level)
        logging.getLogger('data_config').setLevel(log_level)
        #logging.getLogger('botocore').setLevel(logging.ERROR)
        #logging.getLogger('s3transfer').setLevel(logging.ERROR)
        #logging.getLogger('urllib3').setLevel(logging.ERROR)
      
        # MAIN BODY #

        logger.info(f'Processing {conf_file_names}')
        logger.debug(f'System File Encoding: {system_enc}')
        csvConverter = RawCSVConverter(conf_file_names, args.gen_dup_file, args.use_s3, args.local_enc)
        csvConverter.convert_to_csv()
        return 0

    except Exception as e:
        indent = len(program_name) * " "
        logger.error(program_name + ": " + f'{e}' + "\n")
        logger.error(indent + "  for help use --help")
        return 2

if __name__ == '__main__':
    sys.exit(main())