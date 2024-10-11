import csv, logging, os
from pathlib import Path
from typing import Iterator, List

from ..schemas.common_structures import InputData
from ..schemas.reader_configs import CSVReaderConfig
from .base_reader import BaseReader

def get_valid_path(user_specified_path):
    current_file_path = Path(__file__)
    root_path = current_file_path.parent.parent
    if not str(user_specified_path).startswith('/'):
        combined_path = os.path.join(root_path, user_specified_path)
        if os.path.exists(combined_path):
            return combined_path
        

    if os.path.exists(user_specified_path):
        return user_specified_path
    
    raise FileNotFoundError(f'File not found at `{user_specified_path}`')


class CSVReader(BaseReader):
    config: CSVReaderConfig
    default_config= CSVReaderConfig(chunk_size= 10000000000, use_first_column_as_id = False)

    def __init__(self, config: CSVReaderConfig):
        super().__init__(config)
        self.config = config

    def read(self, path:str)-> Iterator[List[InputData]]:
        chunk= []
        issues= []
        chunk_size= self.config.chunk_size
        file_path = get_valid_path(path)
        with open(file_path, 'r') as file:
            header = file.readline().strip().split(",")

            if not header or header[0]=='':
                raise ValueError(f"CSV File at {path} missing header")
            
            file.seek(0)
            reader = csv.DictReader(file)
            for row in reader:
                if any(not value for value in row.values()):
                    issues.append(f"Missing Data on Line {reader.line_num}")
                    continue

                expected_result = None
                if self.config.expected_result_column:
                    expected_result = row.get(self.config.expected_result_column)
                    if expected_result:
                        del row[self.config.expected_result_column]

                    else:
                        issues.append(f"Missing expected result on line {reader.line_num}")

                example_id = self.generate_example_id(row,path)
                input_data_instance = InputData(example_id= example_id, content= row, expected_result = expected_result)
                chunk.append(input_data_instance)

                if len(chunk)>=chunk_size:
                    yield chunk
                    chunk=[]

            if chunk:
                yield chunk
        for issue in issues:
            logging.warning(issue)


BaseReader.register_reader("csv_reader", CSVReader, CSVReaderConfig)
