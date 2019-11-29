import json
from mockdata import mockdata


""" extend csv_mock() class in py-test-utility """
class load_file(mockdata.csv_mock):
    
    def __init__(self, file, type="csv", schema=""):  
      
        self.file_name = file
        self.schema_file_name = schema  
        
        super().__init__(csv=self.file_name, schema=self.schema_file_name)
                
    def to_new_line_delimiter_file(self, output_file):
        
        extract_json = super().to_json()

        f= open(output_file, "w+")
        for d in extract_json:
            f.write(json.dumps(d)) 
            f.write("\n")    
        return 0
