from zipfile import ZipFile
from pathlib import Path
from lxml import etree
from tqdm.notebook import tqdm_notebook
from collections import defaultdict
import pandas as pd
import io

class Zip2CSV(object):
    """class to transform alto2txt metadata files to a csv format"""
    def __init__(self, nlp: str , directory: str='data'):
        """set location of zip files"""
        self.directory = Path(directory)
        self.nlp = nlp
        self.metadata = ZipFile(self.directory / 'metadata' / f'{self.nlp}_metadata.zip')
        self.content = ZipFile(self.directory / 'plaintext' / f'{self.nlp}_plaintext.zip')

    @property
    def xml_files(self):
        """create a generator with all xml files"""
        return (f for f in self.metadata.namelist() if f.endswith('.xml'))
        
    def extract_metadata(self,file: str):
        """retrieve metadata from an alto2txt xml file
        """
        metadata_fields = {'item':['title','item_type','ocr_quality_mean'],'issue':['date']}
        metadata_dict = dict()

        with self.metadata.open(file,'r') as in_xml:
            tree = etree.parse(in_xml)  
            for parent,fields in metadata_fields.items():
                for field in fields:
                    metadata_dict[field] = tree.xpath(f'.//{parent}/{field}')[0].text

        return metadata_dict

    def proces_corpus(self):
        """process the articles, extract metadata from xml 
        and retrieve content from zip archive"""
        self.corpus = defaultdict(dict)
        for xml_file in tqdm_notebook(self.xml_files):
            self.corpus[xml_file] = self.extract_metadata(xml_file)
           
            with io.TextIOWrapper(self.content.open(xml_file[:-4].rstrip('_metadata')+'.txt')) as text: #,'r').read()
                self.corpus[xml_file]['content'] = text.read()

    def convert(self):
        """convert a newspaper to a csv file
        """
        self.proces_corpus()
        df = pd.DataFrame(self.corpus).T
        df.to_csv(f'{self.directory}/{self.nlp}.csv')

class Newspaper(object):
    pass

        

            
