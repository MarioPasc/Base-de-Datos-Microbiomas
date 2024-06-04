import matplotlib.pyplot as plt
import seaborn as sns
import os


class OverallAnalysis:
    
    def __init__(self, xml_path:str, mongo_path:str, sql_path:str) -> None:
        self.xml_path = xml_path
        self.mongo_path = mongo_path
        self.sql_path = sql_path
        