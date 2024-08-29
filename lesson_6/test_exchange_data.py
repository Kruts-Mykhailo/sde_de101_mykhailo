from ..src.data_processor.exchange_data import run_pipeline
from datetime import datetime
import os
import csv
import pytest

# Test exchange data pipeline

class TestExchangeDataETL:

    def get_project_root(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_dir, os.pardir))
        return project_root
    
    def get_partition_key(self):
        return 'test'

    @pytest.fixture
    def destination_file_path(self):
        partition_key = self.get_partition_key()
        file_path = os.path.join(self.get_project_root(), 'processed_data', 'exchange_data', f'{partition_key}.csv')
        return file_path
    
    @pytest.mark.order(1)
    def test_fetch_exchange_date(self, destination_file_path):
        """
        Test that run_pipeline function creates the csv file in the desired location
        """
        
        run_pipeline(self.get_partition_key)

        assert os.path.isfile(destination_file_path), f"File not found in expected location."
        assert os.path.getsize(destination_file_path) > 0, f"File is empty"

    @pytest.mark.order(2)
    def test_file_structure(self, destination_file_path):
        """
        Test if loaded csv has the expected column structure
        """
        
        if not os.path.exists(destination_file_path):
            pytest.skip("CSV file does not exist, skipping...")

        expected_columns = ['id', 'name', 'rank', 'percentTotalVolume', 'volumeUsd', 'tradingPairs', 'socker', 'nexchangeUrl', 'updated']

        with open(destination_file_path, mode='r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader) 

            assert header == expected_columns, f"CSV header does not match expected structure. Found: {header}"



    @pytest.fixture(scope='module', autouse=True)   
    def tear_down(self, destination_file_path):
        yield

        print("Teardown after all tests in class completed")
        
        if os.path.exists(destination_file_path):
            os.remove(destination_file_path)