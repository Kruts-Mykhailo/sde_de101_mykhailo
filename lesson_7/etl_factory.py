from abc import ABC, abstractmethod

class FactoryDuckDBETL(ABC):
    

    @abstractmethod
    def extract(self, conn):
        pass

    @abstractmethod
    def load(self, conn):
        pass

    @abstractmethod
    def transform(self, conn, partition_key):
        pass
    
   
    def run_pipeline(self, conn, partition_key):
        self.extract(conn)
        self.transform(conn, partition_key)
        self.load(conn)
        
