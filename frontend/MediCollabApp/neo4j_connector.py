from py2neo import Graph
import os

class neo4jConnector():
    """
    Connector for neo4j database from Spark
    """

    def __init__(self):
        self.graph = Graph(bolt=True, host='10.0.0.13', password='insight')

if __name__ == "__main__":
    print(neo4jConnector().graph)
