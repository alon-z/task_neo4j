from neo4j import GraphDatabase
import os


class Neo4jConnection:

    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri,
                                                 auth=(self.__user,
                                                       self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(
                database=db) if db is not None else self.__driver.session()
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response


class NeoCreator:

    def __init__(self, conn):
        self.__conn = conn

    def create_root_node(self, path):
        query = f"""
            CREATE (e: Directory {{path: "{path}"}})
            RETURN id(e) AS node_id
        """
        response = conn.query(query)
        return response

    def add_node(self, type, root, attributes):
        query = f"""
            MATCH (r: Directory) WHERE ID(r) = {root}
            CREATE (e: {type} {{{attributes}}})
            CREATE (r)-[l: `HAS-CHILD`]->(e)
            RETURN id(e) AS node_id
        """.replace("\\u", "\\\\u")
        response = conn.query(query)
        return response

    def add_node_directory(self, root, path):
        return self.add_node("Directory", root, f'path: "{path}"')

    def add_node_file(self, root, path):
        return self.add_node("File", root, f'path: "{path}"')

    def add_folder_to_graph(self, root, folder):
        if root is None:
            root = creator.create_root_node(folder)[0]["node_id"]
        else:
            root = self.add_node_directory(root, folder)[0]["node_id"]
        files = [f.path for f in os.scandir(folder) if f.is_file()]
        for file in files:
            self.add_node_file(root, file)
        subfolders = [f.path for f in os.scandir(folder) if f.is_dir()]
        for folder in subfolders:
            self.add_folder_to_graph(root, folder)


if __name__ == "__main__":
    # greeter = HelloWorldExample("bolt://localhost:7687", "neo4j", "asdfasdf")
    # greeter.print_greeting("hello, world")
    # greeter.close()
    # traverse root directory, and list directories as dirs and files as files
    conn = Neo4jConnection(uri="bolt://localhost:7687",
                           user="neo4j",
                           pwd="asdfasdf")
    creator = NeoCreator(conn)
    creator.add_folder_to_graph(None, ".")
    # for root, dirs, files in os.walk("."):
    #     root_id = creator.add_node(root_id, root)[0]["node_id"]
    # node_id = root_id
    # for dir in dirs:
    #     node_id = creator.add_node(node_id, dir)[0]["node_id"]
    # for root, d_names, f_names in os.walk("."):
    #     print(root, d_names, f_names)
