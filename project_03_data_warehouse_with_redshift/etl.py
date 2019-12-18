import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


class LoadInsertTables:
    """Class to load and insert tables (Extract, Transform, Load)"""
    
    def load_staging_tables(self, cur, conn):
        """Load/copy data from S3 bucket onto staging tables using IAM_ROLE"""
        
        for query in copy_table_queries:
            print(query)
            cur.execute(query)
            conn.commit()


    def insert_tables(self, cur, conn):
        """Insert data from staging tables onto final tables (dimension and fact)"""
        
        for query in insert_table_queries:
            print(query)
            cur.execute(query)
            conn.commit()


    def main(self):
        """Main function to load/copy to staging and insert onto final tables using the above functions"""
        
        config = configparser.ConfigParser()
        config.read('dwh.cfg')

        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        self.load_staging_tables(cur, conn)
        self.insert_tables(cur, conn)

        conn.close()


if __name__ == "__main__":
    load_insert_tables = LoadInsertTables()
    load_insert_tables.main()