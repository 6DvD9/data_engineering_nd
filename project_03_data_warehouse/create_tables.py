import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


class CreateDropTables:
    """Class to create and/or drop database tables"""
    
    def drop_tables(self, cur, conn):
        """Drop tables from the database"""
        
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()


    def create_tables(self, cur, conn):
        """Create tables is the db"""
        
        for query in create_table_queries:
            print(query)
            cur.execute(query)
            conn.commit()


    def main(self):
        """Main function to drop tables and create tables using the above functions"""
        
        config = configparser.ConfigParser()
        config.read('dwh.cfg')

        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        self.drop_tables(cur, conn)
        self.create_tables(cur, conn)

        conn.close()


if __name__ == "__main__":
    create_drop_tables = CreateDropTables()
    create_drop_tables.main()