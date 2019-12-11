import configparser
import psycopg2
from sql_queries import row_count_queries


class GetAllTableCounts:
    """Class to get all the counts of all tables"""
    
    def get_table_counts(self, cur, conn):
        """Get all of table counts"""
        
        for i in row_count_queries:
            print(i)
            cur.execute(i)
        
            results = cur.fetchone()
            
            for j in results:
                print(f"Row count total: {j}\n")
            

    def main(self):
        """Main function to get all the counts of all tables using the above function"""
        
        config = configparser.ConfigParser()
        config.read('dwh.cfg')

        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        self.get_table_counts(cur, conn)

        conn.close()


if __name__ == "__main__":
    get_all_table_counts = GetAllTableCounts()
    get_all_table_counts.main()