from DbConnector import DbConnector
from tabulate import tabulate
import logging
import os

class Task1:

    def __init__(self):
        print("hei")
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor        

    def get_labels(self):
        f = open("data/dataset/labeled_ids.txt")
        lines = f.read().splitlines()
        f.close()
        return lines


    def create_user_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id VARCHAR(30),
                   has_labels BOOLEAN)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()
        logging.info("created user table")

    
    def insert_user_data(self, table_name):
        labels = self.get_labels()
        print(labels)
        user_ids = os.walk("data/dataset/Data")
        for i in user_ids:
            user_ids = i[1]
            break

        query = """INSERT INTO %s (id, has_labels) VALUES (%s, %s)"""

        for i in user_ids:
            has_labels = True if i in labels else False
            self.cursor.execute(query % (table_name, i, has_labels))

        self.db_connection.commit()

        


        logging.info("inserted user data")

    def create_acitivity_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   user_id VARCHAR(30),
                   transportation_mode VARCHAR(30),
                   start_date_time DATETIME,
                   end_date_time DATETIME
                   FOREIGN KEY KEY(user_id) REFERENCES User(id) ON DELETE CASCADE
                   )
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_trackpoint_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   activity_id INT NOT NULL,
                   lat DOUBLE,
                   lon DOUBLE,
                   altitude INT,
                   date_days DOUBLE,
                   date_time DATETIME,
                   FOREIGN KEY KEY(activity_id) REFERENCES Activity(id) ON DELETE CASCADE
                   )
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()
    
    def insert_user_data(self):
        print("hei")


def main():
    program = None
    try:
        program = Task1()
        program.create_user_table("User")
        program.create_acitivity_table("Activity")
        program.create_trackpoint_table("TrackPoint")

        # Check that the table is dropped
        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()

