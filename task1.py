from DbConnector import DbConnector
from tabulate import tabulate
import logging
import os
from datetime import datetime

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
    
    def get_user_ids(self):
        user_ids = os.walk("data/dataset/Data")
        for i in user_ids:
            user_ids = i[1]
            break
        return user_ids


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
        user_ids = self.get_user_ids()

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
    
    def insert_acitivty_data(self):
        path = "data/dataset/Data"
        dirs =  os.listdir(path)
        dirs = ["129"]

        labels_datetime_format = "%Y/%m/%d %H:%M:%S"
        activity_datetime_format = "%Y-%m-%d %H:%M:%S"

        for user in dirs:
            trajectories_path = os.path.join(path, user, "Trajectory")
            trajectories = os.listdir(trajectories_path)

            labels_path = os.path.join(path, user, "labels.txt")
            labels_exists = os.path.exists(labels_path)

            labels = {}

            if labels_exists:
                f = open(labels_path)
                lines = [line for line in f.readlines()[1:] if line.strip()]
                f.close()

                for line in lines:
                    line = line.strip().split("\t")
                    start_time = datetime.strptime(line[0], labels_datetime_format)
                    end_time = datetime.strptime(line[1], labels_datetime_format)
                    transport_mode = line[2]
                    labels[start_time] = [end_time, transport_mode]

            for activity in trajectories:
                transport_mode = "other"
                activity_id = 0
                activity_path = os.path.join(trajectories_path, activity)
                f = open(activity_path)
                lines = [line for line in f.readlines()[6:] if line.strip()] # The first 6 lines are the header
                number_of_lines = len(lines)
                f.close()

                if(number_of_lines > 2500):
                    continue

                # TODO fix DRY?
                first_date_time = lines[0].split(",")[-2:]
                first_date_time[1] = first_date_time[1].strip() 
                first_date_time = datetime.strptime(f'{first_date_time[0]} {first_date_time[1]}', activity_datetime_format)

                last_date_time = lines[-1].split(",")[-2:]
                last_date_time[1] = last_date_time[1].strip()
                last_date_time = datetime.strptime(f'{last_date_time[0]} {last_date_time[1]}', activity_datetime_format)

                if labels_exists:
                    is_key_present = first_date_time in labels
                    if is_key_present:
                        vals = labels[first_date_time]
                        if last_date_time == vals[0]:
                            transport_mode = vals[1]
                
                query = """INSERT INTO Activity (id, user_id, transportation_mode, start_date_time, end_date_time) VALUES (%d, %s, %s, %s)"""
                self.cursor.execute(query % (user, transport_mode, first_date_time, last_date_time))
                activity_id = self.cursor.lastrowid

                query = """INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%d, %d, %d, %d, %d, %s)"""

                for line in lines:
                    point = line.strip().split(",")
                    lat = point[0]
                    lon = point[1]
                    altitude = point[3]
                    date_days = point[4]
                    date_time = datetime.strptime(f'{point[5]} {point[6]}', activity_datetime_format)

                    self.cursor.execute(query % (activity_id, lat, lon, altitude, date_days, date_time ))

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


def main():
    program = None
    try:
        program = Task1()
        program.create_user_table("User")
        program.create_acitivity_table("Activity")
        program.create_trackpoint_table("TrackPoint")
        program.show_tables()

        program.insert_user_data("User")
        program.insert_acitivty_data()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()

