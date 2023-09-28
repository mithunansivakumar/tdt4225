from DbConnector import DbConnector
from tabulate import tabulate
import logging
import os
from datetime import datetime
from tqdm import tqdm

class Task1:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor


    def reset(self):
        query = "DROP TABLE IF EXISTS TrackPoint;"
        self.cursor.execute(query)

        query = "DROP TABLE IF EXISTS Activity;"
        self.cursor.execute(query)        
    
        query = "DROP TABLE IF EXISTS User;"
        self.cursor.execute(query)       
      
        self.db_connection.commit()         

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
        print("before query")
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id VARCHAR(30) NOT NULL PRIMARY KEY,
                   has_labels BOOLEAN)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()
        print("after query")
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
                   end_date_time DATETIME,
                   FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE
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
                   FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE
                   )
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()
    
    def insert_acitivty_data(self, activity_table_name, trackpoint_table_name):
        path = "data/dataset/Data"
        dirs =  os.listdir(path)
        #dirs = ["129"] #TODO comment after testing

        labels_datetime_format = "%Y/%m/%d %H:%M:%S"
        activity_datetime_format = "%Y-%m-%d %H:%M:%S"

        for user in tqdm(dirs):
            user_id_formatted = user.lstrip('0')
            if user_id_formatted == '':
                user_id_formatted = '0'
            #print(user_id_formatted)
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
                first_date_time_db = f'{first_date_time[0]} {first_date_time[1]}'
                first_date_time = datetime.strptime(f'{first_date_time[0]} {first_date_time[1]}', activity_datetime_format)

                last_date_time = lines[-1].split(",")[-2:]
                last_date_time[1] = last_date_time[1].strip()
                last_date_time_db = f'{last_date_time[0]} {last_date_time[1]}'
                last_date_time = datetime.strptime(f'{last_date_time[0]} {last_date_time[1]}', activity_datetime_format)

                if labels_exists:
                    is_key_present = first_date_time in labels
                    if is_key_present:
                        vals = labels[first_date_time]
                        if last_date_time == vals[0]:
                            transport_mode = vals[1]
                
                activity_query = """INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES ('%s', '%s', '%s', '%s')"""

                self.cursor.execute(activity_query % (user_id_formatted, transport_mode, first_date_time_db, last_date_time_db))
                activity_id = self.cursor.lastrowid

                trackpoint_query = """INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"""
                trackpoints = []

                for line in lines:
                    point = line.strip().split(",")
                    lat = float(point[0])
                    lon = float(point[1])
                    altitude = int(float(point[3])) #TODO Altitude in feet (-777 if not valid). Er dette noe vi må sette eller det lagt til?
                    date_days = float(point[4])
                    date_time = datetime.strptime(f'{point[5]} {point[6]}', activity_datetime_format)

                    trackpoint = (activity_id, lat, lon, altitude, date_days, date_time)
                    trackpoints.append(trackpoint)

                self.cursor.executemany(trackpoint_query, trackpoints)

                trackpoints = []

                self.db_connection.commit()

def main():
    program = None
    try:
        print("Task 1 ...")
        program = Task1()
        print("Reset db")
        program.reset()
        program.create_user_table("User")
        print("Added user table")
        program.create_acitivity_table("Activity")
        print("Added activity table")
        program.create_trackpoint_table("TrackPoint")
        print("Added trackpoint table")
        program.insert_user_data("User")
        print("inert user table")
        program.insert_acitivty_data("Acitivity", "TrackPoint")
        print("insert activity and trackpoint table")

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()

