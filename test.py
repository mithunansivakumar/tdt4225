import os
from datetime import datetime

####
f = open("data/dataset/labeled_ids.txt")
lines = f.read().splitlines()
f.close()
###

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
        lines = f.readlines()[1:]
        f.close()

        for line in lines:
            line = line.strip().split("\t")
            start_time = datetime.strptime(line[0], labels_datetime_format)
            end_time = datetime.strptime(line[1], labels_datetime_format)
            transport_mode = line[2]
            labels[start_time] = [end_time, transport_mode]

    for activity in trajectories:
        activity_mode = "other"
        activity_id = 0
        activity_path = os.path.join(trajectories_path, activity)
        f = open(activity_path)
        lines = f.readlines()[6:] # The first 6 lines are the header
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
                    activity_mode = vals[1]
        
        #query = """INSERT INTO %s (id, user_id, transportation_mode, start_date_time, end_date_time) VALUES (%d, %s, %s, %s, %s)"""
        #self.cursor.execute(query % (table_name, transport_mode, first_date_time, last_date_time))
        #activity_id = self.cursor.lastrowid

        for line in lines:
            print(line)
            break 






        break

        
        


        

    


