# %%
from avro.datafile import DataFileReader
from avro.io import DatumReader
import json
import csv
import os
import glob
# %%
'''Output directory for CSV files'''
output_dir = "Output"

# %%


def getDevices(avro_files):
    """
    Extracts and returns a sorted list of unique device identifiers from the list of Avro files.
    """
    res = []
    for avro_file in avro_files:
        arr = avro_file.split(os.sep)
        idx = arr.index('participant_data')
        res.append(arr[idx+2])
    res = sorted(set(res))
    return res


def getDeviceDates(avro_files):
    """
    Extracts and returns a sorted list of unique device identifiers from the list of Avro files.
    """
    res = []
    for avro_file in avro_files:
        arr = avro_file.split(os.sep)
        idx = arr.index('participant_data')
        res.append(arr[idx+1])
    res = sorted(set(res))
    return res


def readAvro(avro_file_path):
    """
    Reads an Avro file and returns the data as a dictionary.
    """
    reader = DataFileReader(open(avro_file_path, "rb"), DatumReader())
    schema = json.loads(reader.meta.get('avro.schema').decode('utf-8'))
    data = next(reader)
    return data


def exportAccelerometer(data, device, date):
    """
    Exports accelerometer data to a CSV file.
    """
    acc = data["rawData"]["accelerometer"]

    # To prevent divide by 0
    if acc["samplingFrequency"] == 0:
        return

    timestamp = [round(acc["timestampStart"] + i * (1e6 / acc["samplingFrequency"]))
                 for i in range(len(acc["x"]))]
    # Convert ADC counts in g
    delta_physical = acc["imuParams"]["physicalMax"] - \
        acc["imuParams"]["physicalMin"]
    delta_digital = acc["imuParams"]["digitalMax"] - \
        acc["imuParams"]["digitalMin"]
    x_g = [val * delta_physical / delta_digital for val in acc["x"]]
    y_g = [val * delta_physical / delta_digital for val in acc["y"]]
    z_g = [val * delta_physical / delta_digital for val in acc["z"]]

    folder_path = os.path.join(output_dir, device, date)
    # Check if the folder exists, if not, create it
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)  # Creates the folder

    file_path = os.path.join(folder_path, 'accelerometer.csv')

    # Check if the file exists
    file_exists = os.path.exists(file_path)

    with open(file_path, 'a', newline='') as f:
        writer = csv.writer(f)

        # If the file does not exist, write the header first
        if not file_exists:
            # Writing header
            writer.writerow(["unix_timestamp", "x", "y", "z"])

        # Append the new rows
        writer.writerows([[ts, x, y, z]
                         for ts, x, y, z in zip(timestamp, x_g, y_g, z_g)])


def exportGyroscope(data, device, date):
    """
    Exports accelerometer data to a CSV file.
    """
    gyro = data["rawData"]["gyroscope"]

    # Gyroscope 為空就不建立檔案
    if len(gyro["x"]) == 0:
        return

    timestamp = [round(gyro["timestampStart"] + i * (1e6 / gyro["samplingFrequency"]))
                 for i in range(len(gyro["x"]))]

    folder_path = os.path.join(output_dir, device, date)
    # Check if the folder exists, if not, create it
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)  # Creates the folder

    file_path = os.path.join(folder_path, 'gyroscope.csv')

    # Check if the file exists
    file_exists = os.path.exists(file_path)

    with open(file_path, 'a', newline='') as f:
        writer = csv.writer(f)

        # If the file does not exist, write the header first
        if not file_exists:
            # Writing header
            writer.writerow(["unix_timestamp", "x", "y", "z"])

        # Append the new rows
        writer.writerows([[ts, x, y, z] for ts, x, y, z in zip(
            timestamp, gyro["x"], gyro["y"], gyro["z"])])


def exportEda(data, device, date):
    """
    Exports EDA (Electrodermal Activity) data to a CSV file.
    """
    eda = data["rawData"]["eda"]

    # To prevent divide by 0
    if eda["samplingFrequency"] == 0:
        return

    timestamp = [round(eda["timestampStart"] + i * (1e6 / eda["samplingFrequency"]))
                 for i in range(len(eda["values"]))]

    folder_path = os.path.join(output_dir, device, date)
    # Check if the folder exists, if not, create it
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)  # Creates the folder

    file_path = os.path.join(folder_path, 'eda.csv')

    # Check if the file exists
    file_exists = os.path.exists(file_path)

    with open(file_path, 'a', newline='') as f:
        writer = csv.writer(f)

        # If the file does not exist, write the header first
        if not file_exists:
            # Writing header
            writer.writerow(["unix_timestamp", "eda"])

        # Append the new rows
        writer.writerows([[ts, eda]
                         for ts, eda in zip(timestamp, eda["values"])])


def exportTemperature(data, device, date):
    """
    Exports temperature data to a CSV file.
    """
    tmp = data["rawData"]["temperature"]

    # To prevent divide by 0
    if tmp["samplingFrequency"] == 0:
        return
    
    timestamp = [round(tmp["timestampStart"] + i * (1e6 / tmp["samplingFrequency"]))
                 for i in range(len(tmp["values"]))]

    folder_path = os.path.join(output_dir, device, date)
    # Check if the folder exists, if not, create it
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)  # Creates the folder

    file_path = os.path.join(folder_path, 'temperature.csv')

    # Check if the file exists
    file_exists = os.path.exists(file_path)

    with open(file_path, 'a', newline='') as f:
        writer = csv.writer(f)

        # If the file does not exist, write the header first
        if not file_exists:
            # Writing header
            writer.writerow(["unix_timestamp", "temperature"])

        # Append the new rows
        writer.writerows([[ts, tmp]
                         for ts, tmp in zip(timestamp, tmp["values"])])


def exportTags(data, device, date):
    """
    Exports tags data to a CSV file.
    """
    tags = data["rawData"]["tags"]

    # tags 為空就不建立檔案
    if len(tags["tagsTimeMicros"]) == 0:
        return

    folder_path = os.path.join(output_dir, device, date)
    # Check if the folder exists, if not, create it
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)  # Creates the folder

    file_path = os.path.join(folder_path, 'tags.csv')

    # Check if the file exists
    file_exists = os.path.exists(file_path)

    with open(file_path, 'a', newline='') as f:
        writer = csv.writer(f)

        # If the file does not exist, write the header first
        if not file_exists:
            # Writing header
            writer.writerow(["tags_timestamp"])

        # Append the new rows
        writer.writerows([[tag] for tag in tags["tagsTimeMicros"]])


def exportBVP(data, device, date):
    """
    Exports BVP (Blood Volume Pulse) data to a CSV file.
    """
    bvp = data["rawData"]["bvp"]

    # To prevent divide by 0
    if bvp["samplingFrequency"] == 0:
        return
    
    timestamp = [round(bvp["timestampStart"] + i * (1e6 / bvp["samplingFrequency"]))
                 for i in range(len(bvp["values"]))]

    folder_path = os.path.join(output_dir, device, date)
    # Check if the folder exists, if not, create it
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)  # Creates the folder

    file_path = os.path.join(folder_path, 'bvp.csv')

    # Check if the file exists
    file_exists = os.path.exists(file_path)

    with open(file_path, 'a', newline='') as f:
        writer = csv.writer(f)

        # If the file does not exist, write the header first
        if not file_exists:
            # Writing header
            writer.writerow(["unix_timestamp", "bvp"])

        # Append the new rows
        writer.writerows([[ts, bvp]
                         for ts, bvp in zip(timestamp, bvp["values"])])


def exportSystolicPeaks(data, device, date):
    """
    Exports systolic peaks data to a CSV file.
    """
    sps = data["rawData"]["systolicPeaks"]

    folder_path = os.path.join(output_dir, device, date)
    # Check if the folder exists, if not, create it
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)  # Creates the folder

    file_path = os.path.join(folder_path, 'systolicPeaks.csv')

    # Check if the file exists
    file_exists = os.path.exists(file_path)

    with open(file_path, 'a', newline='') as f:
        writer = csv.writer(f)

        # If the file does not exist, write the header first
        if not file_exists:
            # Writing header
            writer.writerow(["systolic_peak_timestamp"])

        # Append the new rows
        writer.writerows([[sp] for sp in sps["peaksTimeNanos"]])


def exportSteps(data, device, date):
    """
    Exports steps data to a CSV file.
    """
    steps = data["rawData"]["steps"]
    
    # To prevent divide by 0
    if steps["samplingFrequency"] == 0:
        return
    
    timestamp = [round(steps["timestampStart"] + i * (1e6 / steps["samplingFrequency"]))
                 for i in range(len(steps["values"]))]

    folder_path = os.path.join(output_dir, device, date)
    # Check if the folder exists, if not, create it
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)  # Creates the folder

    file_path = os.path.join(folder_path, 'steps.csv')

    # Check if the file exists
    file_exists = os.path.exists(file_path)

    with open(file_path, 'a', newline='') as f:
        writer = csv.writer(f)

        # If the file does not exist, write the header first
        if not file_exists:
            # Writing header
            writer.writerow(["unix_timestamp", "steps"])

        # Append the new rows
        writer.writerows([[ts, step]
                         for ts, step in zip(timestamp, steps["values"])])


# %%
# Get all Avro files recursively
avro_files = glob.glob("**/*.avro", recursive=True)

# Get unique devices from the Avro files
devices = getDevices(avro_files)

for device in devices:
    # Get Avro files for the same device
    same_device_avro_files = [
        avro_file for avro_file in avro_files if device in avro_file]

    # Get unique dates for the device
    dates = getDeviceDates(same_device_avro_files)

    for date in dates:
        # Get Avro files for the same date
        same_date_avro_files = [
            avro_file for avro_file in same_device_avro_files if date in avro_file]
        same_date_avro_files.sort()

        for same_date_avro_file in same_date_avro_files:
            # Read data from Avro file
            data = readAvro(same_date_avro_file)

            # Export sensors data to csv files
            exportAccelerometer(data, device, date)
            exportGyroscope(data, device, date)
            exportEda(data, device, date)
            exportTemperature(data, device, date)
            exportTags(data, device, date)
            exportBVP(data, device, date)
            exportSystolicPeaks(data, device, date)
            exportSteps(data, device, date)
# %%
