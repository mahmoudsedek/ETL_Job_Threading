import job_config
import threading
import argparse
import time
import json
from math import ceil
import logging
import pandas as pd
import ntpath
import shutil
import sys
from os.path import isfile, join, split
from os import listdir, makedirs
from pathlib import Path



threads = []
number_of_threads = 0
number_of_threads_lock = threading.Lock()

class ETL_thread(threading.Thread):
    """This class has the thread initialization and handling"""
    def __init__(self, logger: logging.RootLogger, rtw_file_path: str, identity_file_path: str, nationality_path: str, employer_path: str, output_path: str, archive_path: str, error_path: str) -> None:
        """Initialize the constructor parameters"""
        super().__init__()
        self.logger = logger
        self.rtw_file_path = rtw_file_path
        self.identity_file_path = identity_file_path
        self.nationality_path = nationality_path
        self.employer_path = employer_path
        self.output_path = output_path
        self.archive_path = archive_path
        self.error_path = error_path
        

    def format_date(self, epoch: str) -> str:
        """Method to format the date as needed, iso8601 timestamp with a BST time zone"""
        time_stamp = pd.to_datetime(epoch, unit='s')
        time_stamp_bst = time_stamp.tz_localize("GMT").tz_convert("Europe/London")
        time_stamp_iso = time_stamp_bst.isoformat()
        time_stamp_formatted = time_stamp_iso[0:-6]
        return time_stamp_formatted


    def read_file(self, file_path: str) -> dict:
        """Method to read the metadata json files and load it as a python dict"""
        with open(file_path, 'r') as file:
            return json.load(file)


    def write_file(self, file_path: str, data_list: list) -> None:
        """Method to write/dump the output files as json"""
        with open(file_path, 'w') as file:
            for item in data_list:
                json.dump(item, file)
                file.write("\n")


    def move_file(self, src_path: str, dst_path: str) -> None:
        """Method to move the files from the source path to the destination one"""
        shutil.move(src_path, dst_path)


    def process(self) -> bool:
        """The actual magic happens here, and it's called by the run method"""
        try:
            try:
                # read static files
                nationality_list = self.read_file(self.nationality_path)
                employer_list = self.read_file(self.employer_path)
            except FileNotFoundError as e:
                self.logger.error("Metadate path doesn't exist...")
                self.logger.error(e, exc_info=True)
                self.logger.error(f"Processing file {self.rtw_file_path} failed due to the absence of one of the following files {self.nationality_path}, {self.employer_path}")
                return False
            
            df_nationality = pd.DataFrame(nationality_list, columns = ["applicant_nationality", "applicant_nationality_name"])
            df_employer = pd.DataFrame(employer_list, columns = ["applicant_employer", "applicant_employer_name"])

            try:
                # read dynamic files
                df_rtw = pd.read_csv(self.rtw_file_path, header=0)
                df_identity = pd.read_csv(self.identity_file_path, header=0)
            except FileNotFoundError as e:
                self.logger.error(f"Corresponding identity file to the right_to_work file named {self.rtw_file_path} doesn't not exist...")
                self.logger.error(e, exc_info=True)
                self.logger.error(f"Processing file {self.rtw_file_path} failed due to the absence of the corresponding identity file...")
                return False

            
            df_identity = df_identity.drop(columns=["unix_timestamp"])

            # join nationality dataframe with right_to_work dataframe
            df_rtw_nationality = df_rtw.merge(df_nationality, how="inner", on="applicant_nationality")
            df_rtw_nationality = df_rtw_nationality.drop(columns=["applicant_nationality"])
            df_rtw_nationality = df_rtw_nationality.rename(columns={"applicant_nationality_name": "applicant_nationality"})

            # join result dataframe with employer dataframe
            df_rtw_nationality_emp = df_rtw_nationality.merge(df_employer, how="inner", on="applicant_employer")
            df_rtw_nationality_emp = df_rtw_nationality_emp.drop(columns=["applicant_employer"])
            df_rtw_nationality_emp = df_rtw_nationality_emp.rename(columns={"applicant_employer_name": "applicant_employer"})
            df_rtw_nationality_emp = df_rtw_nationality_emp[["unix_timestamp", "applicant_id", "applicant_employer", "applicant_nationality", "is_eligble"]]

            # join the result dataframe with identity dataframe to include the "is_verified" column
            df = df_rtw_nationality_emp.merge(df_identity, how="left", on="applicant_id")

            # convert the date format, and int to string to match the output ("1" instead of 1)
            df["unix_timestamp"] = df["unix_timestamp"].apply(self.format_date)
            df = df.rename(columns={"unix_timestamp": "iso8601_timestamp"})
            df["applicant_id"] = df["applicant_id"].astype(str)

            # convert dataframe to json with removing nulls
            json_string = df.apply(lambda x: [x.dropna()], axis=1).to_json()
            data_dict = json.loads(json_string)
            data_list = []
            for value in data_dict.values():
                data_list.append(value[0])
                
            # write the data to the output path
            rtw_file = ntpath.basename(self.rtw_file_path)
            output_file_name = rtw_file.split(".")[0] + ".json"
            output_file_path = join(self.output_path, output_file_name)
            makedirs(self.output_path, exist_ok=True)
            self.write_file(output_file_path, data_list)
            return True
        except Exception as e:
            self.logger.error(f"file {self.rtw_file_path} failed while processing, check the following Traceback...")
            self.logger.error(e, exc_info=True)
            return False


    def run(self) -> None:
        """Runnable method to run the thread"""
        global number_of_threads, number_of_threads_lock
        rtw_file = ntpath.basename(self.rtw_file_path)
        output_file_name = rtw_file.split(".")[0]
        self.logger.info("Hour " + output_file_name + " ETL start.")
        start_time = time.time()
        process_status = self.process()
        if(process_status == True): # processing is done successfully
            # move file to the archive directory
            rtw_dir = split(split(self.rtw_file_path)[0])[1]
            identity_dir = split(split(self.identity_file_path)[0])[1]
            makedirs(join(self.archive_path, rtw_dir), exist_ok=True)
            makedirs(join(self.archive_path, identity_dir), exist_ok=True)
            try:
                self.move_file(self.rtw_file_path, (join(self.archive_path, rtw_dir, rtw_file)))
                self.move_file(self.identity_file_path, (join(self.archive_path, identity_dir, rtw_file)))
            except FileNotFoundError as e:
                self.logger.error("Couldn't move the file to the archive directory check the following Traceback to recognize which file...")
                self.logger.error(e, exc_info=True)
                

            elapsed_time = ceil(time.time() - start_time)
            self.logger.info("Hour " + output_file_name + f" ETL complete,\nelapsed time: {elapsed_time}s.")
            number_of_threads_lock.acquire()
            number_of_threads = number_of_threads - 1
            number_of_threads_lock.release()
        else: # Processing has failed
            # move file to the error directory
            rtw_dir = split(split(self.rtw_file_path)[0])[1]
            identity_dir = split(split(self.identity_file_path)[0])[1]
            makedirs(join(self.error_path, rtw_dir), exist_ok=True)
            makedirs(join(self.error_path, identity_dir), exist_ok=True)
            try:
                self.move_file(self.rtw_file_path, (join(self.error_path, rtw_dir, rtw_file)))
                self.move_file(self.identity_file_path, (join(self.error_path, identity_dir, rtw_file)))
            except FileNotFoundError as e:
                self.logger.error("Couldn't move the file to the error directory check the following Traceback to recognize which file...")
                self.logger.error(e, exc_info=True)
            
            number_of_threads_lock.acquire()
            number_of_threads = number_of_threads - 1
            number_of_threads_lock.release()
        

def prepare_paths(logger: logging.RootLogger) -> tuple:
    """Method to prepare paths of the input & output files, after taking the paths the method tries to first
    list all files if exist in the right_to_work directory then append files names into the identity list,
    since it's one-to-one mapping (one file right_to_work >> one file in identity with the same name"""
    output_path = job_config.OUTPUT_PATH
    rtw_path = job_config.RTW_PATH
    identity_path =job_config.IDENTITY_PATH
    nationality_path = job_config.NATIONALITY_PATH
    employer_path = job_config.EMPLOYER_PATH
    archive_path = job_config.ARCHIVE_PATH
    error_path = job_config.ERROE_PATH

    try:
        rtw_files_path = [join(rtw_path, f) for f in listdir(rtw_path) if isfile(join(rtw_path, f))]
    except FileNotFoundError as e:
        logger.error("input path doesn't exist")
        logger.error(e, exc_info=True)
        logger.error("job will terminate...")
        sys.exit(0)

    identity_files_path = []
    for rtw_file_path in rtw_files_path:
        rtw_file = ntpath.basename(rtw_file_path)
        identity_files_path.append(join(identity_path, rtw_file))
    return (rtw_files_path, identity_files_path, nationality_path, employer_path, output_path, archive_path, error_path)


def get_argument() -> argparse.Namespace:
    """Method to get an argument of how many threads the user wants to have to execute the job"""
    parser = argparse.ArgumentParser("ETL Job", add_help=False)
    parser.add_argument('-h', '--help', action='help', default=argparse.SUPPRESS, help="""
        This is an ETL job, that takes 1 parameter -p which is the number of threads for the job
        meaning one thread per hour/chunk (example: one thread for 2017-07-26-05 files)""")
    parser.add_argument("-p", "--parallelism", default=3, type=int, help="Parallelism parameter with the default value of 3")
    args = parser.parse_args()
    return args


def main() -> None:
    global number_of_threads, number_of_threads_lock, threads
    
    args = get_argument()
    max_threads = args.parallelism
    print(type(max_threads))

    # configure logger
    path = Path(job_config.LOG_PATH)
    makedirs(path.parent.absolute(), exist_ok=True)
    logging.basicConfig(filename=job_config.LOG_PATH, format='[%(asctime)s] - %(levelname)s - From %(threadName)s - %(message)s', filemode='w')
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # prepare paths
    rtw_files_path, identity_files_path, nationality_path, employer_path, output_path, archive_path, error_path = prepare_paths(logger)

    if(rtw_files_path): # if the files list is not empty initialize a thread and start working
        for rtw_file_path, identity_file_path in zip(rtw_files_path, identity_files_path):
            if(number_of_threads < max_threads):
                number_of_threads_lock.acquire()
                number_of_threads = number_of_threads + 1
                number_of_threads_lock.release()
                etl_thread = ETL_thread(logger, rtw_file_path, identity_file_path, nationality_path, employer_path, output_path, archive_path, error_path)
                threads.append(etl_thread)
                etl_thread.start()
            else:
                while(True):
                    time.sleep(1)
                    if(number_of_threads < max_threads):
                        number_of_threads_lock.acquire()
                        number_of_threads = number_of_threads + 1
                        number_of_threads_lock.release()
                        etl_thread = ETL_thread(logger, rtw_file_path, identity_file_path, nationality_path, employer_path, output_path, archive_path, error_path)
                        threads.append(etl_thread)
                        etl_thread.start()
                        break
        
        for thread in threads: # wait for threads to finish
            thread.join()
    
    else:
        logger.warning("There are no files to be processed...")

    print("Exit...")


if __name__ == "__main__":
    main()


