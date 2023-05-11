import pandas as pd
import os
import glob
import numpy as np
import datetime
import itertools as it
import argparse
import re
from urllib.parse import urlparse
import pysftp

#NOTE for now, each of the arrays should be all lowercase
#TODO eventually make them case agnostic
# all of the columns we want to extract from the csv file
# excluding the question ids (they are found using regex)
final_columns_student = {
    'Start Date': ['startdate', 'start date'],
    'End Date': ['enddate', 'end date'],
    'Status': ['status'],
    'Ip Address': ['ip address', 'ipaddress'],
    'Progress': ['progress'],
    'Duration': ['duration', 'duration..in.seconds', 'duration (in seconds)'],
    'District': ['district', 'please select your school district.'],
    'LASID': ['lasid', 'please enter your locally assigned student id number (lasid, or student lunch number).'],
    'Grade': ['grade', 'what grade are you in?'],
    'Gender': ['gender', 'what is your gender?', 'what is your gender? - selected choice'],
    'Race': ['race'],
    'Recorded Date': ['recorded date', 'recordeddate'],
    'Response Id': ['responseid', 'response id'],
    'Dese Id': ['deseid', 'dese id', 'school'],
}

final_columns_teacher = {
    'Start Date': ['startdate', 'start date'],
    'End Date': ['enddate', 'end date'],
    'Status': ['status'],
    'Ip Address': ['ip address', 'ipaddress'],
    'Progress': ['progress'],
    'Duration': ['duration', 'duration..in.seconds', 'duration (in seconds)'],
    'District': ['district', 'please select your school district.'],
    'Recorded Date': ['recorded date', 'recordeddate'],
    'Response Id': ['responseid', 'response id'],
    'Dese Id': ['deseid', 'dese id', 'school'],
}

argVerbose = False
argQuiet = True


class Sftp:
    def __init__(self, hostname, username, password, cnopts, port=22):
        """Constructor Method"""
        # Set connection object to None (initial value)
        self.connection = None
        self.hostname = hostname
        self.username = username
        self.password = password
        self.cnopts = cnopts
        self.port = port

    def connect(self):
        """Connects to the sftp server and returns the sftp connection object"""

        try:
            # Get the sftp connection object
            self.connection = pysftp.Connection(
                host=self.hostname,
                username=self.username,
                password=self.password,
                cnopts=self.cnopts,
                port=self.port,
            )
        except Exception as err:
            raise Exception(err)
        finally:
            if not argQuiet: print(f"Connected to {self.hostname} as {self.username}.")

    def disconnect(self):
        """Closes the sftp connection"""
        self.connection.close()
        if not argQuiet: print(f"Disconnected from host {self.hostname}")

    def listdir(self, remote_path):
        """lists all the files and directories in the specified path and returns them"""
        for obj in self.connection.listdir(remote_path):
            yield obj

    def listdir_attr(self, remote_path):
        """lists all the files and directories (with their attributes) in the specified path and returns them"""
        for attr in self.connection.listdir_attr(remote_path):
            yield attr

    def download(self, remote_path, target_local_path):
        """
        Downloads the file from remote sftp server to local.
        Also, by default extracts the file to the specified target_local_path
        """

        try:
            if not argQuiet: print(
                f"downloading from {self.hostname} as {self.username} [(remote path : {remote_path});(local path: {target_local_path})]"
            )

            # Create the target directory if it does not exist
            path, _ = os.path.split(target_local_path)
            if not os.path.isdir(path):
                try:
                    os.makedirs(path)
                except Exception as err:
                    raise Exception(err)

            # Download from remote sftp server to local
            self.connection.get(remote_path, target_local_path)
            if not argQuiet: print("download completed")

        except Exception as err:
            raise Exception(err)

    def upload(self, source_local_path, remote_path):
        """
        Uploads the source files from local to the sftp server.
        """

        try:
            if not argQuiet: print(
                f"uploading to {self.hostname} as {self.username} [(remote path: {remote_path});(source local path: {source_local_path})]"
            )

            # Download file from SFTP
            self.connection.put(source_local_path, remote_path)
            if not argQuiet: print("upload completed")

        except Exception as err:
            raise Exception(err)


# prepare csv and merged csv directories
def prep_dir(folder=''):
    # prepare directories
    cwd = os.path.join(os.getcwd(), folder)
    mwd = os.path.join(cwd, 'merged')
    if not os.path.exists(mwd):
        if argVerbose: print(f'Creating directory {mwd}')
        os.mkdir(mwd)
    if argVerbose: print('Source data directory: ' + cwd)
    if argVerbose: print('Merged data directory: ' + mwd)
    return cwd, mwd


# get current date in Month-XX-YYYY format
def get_date():
    return datetime.date.today().strftime("%B-%d-%Y")


# in dataframe df, merges any column in possibilities into the final column col
def combine_cols(df, col, possibilities):
    # if final column doesn't exist, create it
    if col not in df.columns:
        tmpdf = pd.DataFrame([np.nan], columns=[col])
        df = pd.concat((df, tmpdf), axis=1)
    # list to store replaced columns
    drops = []
    # for every column possibility that does exist...
    for cl in df.columns:
        if cl.lower() in possibilities:
            # we don't want to merge and drop our final column
            if cl == col:
                continue
            # replace the column...
            if argVerbose: print(f'Replacing column {cl}')
            df[col] = df[col].replace(r'^\s*$', np.nan, regex=True).fillna(df[cl])
            # and add it to the drop list
            drops.append(cl)
    # drop spent columns
    df = df.drop(columns=drops)
    if argVerbose: print(f'Dropped columns: {drops}')
    return df


# removes unused columns from student data
def clean_cols_student(df):
    keep = list(final_columns_student.keys())
    keep = list(map(str.lower, keep))
    drops = []
    question_pattern = re.compile("^s-[a-zA-Z]{4}-q[0-9][0-9]?$")
    for col in df.columns:
        if col.lower() not in keep and not bool(question_pattern.match(col)):
            drops.append(col)
    df = df.drop(columns=drops)
    if argVerbose: print(f'Dropped columns: {drops}')
    return df


# removes unused columns from teacher data
def clean_cols_teacher(df):
    keep = list(final_columns_teacher.keys())
    keep = list(map(str.lower, keep))
    drops = []
    question_pattern = re.compile("^t-[a-zA-Z]{4}-q[0-9][0-9]?$")
    for col in df.columns:
        if col.lower() not in keep and not bool(question_pattern.match(col)):
            drops.append(col)
    df = df.drop(columns=drops)
    if argVerbose: print(f'Dropped columns: {drops}')
    return df


# performs all merging operations for student data
def do_merge_student(cwd, mwd):
    # identify and merge student files
    if not argQuiet: print('---Merging Student Data---')
    all_files = glob.glob(os.path.join(cwd, "*student*.csv"))
    if not argQuiet: print(f'Found {len(all_files)} Student CSV files')
    if len(all_files) < 1:
        if not argQuiet: print('No files found. Skipping merge...')
        return
    if not argQuiet: print('Merging...')
    files = [pd.read_csv(f, low_memory=False) for f in all_files]
    # count lines in read csv files
    lines = 0
    for fi in files:
        lines += fi.shape[0]
    # combine csv files
    df = pd.concat(files, axis=0)
    # combine related columns
    if not argQuiet: print('Repairing rows...')
    df = repair_student_columns(df)
    # clean out unnecessary columns
    if not argQuiet: print('Cleaning out columns...')
    df = clean_cols_student(df)
    # ensure line count matches what is expected
    if df.shape[0] != lines:
        print(f'Warning: Line count mismatch: {lines} expected, but got {df.shape[0]}')
    # save merged file
    date = get_date()
    if args.project:
        proj = '-' + args.project
    else:
        proj = ''
    fn = f'{date}{proj}-student-data-merged.csv'
    df.to_csv(os.path.join(mwd, fn), index=False)
    if not argQuiet: print('Student data merged successfully!')
    return fn


# performs all merging operations for teacher data
def do_merge_teacher(cwd, mwd):
    # identify and merge teacher files
    if not argQuiet: print('---Merging Teacher Data---')
    all_files = glob.glob(os.path.join(cwd, "*teacher*.csv"))
    if not argQuiet: print(f'Found {len(all_files)} Teacher CSV files')
    if len(all_files) < 1:
        if not argQuiet: print('No files found. Skipping merge...')
        return
    if not argQuiet: print('Merging...')
    files = [pd.read_csv(f, low_memory=False) for f in all_files]
    # count lines in read csv files
    lines = 0
    for f in files:
        lines += f.shape[0]
    # combine csv files
    df = pd.concat(files, axis=0)
    # combine related columns
    if not argQuiet: print('Repairing columns...')
    df = repair_teacher_columns(df)
    # clean out unnecessary columns
    if not argQuiet: print('Cleaning out columns...')
    df = clean_cols_teacher(df)
    # ensure line count matches what is expected
    if df.shape[0] != lines:
        print(f'Warning: Line count mismatch: {lines} expected, but got {df.shape[0]}')
    # save merged file
    date = get_date()
    if args.project:
        proj = '-' + args.project
    else:
        proj = ''
    fn = f'{date}{proj}-teacher-data-merged.csv'
    df.to_csv(os.path.join(mwd, fn), index=False)
    if not argQuiet: print('Teacher data merged successfully!')
    return fn


# merges teacher columns that may have mismatched names
def repair_teacher_columns(df):
    for col in final_columns_teacher:
        df = combine_cols(df, col, final_columns_teacher[col])
    return df


# merges student columns that may have mismatched names, 
# and combines question variants
def repair_student_columns(df):
    for col in final_columns_student:
        df = combine_cols(df, col, final_columns_student[col])
    if not argQuiet: print('Combining Question Variants...')
    df = combine_variants(df)
    return df


# combines question variants into non-variant columns
def combine_variants(df):
    drops = []
    for col in df:
        x = re.search(r'^s-[a-z]{4}-q[0-9][0-9]?-1$', col)
        if x is not None:
            # get non variant version
            nonvar = col[:-2]
            # combine into non variant
            df[nonvar] = df[nonvar].replace(r'^\s*$', np.nan, regex=True).fillna(df[col])
            # and add it to the drop list
            drops.append(col)
    df = df.drop(columns=drops)
    return df



if __name__ == '__main__':

    # parse flags
    parser = argparse.ArgumentParser(
        prog='merge-csv',
        description='Merges CSV Files containing student and teacher data',
        epilog='Usage: python merge-csv.py (-stq) (-d directory) (-r remote) (-p project)')
    parser.add_argument('-d', '--directory',
                        action='store',
                        help='directory for local csv , defaults to current directory')
    parser.add_argument('-t', '--teacher',
                        action='store_true',
                        dest='teacher',
                        help='merge teacher data')
    parser.add_argument('-s', '--student',
                        action='store_true',
                        dest='student',
                        help='merge student data')
    parser.add_argument('-q', '--quiet',
                        action='store_true',
                        dest='quiet',
                        help='run without output (besides errors and warnings)')
    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        dest='verbose',
                        help='run with extra output information')
    parser.add_argument('-p', '--project',
                        action='store',
                        help='add a project name to the merged csv file name')
    parser.add_argument('-r', '--remote-url',
                        action='store',
                        dest='remote_url',
                        help='sftp url for remote merging')
    args = parser.parse_args()

    argVerbose = args.verbose
    argQuiet = args.quiet

    #quiet takes precedence over verbose
    if argQuiet:
        argVerbose = False

    # make sure -s or -t is set
    if not (args.student or args.teacher):
        if not argQuiet: print('Notice: Neither -s nor -t are specified. No merge will be performed.')

    if args.directory and not args.remote_url:
        c, m = prep_dir(args.directory)
    elif not args.directory:
        if not argQuiet: print('Notice: No directory specified. Defaulting to current directory.')
        c, m = prep_dir()

    # prepare sftp if flagged
    if args.remote_url:
        if not argQuiet: print(f'Remote destination set, fetching files...')
        parsed_url = urlparse(args.remote_url)
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        sftp = Sftp(
            hostname=parsed_url.hostname,
            username=parsed_url.username,
            password=parsed_url.password,
            cnopts=cnopts,
        )
        sftp.connect()
        # current directory is used for remote file merging
        c, m = prep_dir()

        # prepare remote path
        path = args.directory
        if not path:
            path = "/"
        # ensure trailing slash
        if not path.endswith("/"): path += "/"

        filelist = []
        # get csv list from remote
        for file in sftp.listdir_attr(path):
            if file.filename.endswith(".csv"):
                filelist.append(file.filename)
                if not argQuiet: print(f'Fetching file {file.filename}...')
                sftp.download(path + file.filename, c + file.filename)

    # perform merges
    if args.teacher:
        tmd = do_merge_teacher(c, m)
    if args.student:
        smd = do_merge_student(c, m)

    if args.remote_url:
        # upload tmd and smd to remote
        if not argQuiet: print('Uploading merged data...')
        sftp.upload(m + '/' + tmd, path + 'merged/' + tmd)
        sftp.upload(m + '/' + smd, path + 'merged/' + smd)
        # remove merged directory
        if not argQuiet: print('Cleaning up...')
        os.remove(m + '/' + tmd)
        os.remove(m + '/' + smd)
        os.rmdir(m)
        # remove downloaded files
        for f in filelist:
            if os.path.exists(f):
                os.remove(f)
        sftp.disconnect()
    if not argQuiet: print('Done!')
