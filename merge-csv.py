import pandas as pd
import os
import glob
from dotenv import load_dotenv
import numpy as np
import datetime
import itertools as it
import argparse
import pprint


def prep_dir(folder):
    # prepare directories
    cwd = os.path.join(os.getcwd(), folder)
    mwd = os.path.join(cwd, 'merged')
    if not os.path.exists(mwd):
        os.mkdir(mwd)
    return cwd, mwd


def get_date():
    return datetime.date.today().strftime("%B-%d-%Y")


def cap_permutations(s):
    lu_sequence = ((c.lower(), c.upper()) for c in s)
    return [''.join(x) for x in it.product(*lu_sequence)]


def combine_rows(df, col, possibilities):
    # if final column doesn't exist, create it
    if col not in df.columns:
        df[col] = np.nan
    # generate all upper/lowercase possibilities for columns
    allp = []
    for p in possibilities:
        allp += cap_permutations(p)
    # also have to remove the final column from the possibilities
    while col in allp:
        allp.remove(col)
    # list to store replaced columns
    drops = []
    # for every column possibility that does exist...
    for c in allp:
        if c in df.columns:
            # replace the column...
            # print(f'Replacing column {c}')
            df[col] = df[col].replace(r'^\s*$', np.nan, regex=True).fillna(df[c])
            # and add it to the drop list
            drops.append(c)
    # drop spent columns
    df = df.drop(columns=drops)
    # print(f'Dropped columns: {drops}')
    return df


def do_merge_student(cwd, mwd):
    # identify and merge student files
    all_files = glob.glob(os.path.join(cwd, "*student*.csv"))
    print(all_files)
    df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
    date = get_date()
    df.to_csv(os.path.join(mwd, f'{date}-student-data-merged.csv'))


def do_merge_teacher(cwd, mwd):
    # identify and merge teacher files
    print('---Merging Teacher Data---')
    all_files = glob.glob(os.path.join(cwd, "*teacher*.csv"))
    print(f'Found {len(all_files)} CSV files')
    print('Merging...')
    files = [pd.read_csv(f) for f in all_files]
    lines = 0
    for f in files:
        lines += f.shape[0]
    df = pd.concat(files, ignore_index=True)
    print('Repairing rows...')
    df = repair_teacher_rows(df)
    if df.shape[0] != lines:
        print(f'Warning! Line count mismatch: {lines} expected, but got {df.shape[0]}')
    date = get_date()
    df.to_csv(os.path.join(mwd, f'{date}-teacher-data-merged.csv'))
    print('Teacher data merged successfully!')


def repair_teacher_rows(df):
    df = combine_rows(df, 'Recorded Date', ['recorded date', 'recordeddate'])
    df = combine_rows(df, 'Response ID', ['Responseid', 'Response id'])
    df = combine_rows(df, 'DeseId', ['deseid', 'dese id', 'school'])
    return df


if __name__ == '__main__':
    # load environment vars
    load_dotenv()
    # parse flags
    parser = argparse.ArgumentParser(
        prog='merge-csv',
        description='Merges CSV Files containing student and teacher data',
        epilog='Usage: python merge-csv.py (-sth) (directory)')
    parser.add_argument('-d', '--folder',
                        action='store',
                        help='directory for local csv merging')
    parser.add_argument('-t', '--teacher',
                        action='store_true',
                        dest='teacher',
                        help='merge teacher data')  # only merge teacher data
    parser.add_argument('-s', '--student',
                        action='store_true',
                        dest='student',
                        help='merge student data')  # on/off flag
    args = parser.parse_args()
    # make sure -s or -t is set
    if not (args.student or args.teacher):
        print('Warning: Neither -s nor -t are specified. No merge will be performed.')
    # do merge
    c, m = prep_dir(args.folder)
    if args.teacher:
        do_merge_teacher(c, m)
    # if args.student:
        # do_merge_student(c, m)