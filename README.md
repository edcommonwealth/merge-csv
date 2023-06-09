# merge-csv
Tool to merge teacher and student data stored in UTF-8 encoded CSV files.

## Dependencies
```bash
pip install numpy
pip install pandas
pip install pysftp
```
## Usage
```bash
python3 merge.py (-stvq) (-d directory) (-r remote)  (-p project)
```
## Arguments
```bash
 -h, --help       show this help message and exit
 -t, --teacher    merge teacher data
 -s, --student    merge student data
 -q, --quiet      run without output (besides errors and warnings)
 -v, --verbose    run with extra output information
 -d, --directory  directory for local csv merging
 -r, --remote-url sftp url for remote merging
 -p, --project    add a project name to the merged csv file name
```

## Testing
```bash
python3 test.py
```