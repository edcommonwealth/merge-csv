# merge-csv

## Dependencies
```bash
pip install numpy
pip install pandas
pip install pysftp
```
## Usage
```bash
python merge-csv.py (-stq) (-d directory) (-r remote)  (-p project)
```
## Arguments
```bash
 -h, --help       show this help message and exit
 -t, --teacher    merge teacher data
 -s, --student    merge student data
 -q, --quiet      run without output (besides errors and warnings)
 -d, --directory  directory for local csv merging
 -r, --remote-url sftp url for remote merging
 -p, --project    add a project name to the merged csv file name
```
## Examples
```bash
python merge-csv.py -st -d ./test-csv/
```