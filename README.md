# merge-csv

## Dependencies
```bash
pip install numpy
pip install python-dotenv
```
## Usage
```bash
python merge-csv.py (-sth) (-d directory)
```
## Arguments
```bash
 -h, --help     show this help message and exit
 -t, --teacher  merge teacher data
 -s, --student  merge student data
 -d, --folder   directory for local csv merging
```
## Example
```bash
python merge-csv.py -st -d ./test-csv/
```