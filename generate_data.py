import os

os.mkdir('data')

os.chdir('data')

os.system('wget https://github.com/cwida/duckdb-data/releases/download/v1.0/J1_1e7_NA_0_0.csv.gz')
os.system('wget https://github.com/cwida/duckdb-data/releases/download/v1.0/J1_1e7_1e1_0_0.csv.gz')
os.system('wget https://github.com/cwida/duckdb-data/releases/download/v1.0/J1_1e7_1e4_0_0.csv.gz')
os.system('wget https://github.com/cwida/duckdb-data/releases/download/v1.0/J1_1e7_1e7_0_0.csv.gz')
os.system('wget https://github.com/cwida/duckdb-data/releases/download/v1.0/G1_1e7_1e2_5_0.csv.gz')
