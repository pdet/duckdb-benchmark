import statistics
import duckdb
import time
import pyarrow
import pyarrow.csv
import os


class BenchmarkRunner:
    create_statements = None
    arrow_tables = None
    drop_statements = None
    queries = []
    con = None
    con_test = None

    # We run the benchmark 5 times and get the mean value
    def execute_benchmark(self, name, engine_id):
        print("Running " + name)
        self.con.execute("INSERT INTO TEST (ENGINE_ID, NAME) VALUES ( ?, ?)",
                         (engine_id, name))
        benchmark_id = self.con.execute("SELECT MAX(ID) FROM TEST").fetchone()[0]
        
        for create_statement in self.create_statements:
            self.con_test.execute(create_statement)
        query_number = 0
        for query in self.queries:
            total_time = []
            print("Query # " + str(query_number))
            for i in range(2):
                start_time = time.time()
                self.con_test.execute(query)
                cur_time = time.time() - start_time
                self.con_test.execute("DROP TABLE ans;")

                total_time.append(cur_time)

            self.con.execute("INSERT INTO RESULT (ID_TEST, QUERY, TIME_FIRST, TIME_SECOND) VALUES (?, ?, ?, ?)",
                             (benchmark_id, query_number, total_time[0],total_time[1]))
            query_number += 1
        for drop_statement in self.drop_statements:
            self.con_test.execute(drop_statement)

    def __init__(self, create_sql, drop_sql, queries, con, con_test):
        self.create_statements = create_sql
        self.drop_statements = drop_sql
        self.queries = queries
        self.con = con
        self.con_test = con_test


class DuckDBBenchmark:
    con = None
    commit_hash = None
    name = None
    con_test = None

    def start(self):
        print ("Start Engine "+self.name)
        run = self.con.execute(
            "SELECT * FROM ENGINE WHERE name = '" + self.name + "' AND HASH = '" + self.commit_hash + "'").fetchone()
        # We already executed this engine
        if run is not None:
            return
        # TODO Should be able to change to commits and build
        self.con.execute("INSERT INTO ENGINE (NAME,HASH) VALUES ( ?, ?)",
                         (self.name, self.commit_hash))
        engine_id = self.con.execute("SELECT MAX(ID) FROM ENGINE").fetchone()[0]
        self.run_group_by(engine_id)
        # self.run_join(engine_id)

    def get_queries(self, file_path):
        file = open(file_path, 'r')
        return file.readlines()

    def run_group_by(self, engine_id):
        create_sql = ["CREATE TABLE IF NOT EXISTS x_group AS SELECT * FROM read_csv_auto('data/G1_1e7_1e2_5_0.csv.gz');"]
        drop_sql = ["DROP TABLE IF EXISTS x_group"]
        queries = self.get_queries('group_by_queries.txt')
        runner = BenchmarkRunner(create_sql, drop_sql, queries, self.con, self.con_test)
        runner.execute_benchmark("Group By", engine_id)

    def run_join(self, engine_id):
        create_sql = self.get_queries('join_create.txt')
        drop_sql = self.get_queries('join_drop.txt')
        queries = self.get_queries('join_queries.txt')
        runner = BenchmarkRunner(create_sql, drop_sql, queries, self.con, self.con_test)
        runner.execute_benchmark("Join", engine_id)

    def __init__(self, con, commit_hash, name, con_test):
        self.name = name
        self.commit_hash = commit_hash
        self.con = con
        self.con_test = con_test


def load_arrow(con):
    arrow_table = pyarrow.csv.read_csv('data/G1_1e7_1e2_5_0.csv.gz').to_batches(2500000)
    arrow_table = pyarrow.Table.from_batches(arrow_table)
    con .register_arrow("x_group", arrow_table)

    # arrow_table = pyarrow.Table.from_batches(pyarrow.csv.read_csv('data/J1_1e7_NA_0_0.csv.gz').to_batches(2500000))
    # con.from_arrow_table(arrow_table).create('x')

    # arrow_table = pyarrow.Table.from_batches(pyarrow.csv.read_csv('data/J1_1e7_1e1_0_0.csv.gz').to_batches(2500000))
    # con.from_arrow_table(arrow_table).create('small')

    # arrow_table = pyarrow.Table.from_batches(pyarrow.csv.read_csv('data/J1_1e7_1e4_0_0.csv.gz').to_batches(2500000))
    # con.from_arrow_table(arrow_table).create('medium')

    # arrow_table = pyarrow.Table.from_batches(pyarrow.csv.read_csv('data/J1_1e7_1e7_0_0.csv.gz').to_batches(2500000))
    # con.from_arrow_table(arrow_table).create('big')


con = duckdb.connect("benchmark.db")

con_test = duckdb.connect()

benchmark = DuckDBBenchmark(con, "Master", "DuckDB",con_test)
benchmark.start()

load_arrow(con_test)
benchmark = DuckDBBenchmark(con, "Master", "DuckDB-Arrow",con_test)
benchmark.start()

con.execute("PRAGMA threads=4")
con.execute("PRAGMA force_parallelism")

benchmark = DuckDBBenchmark(con, "Master", "DuckDB-4T",con_test)
benchmark.start()

load_arrow(con)
benchmark = DuckDBBenchmark(con, "Master", "DuckDB-Arrow-4T",con_test)
benchmark.start()
