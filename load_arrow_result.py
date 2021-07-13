import duckdb

con = duckdb.connect("benchmark.db")

con.execute("INSERT INTO ENGINE (NAME,HASH) VALUES ('arrow-dplyr','4.0')")
engine_id = con.execute("SELECT MAX(ID) FROM ENGINE").fetchone()[0]

con.execute("INSERT INTO TEST (ENGINE_ID, NAME) VALUES (" + str(engine_id) + ",'Group By')")

file = open('arrow/group_by_result.txt', 'r')
query_results =  file.readlines()

benchmark_id = con.execute("SELECT MAX(ID) FROM TEST").fetchone()[0]

i = 0
query_number = 0
while (i < len(query_results)):
    con.execute("INSERT INTO RESULT (ID_TEST, QUERY, TIME) VALUES (?, ?, ?)",
                             (benchmark_id, query_number, query_results[i]))
    con.execute("INSERT INTO RESULT (ID_TEST, QUERY, TIME) VALUES (?, ?, ?)",
                             (benchmark_id, query_number, query_results[i+1]))
    i += 2
    query_number +=1
