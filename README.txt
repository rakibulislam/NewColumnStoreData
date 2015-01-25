Cheetah Project
This is a fast in-memory data analytics engine.

- - - 

To run:

make clean

make
./run SimpleQueryExecutor -method NewColStoreEng -data_file testjson/nobench_data.json -query_log testjson/nobench.query -print_summary -data_format 1 -to_file <name of the file to which selected result will be written to>
