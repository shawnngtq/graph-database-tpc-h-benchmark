# Graph database TPC-H benchmark

## What is it?

I want to find out the performance of the most popular graph databases as of 2021. I am using TPC-H datasets to conduct the benchmark.

## Graph databases

Based on DB-Engines (<https://db-engines.com/en/ranking/graph+dbms>) ranking of GDBMS, here are the "pure" graph model DB I will test.

- Neo4j
- JanusGraph
- TigerGraph
- Dgraph

## Dependencies

- Install TPC-H tools (<http://www.tpc.org/tpch/>) and generate the data
- Convert the `tbl` files into `csv` and `json` so that the DB are able to import them

## How to start

After you config the `make` file, `dbgen` executable file will be generated. Execute `dbgen` to generate `tbl` data files. Then convert the `tbl` files into `csv` files.

```python
import pandas as pd

# customer.tbl
names = ["C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT"]
a = pd.read_csv("customer.tbl", header=None, index_col=False, names=names, sep="|")
a.info()
a.to_csv("customer.csv", index=False, sep="|")

# lineitem.tbl
names = ["L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX", "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE", "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT"]
a = pd.read_csv("lineitem.tbl", header=None, index_col=False, names=names, sep="|")
a.info()
a.to_csv("lineitem.csv", index=False, sep="|")

# nation.tbl
names = ["N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"]
a = pd.read_csv("nation.tbl", header=None, index_col=False, names=names, sep="|")
a.info()
a.to_csv("nation.csv", index=False, sep="|")

# orders.tbl
names = ["O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE", "O_ORDERDATE", "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT"]
a = pd.read_csv("orders.tbl", header=None, index_col=False, names=names, sep="|")
a.info()
a.to_csv("orders.csv", index=False, sep="|")

# partsupp.tbl
names = ["PS_PARTKEY", "PS_SUPPKEY", "PS_AVAILQTY", "PS_SUPPLYCOST", "PS_COMMENT"]
a = pd.read_csv("partsupp.tbl", header=None, index_col=False, names=names, sep="|")
a.info()
a.to_csv("partsupp.csv", index=False, sep="|")

# part.tbl
names = ["P_PARTKEY", "P_NAME", "P_MFGR", "P_BRAND", "P_TYPE", "P_SIZE", "P_CONTAINER", "P_RETAILPRICE", "P_COMMENT"]
a = pd.read_csv("part.tbl", header=None, index_col=False, names=names, sep="|")
a.info()
a.to_csv("part.csv", index=False, sep="|")

# region.tbl
names = ["R_REGIONKEY", "R_NAME", "R_COMMENT"]
a = pd.read_csv("region.tbl", header=None, index_col=False, names=names, sep="|")
a.info()
a.to_csv("region.csv", index=False, sep="|")

# supplier.tbl
names = ["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT"]
a = pd.read_csv("supplier.tbl", header=None, index_col=False, names=names, sep="|")
a.info()
a.to_csv("supplier.csv", index=False, sep="|")
```

## License
