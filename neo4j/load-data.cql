// https://neo4j.com/docs/cypher-manual/current/administration/constraints/#administration-constraints-node-key

// create nodes
// load customer.csv
CREATE CONSTRAINT customer_id IF NOT EXISTS ON (c:CUSTOMER) ASSERT c.id IS UNIQUE;
CREATE INDEX c_nationkey_id IF NOT EXISTS FOR (c:CUSTOMER) ON (c.C_NATIONKEY);

USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///customer.csv' AS row
FIELDTERMINATOR '|'
MERGE (c:CUSTOMER { id: toInteger(row.C_CUSTKEY) })
ON CREATE SET
    c.C_NAME = row.C_NAME,
	c.C_ADDRESS = row.C_ADDRESS,
    c.C_NATIONKEY = toInteger(row.C_NATIONKEY),
    c.C_PHONE = row.C_PHONE,
    c.C_ACCTBAL = toFloat(row.C_ACCTBAL),
    c.C_MKTSEGMENT = row.C_MKTSEGMENT,
    c.C_COMMENT = row.C_COMMENT
;


// load lineitem.csv
// CREATE CONSTRAINT orderkey_linenumber_id IF NOT EXISTS ON (l:LINEITEM) ASSERT (l.L_ORDERKEY, l.L_LINENUMBER) IS NODE KEY
CREATE INDEX l_orderkey_id IF NOT EXISTS FOR (l:LINEITEM) ON (l.L_ORDERKEY);
CREATE INDEX l_partkey_id IF NOT EXISTS FOR (l:LINEITEM) ON (l.L_PARTKEY);
CREATE INDEX l_suppkey_id IF NOT EXISTS FOR (l:LINEITEM) ON (l.L_SUPPKEY);
CREATE INDEX l_linenumber_id IF NOT EXISTS FOR (l:LINEITEM) ON (l.L_LINENUMBER);

USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///lineitem.csv' AS row
FIELDTERMINATOR '|'
CREATE (l:LINEITEM)
SET
    l.L_ORDERKEY = toInteger(row.L_ORDERKEY),
    l.L_PARTKEY = toInteger(row.L_PARTKEY),
    l.L_SUPPKEY = toInteger(row.L_SUPPKEY),
    l.L_LINENUMBER = toInteger(row.L_LINENUMBER),
    l.L_QUANTITY = toFloat(row.L_QUANTITY),
    l.L_EXTENDEDPRICE = toFloat(row.L_EXTENDEDPRICE),
    l.L_DISCOUNT = toFloat(row.L_DISCOUNT),
    l.L_TAX = toFloat(row.L_TAX),
    l.L_RETURNFLAG = row.L_RETURNFLAG,
    l.L_LINESTATUS = row.L_LINESTATUS,
    l.L_SHIPDATE = row.L_SHIPDATE,
    l.L_COMMITDATE = row.L_COMMITDATE,
    l.L_RECEIPTDATE = row.L_RECEIPTDATE,
    l.L_SHIPINSTRUCT = row.L_SHIPINSTRUCT,
    l.L_SHIPMODE = row.L_SHIPMODE,
    l.L_COMMENT = row.L_COMMENT
;


// load nation.csv
CREATE CONSTRAINT nation_id IF NOT EXISTS ON (n:NATION) ASSERT n.id IS UNIQUE;
CREATE INDEX n_regionkey_id IF NOT EXISTS FOR (n:NATION) ON (n.N_REGIONKEY);

USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///nation.csv' AS row
FIELDTERMINATOR '|'
MERGE (n:NATION { id: toInteger(row.N_NATIONKEY) })
ON CREATE SET
    n.N_NAME = row.N_NAME,
    n.N_REGIONKEY = toInteger(row.N_REGIONKEY),
    n.N_COMMENT = row.N_COMMENT
;


// load orders.csv
CREATE CONSTRAINT orders_id IF NOT EXISTS ON (o:ORDERS) ASSERT o.id IS UNIQUE;
CREATE INDEX o_custkey_id IF NOT EXISTS FOR (o:ORDERS) ON (o.O_CUSTKEY);

USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///orders.csv' AS row
FIELDTERMINATOR '|'
MERGE (o:ORDERS { id: toInteger(row.O_ORDERKEY) })
ON CREATE SET
    o.O_CUSTKEY = toInteger(row.O_CUSTKEY),
    o.O_ORDERSTATUS = row.O_ORDERSTATUS,
    o.O_TOTALPRICE = toFloat(row.O_TOTALPRICE),
    o.O_ORDERDATE = row.O_ORDERDATE,
    o.O_ORDERPRIORITY = row.O_ORDERPRIORITY,
    o.O_CLERK = row.O_CLERK,
    o.O_SHIPPRIORITY = toInteger(row.O_SHIPPRIORITY),
    o.O_COMMENT = row.O_COMMENT
;


// load partsupp.csv
// CREATE CONSTRAINT partkey_suppkey_id IF NOT EXISTS ON (ps:PARTSUPP) ASSERT (ps.PS_PARTKEY, ps.PS_SUPPKEY) IS NODE KEY
CREATE INDEX ps_partkey_id IF NOT EXISTS FOR (ps:PARTSUPP) ON (ps.PS_PARTKEY);
CREATE INDEX ps_suppkey_id IF NOT EXISTS FOR (ps:PARTSUPP) ON (ps.PS_SUPPKEY);

USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///partsupp.csv' AS row
FIELDTERMINATOR '|'
CREATE (ps:PARTSUPP)
SET
    ps.PS_PARTKEY = toInteger(row.PS_PARTKEY),
    ps.PS_SUPPKEY = toInteger(row.PS_SUPPKEY),
    ps.PS_AVAILQTY = toInteger(row.PS_AVAILQTY),
    ps.PS_SUPPLYCOST = toFloat(row.PS_SUPPLYCOST),
    ps.PS_COMMENT = row.PS_COMMENT
;


// load part.csv
CREATE CONSTRAINT part_id IF NOT EXISTS ON (p:PART) ASSERT p.id IS UNIQUE;

USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///part.csv' AS row
FIELDTERMINATOR '|'
MERGE (p:PART { id: toInteger(row.P_PARTKEY) })
ON CREATE SET
    p.P_NAME = row.P_NAME,
    p.P_MFGR = row.P_MFGR,
    p.P_BRAND = row.P_BRAND,
    p.P_TYPE = row.P_TYPE,
    p.P_SIZE = toInteger(row.P_SIZE),
    p.P_CONTAINER = row.P_CONTAINER,
    p.P_RETAILPRICE = toFloat(row.P_RETAILPRICE),
    p.P_COMMENT = row.P_COMMENT
;


// load region.csv
CREATE CONSTRAINT region_id IF NOT EXISTS ON (r:REGION) ASSERT r.id IS UNIQUE;

USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///region.csv' AS row
FIELDTERMINATOR '|'
MERGE (r:REGION { id: toInteger(row.R_REGIONKEY) })
ON CREATE SET
    r.R_NAME = row.R_NAME,
    r.R_COMMENT = row.R_COMMENT
;


// load supplier.csv
CREATE CONSTRAINT supplier_id IF NOT EXISTS ON (s:SUPPLIER) ASSERT s.id IS UNIQUE;
CREATE INDEX s_nationkey_id IF NOT EXISTS FOR (s:SUPPLIER) ON (s.S_NATIONKEY);

USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///supplier.csv' AS row
FIELDTERMINATOR '|'
MERGE (s:SUPPLIER { id: toInteger(row.S_SUPPKEY) })
ON CREATE SET
    s.S_NAME = row.S_NAME,
    s.S_ADDRESS = row.S_ADDRESS,
    s.S_NATIONKEY = toInteger(row.S_NATIONKEY),
    s.S_PHONE = row.S_PHONE,
    s.S_ACCTBAL = toFloat(row.S_ACCTBAL),
    s.S_COMMENT = row.S_COMMENT
;




// create edges
// load customer.csv
USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///customer.csv' AS row
FIELDTERMINATOR '|'
MATCH (c:CUSTOMER { id: toInteger(row.C_CUSTKEY) })
MATCH (n:NATION { id: toInteger(row.C_NATIONKEY) })
MERGE (c)-[:FROM_4]->(n);


// load lineitem.csv
USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///lineitem.csv' AS row
FIELDTERMINATOR '|'
MATCH (l:LINEITEM { L_ORDERKEY: toInteger(row.L_ORDERKEY), L_PARTKEY: toInteger(row.L_PARTKEY), L_SUPPKEY: toInteger(row.L_SUPPKEY), L_LINENUMBER: toInteger(row.L_LINENUMBER) })
MATCH (o:ORDERS { id: toInteger(row.L_ORDERKEY) })
MERGE (l)-[:BELONGS_TO_7]->(o);

USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///lineitem.csv' AS row
FIELDTERMINATOR '|'
MATCH (l:LINEITEM { L_ORDERKEY: toInteger(row.L_ORDERKEY), L_PARTKEY: toInteger(row.L_PARTKEY), L_SUPPKEY: toInteger(row.L_SUPPKEY), L_LINENUMBER: toInteger(row.L_LINENUMBER) })
MATCH (ps:PARTSUPP { PS_PARTKEY:toInteger(row.PS_PARTKEY) , PS_SUPPKEY: toInteger(row.PS_SUPPKEY) })
MERGE (l)-[:COMPOSED_BY_6]->(ps);


// load nation.csv
USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///nation.csv' AS row
FIELDTERMINATOR '|'
MATCH (n:NATION { id: toInteger(row.N_NATIONKEY) })
MATCH (r:REGION { id: toInteger(row.N_REGIONKEY) })
MERGE (n)-[:FROM_10]->(r);


// load orders.csv
USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///orders.csv' AS row
FIELDTERMINATOR '|'
MATCH (o:ORDERS { id: toInteger(row.O_ORDERKEY) })
MATCH (c:CUSTOMER { id: toInteger(row.O_CUSTKEY) })
MERGE (o)-[:BY_5]->(c);


// load partsupp.csv
USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///partsupp.csv' AS row
FIELDTERMINATOR '|'
MATCH (ps:PARTSUPP { PS_PARTKEY: toInteger(row.P_PARTKEY) })
MATCH (p:PART { id: toInteger(row.P_PARTKEY) })
MERGE (ps)-[:COMPOSED_BY_2]->(p);

USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///partsupp.csv' AS row
FIELDTERMINATOR '|'
MATCH (ps:PARTSUPP { PS_SUPPKEY: toInteger(row.PS_SUPPKEY) })
MATCH (s:SUPPLIER { id: toInteger(row.PS_SUPPKEY) })
MERGE (ps)-[:SUPPLIED_BY_3]->(s);


// load supplier.csv
USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///supplier.csv' AS row
FIELDTERMINATOR '|'
MATCH (s:SUPPLIER { id: toInteger(row.S_SUPPKEY) })
MATCH (n:NATION { id: toInteger(row.S_NATIONKEY) })
MERGE (s)-[:BELONGS_TO_1]->(n);
