// create node
// load customer.csv
graph = TinkerGraph.open()
g = graph.traversal()
graph.createIndex('cust_key', Vertex.class)
graph.createIndex('C_NATIONKEY', Vertex.class)

timeStart = System.currentTimeMillis()
new File('customer.csv').eachLine(0) {line, number ->
    if (number != 0) {
        (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT) = line.split('\\|')
        // println(line)
        // println(C_CUSTKEY)
        // println(C_CUSTKEY.getClass())
        // println(C_CUSTKEY.isInteger())
        g.V().has('customer', 'cust_key', C_CUSTKEY).fold().coalesce(
            unfold(), 
            addV('customer')
                .property('cust_key', C_CUSTKEY)
                .property('C_NAME', C_NAME)
                .property('C_ADDRESS', C_ADDRESS)
                .property('C_NATIONKEY', C_NATIONKEY)
                .property('C_PHONE', C_PHONE)
                .property('C_ACCTBAL', C_ACCTBAL)
                .property('C_MKTSEGMENT', C_MKTSEGMENT)
                .property('C_COMMENT', C_COMMENT)
        ).next()
    }
}
timeEnd = System.currentTimeMillis()
println("load customer.csv time taken: " + timeEnd - timeStart)


// load lineitem.csv
graph = TinkerGraph.open()
g = graph.traversal()
graph.createIndex('L_ORDERKEY', Vertex.class)
graph.createIndex('L_PARTKEY', Vertex.class)
graph.createIndex('L_SUPPKEY', Vertex.class)
graph.createIndex('L_LINENUMBER', Vertex.class)

timeStart = System.currentTimeMillis()
new File('lineitem.csv').eachLine(0) {line, number ->
    if (number != 0) {
        (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) = line.split('\\|')
        g.V().has('lineitem', 'L_ORDERKEY', L_ORDERKEY).has('L_LINENUMBER', L_LINENUMBER).fold().coalesce(
            unfold(), 
            addV('lineitem')
                .property('L_ORDERKEY', L_ORDERKEY)
                .property('L_PARTKEY', L_PARTKEY)
                .property('L_SUPPKEY', L_SUPPKEY)
                .property('L_LINENUMBER', L_LINENUMBER)
                .property('L_QUANTITY', L_QUANTITY)
                .property('L_EXTENDEDPRICE', L_EXTENDEDPRICE)
                .property('L_DISCOUNT', L_DISCOUNT)
                .property('L_TAX', L_TAX)
                .property('L_RETURNFLAG', L_RETURNFLAG)
                .property('L_LINESTATUS', L_LINESTATUS)
                .property('L_SHIPDATE', L_SHIPDATE)
                .property('L_COMMITDATE', L_COMMITDATE)
                .property('L_RECEIPTDATE', L_RECEIPTDATE)
                .property('L_SHIPINSTRUCT', L_SHIPINSTRUCT)
                .property('L_SHIPMODE', L_SHIPMODE)
                .property('L_COMMENT', L_COMMENT)
        ).next()
    }
}
timeEnd = System.currentTimeMillis()
println("load lineitem.csv time taken: " + timeEnd - timeStart)


// load nation.csv
graph = TinkerGraph.open()
g = graph.traversal()
graph.createIndex('nation_key', Vertex.class)
graph.createIndex('N_REGIONKEY', Vertex.class)

timeStart = System.currentTimeMillis()
new File('nation.csv').eachLine(0) {line, number ->
    if (number != 0) {
        (N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT) = line.split('\\|')
        g.V().has('nation', 'nation_key', N_NATIONKEY).fold().coalesce(
            unfold(), 
            addV('nation')
                .property('nation_key', N_NATIONKEY)
                .property('N_NAME', N_NAME)
                .property('N_REGIONKEY', N_REGIONKEY)
                .property('N_COMMENT', N_COMMENT)
        ).next()
    }
}
timeEnd = System.currentTimeMillis()
println("load nation.csv time taken: " + timeEnd - timeStart)


// load orders.csv
graph = TinkerGraph.open()
g = graph.traversal()
graph.createIndex('order_key', Vertex.class)
graph.createIndex('O_CUSTKEY', Vertex.class)

timeStart = System.currentTimeMillis()
new File('orders.csv').eachLine(0) {line, number ->
    if (number != 0) {
        (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT) = line.split('\\|')
        g.V().has('orders', 'order_key', O_ORDERKEY).fold().coalesce(
            unfold(), 
            addV('orders')
                .property('order_key', O_ORDERKEY)
                .property('O_CUSTKEY', O_CUSTKEY)
                .property('O_ORDERSTATUS', O_ORDERSTATUS)
                .property('O_TOTALPRICE', O_TOTALPRICE)
                .property('O_ORDERDATE', O_ORDERDATE)
                .property('O_ORDERPRIORITY', O_ORDERPRIORITY)
                .property('O_CLERK', O_CLERK)
                .property('O_SHIPPRIORITY', O_SHIPPRIORITY)
                .property('O_COMMENT', O_COMMENT)
        ).next()
    }
}
timeEnd = System.currentTimeMillis()
println("load orders.csv time taken: " + timeEnd - timeStart)


// load partsupp.csv
graph = TinkerGraph.open()
g = graph.traversal()
graph.createIndex('PS_PARTKEY', Vertex.class)
graph.createIndex('PS_SUPPKEY', Vertex.class)

timeStart = System.currentTimeMillis()
new File('partsupp.csv').eachLine(0) {line, number ->
    if (number != 0) {
        (PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT) = line.split('\\|')
        g.V().has('partsupp', 'PS_PARTKEY', PS_PARTKEY).has('PS_SUPPKEY', PS_SUPPKEY).fold().coalesce(
            unfold(), 
            addV('partsupp')
                .property('PS_PARTKEY', PS_PARTKEY)
                .property('PS_SUPPKEY', PS_SUPPKEY)
                .property('PS_AVAILQTY', PS_AVAILQTY)
                .property('PS_SUPPLYCOST', PS_SUPPLYCOST)
        ).next()
    }
}
timeEnd = System.currentTimeMillis()
println("load partsupp.csv time taken: " + timeEnd - timeStart)


// load part.csv
graph = TinkerGraph.open()
g = graph.traversal()
graph.createIndex('part_key', Vertex.class)

timeStart = System.currentTimeMillis()
new File('part.csv').eachLine(0) {line, number ->
    if (number != 0) {
        (P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT) = line.split('\\|')
        g.V().has('part', 'part_key', P_PARTKEY).fold().coalesce(
            unfold(), 
            addV('part')
                .property('part_key', P_PARTKEY)
                .property('P_NAME', P_NAME)
                .property('P_MFGR', P_MFGR)
                .property('P_BRAND', P_BRAND)
                .property('P_TYPE', P_TYPE)
                .property('P_SIZE', P_SIZE)
                .property('P_CONTAINER', P_CONTAINER)
                .property('P_RETAILPRICE', P_RETAILPRICE)
                .property('P_COMMENT', P_COMMENT)
        ).next()
    }
}
timeEnd = System.currentTimeMillis()
println("load part.csv time taken: " + timeEnd - timeStart)


// load region.csv
graph = TinkerGraph.open()
g = graph.traversal()
graph.createIndex('region_key', Vertex.class)

timeStart = System.currentTimeMillis()
new File('region.csv').eachLine(0) {line, number ->
    if (number != 0) {
        (R_REGIONKEY, R_NAME, R_COMMENT) = line.split('\\|')
        g.V().has('region', 'region_key', R_REGIONKEY).fold().coalesce(
            unfold(), 
            addV('region')
                .property('region_key', R_REGIONKEY)
                .property('R_NAME', R_NAME)
                .property('R_COMMENT', R_COMMENT)
        ).next()
    }
}
timeEnd = System.currentTimeMillis()
println("load region.csv time taken: " + timeEnd - timeStart)



// load supplier.csv
graph = TinkerGraph.open()
g = graph.traversal()
graph.createIndex('supp_key', Vertex.class)
graph.createIndex('S_NATIONKEY', Vertex.class)

timeStart = System.currentTimeMillis()
new File('supplier.csv').eachLine(0) {line, number ->
    if (number != 0) {
        (S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT) = line.split('\\|')
        g.V().has('supplier', 'supp_key', S_SUPPKEY).fold().coalesce(
            unfold(), 
            addV('supplier')
                .property('supp_key', S_SUPPKEY)
                .property('S_NAME', S_NAME)
                .property('S_ADDRESS', S_ADDRESS)
                .property('S_NATIONKEY', S_NATIONKEY)
                .property('S_PHONE', S_PHONE)
                .property('S_ACCTBAL', S_ACCTBAL)
                .property('S_COMMENT', S_COMMENT)
        ).next()
    }
}
timeEnd = System.currentTimeMillis()
println("load supplier.csv time taken: " + timeEnd - timeStart)




// create edges
timeStart = System.currentTimeMillis()
new File('customer.csv').eachLine(0) {line, number ->
    if (number != 0) {
        (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT) = line.split('\\|')
        g.addE('FROM_4').from(
            g.V().has('customer', 'cust_key', C_CUSTKEY)
        ).to(
            g.V().has('nation', 'nation_key', C_NATIONKEY)
        ).iterate()
    }
}
timeEnd = System.currentTimeMillis()
println("load customer.csv edges time taken: " + timeEnd - timeStart)


// load lineitem.csv
timeStart = System.currentTimeMillis()
new File('lineitem.csv').eachLine(0) {line, number ->
    if (number != 0) {
        (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) = line.split('\\|')
        g.addE('BELONGS_TO_7').from(
            g.V().has('lineitem', 'L_LINENUMBER', L_LINENUMBER).has('L_ORDERKEY', L_ORDERKEY)
        ).to(
            g.V().has('orders', 'order_key', L_ORDERKEY)
        ).iterate()
    }
}
timeEnd = System.currentTimeMillis()
println("load lineitem.csv edges time taken: " + timeEnd - timeStart)

timeStart = System.currentTimeMillis()
new File('lineitem.csv').eachLine(0) {line, number ->
    if (number != 0) {
        (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) = line.split('\\|')
        g.addE('COMPOSED_BY_6').from(
            g.V().has('lineitem', 'L_LINENUMBER', L_LINENUMBER).has('L_ORDERKEY', L_ORDERKEY)
        ).to(
            g.V().has('partsupp', 'PS_PARTKEY', L_PARTKEY).has('PS_SUPPKEY', L_SUPPKEY)
        ).iterate()
    }
}
timeEnd = System.currentTimeMillis()
println("load lineitem.csv edges time taken: " + timeEnd - timeStart)


// load nation.csv
timeStart = System.currentTimeMillis()
new File('nation.csv').eachLine(0) {line, number ->
    if (number != 0) {
        (N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT) = line.split('\\|')
        g.addE('FROM_10').from(
            g.V().has('nation', 'nation_key', N_NATIONKEY)
        ).to(
            g.V().has('region', 'region_key', N_REGIONKEY)
        ).iterate()
    }
}
timeEnd = System.currentTimeMillis()
println("load nation.csv edges time taken: " + timeEnd - timeStart)


// load orders.csv
timeStart = System.currentTimeMillis()
new File('orders.csv').eachLine(0) {line, number ->
    if (number != 0) {
        (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT) = line.split('\\|')
        g.addE('FROM_10').from(
            g.V().has('orders', 'order_key', O_ORDERKEY)
        ).to(
            g.V().has('customer', 'cust_key', O_CUSTKEY)
        ).iterate()
    }
}
timeEnd = System.currentTimeMillis()
println("load orders.csv edges time taken: " + timeEnd - timeStart)


// load partsupp.csv
timeStart = System.currentTimeMillis()
new File('partsupp.csv').eachLine(0) {line, number ->
    if (number != 0) {
        (PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT) = line.split('\\|')
        g.addE('COMPOSED_BY_2').from(
            g.V().has('partsupp', 'PS_PARTKEY', PS_PARTKEY)
        ).to(
            g.V().has('part', 'part_key', PS_PARTKEY)
        ).iterate()
    }
}
timeEnd = System.currentTimeMillis()
println("load partsupp.csv edges time taken: " + timeEnd - timeStart)

timeStart = System.currentTimeMillis()
new File('partsupp.csv').eachLine(0) {line, number ->
    if (number != 0) {
        (PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT) = line.split('\\|')
        g.addE('SUPPLIED_BY_3').from(
            g.V().has('partsupp', 'PS_SUPPKEY', PS_SUPPKEY)
        ).to(
            g.V().has('supplier', 'supp_key', PS_SUPPKEY)
        ).iterate()
    }
}
timeEnd = System.currentTimeMillis()
println("load partsupp.csv edges time taken: " + timeEnd - timeStart)


// load supplier.csv
timeStart = System.currentTimeMillis()
new File('supplier.csv').eachLine(0) {line, number ->
    if (number != 0) {
        (S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT) = line.split('\\|')
        g.addE('BELONGS_TO_1').from(
            g.V().has('supplier', 'supp_key', S_SUPPKEY)
        ).to(
            g.V().has('nation', 'nation_key', S_NATIONKEY)
        ).iterate()
    }
}
timeEnd = System.currentTimeMillis()
println("load supplier.csv edges time taken: " + timeEnd - timeStart)
