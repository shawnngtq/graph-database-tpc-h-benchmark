// Q1
WITH date('1998-12-01') - duration('P90D') AS my_date
MATCH (l:LINEITEM)
WHERE date(l.L_SHIPDATE) <= date(my_date)
RETURN 
    l.L_RETURNFLAG,
    l.L_LINESTATUS,
    sum(l.L_QUANTITY) AS sum_qty,
    sum(l.L_EXTENDEDPRICE) AS sum_base_price,
    sum(l.L_EXTENDEDPRICE*(1-l.L_DISCOUNT)) AS sum_disc_price,
    sum(l.L_EXTENDEDPRICE*(1-l.L_DISCOUNT)*(1+l.L_TAX)) AS sum_charge,
    avg(l.L_QUANTITY) AS avg_qty,
    avg(l.L_EXTENDEDPRICE) AS avg_price,
    avg(l.L_DISCOUNT) AS avg_disc,
    COUNT(*) AS count_order
ORDER BY l.L_RETURNFLAG, l.L_LINESTATUS;


// Q2
MATCH (p:PARTSUPP)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:BELONG_TO]->(n:NATION)-[:IS_FROM]->(r:REGION) 
    WHERE r.R_NAME = 'EUROPE' WITH p, min(p.PS_SUPPLYCOST) AS minvalue
MATCH (ps:PARTSUPP)-[:COMPOSED_BY]->(p:PART)
MATCH (ps:PARTSUPP)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:BELONG_TO]->(n:NATION)-[:IS_FROM]->(r:REGION)
WHERE p.P_SIZE = 15 
    AND p.P_TYPE =~ '.*BRASS.*' 
    AND r.R_NAME = 'EUROPE' 
    AND p.PS_SUPPLYCOST = minvalue
RETURN 
    p.id, 
    p.P_MFGR, 
    s.S_ACCTBAL, 
    s.S_NAME, 
    s.S_ADDRESS, 
    s.S_PHONE, 
    s.S_COMMENT, n.N_NAME
ORDER BY 
    s.S_ACCTBAL DESC, 
    n.N_NAME, 
    s.S_NAME, 
    p.id;


// Q3
MATCH (l:LINEITEM)-[:IS_PART_OF]->(o:ORDER)-[:MADE_BY]->(c:CUSTOMER)
WHERE c.C_MKTSEGMENT = 'BUILDING' 
    AND date(o.O_ORDERDATE) < date('1995-03-15') 
    AND date(l.L_SHIPDATE) > date('1995-03-15')
RETURN 
    o.id, 
    sum(l.L_EXTENDEDPRICE*(1-l.L_DISCOUNT)) AS REVENUE, 
    o.O_ORDERDATE, 
    o.O_SHIPPRIORITY
ORDER BY 
    REVENUE DESC, 
    o.O_ORDERDATE
LIMIT 10;


// Q4
WITH date('1993-07-01') + duration('P3M') AS my_date
MATCH (l:LINEITEM)-[:IS_PART_OF]->(o:ORDER)
WHERE date(l.L_COMMITDATE) < date(l.L_RECEIPTDATE) 
    AND date(o.O_ORDERDATE) >= date('1993-07-01') 
    AND date(o.O_ORDERDATE) < date(my_date)
RETURN o.O_ORDERPRIORITY, COUNT(*) AS ORDER_COUNT
ORDER BY o.O_ORDERPRIORITY;


// Q5
WITH date('1994-01-01') + duration('P1Y') AS my_date
MATCH (l:LINEITEM)-[:IS_PART_OF]->(o:ORDER)-[:MADE_BY]->(c:CUSTOMER)-[:BELONG_TO]->(n:NATION)-[:IS_FROM]-(r:REGION)
WHERE r.R_NAME = 'ASIA' 
    AND date(o.O_ORDERDATE) >= date('1994-01-01') 
    AND date(o.O_ORDERDATE) < date(my_date)
RETURN n.N_NAME, SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS REVENUE
ORDER BY REVENUE DESC;


// Q6
MATCH (l:LINEITEM)
WHERE date(l.L_SHIPDATE) >= date('1994-01-01')
    AND date(l.L_SHIPDATE) < date('1995-01-01')
    AND toFloat(l.L_DISCOUNT) > 0.06 - 0.01
    AND toFloat(l.L_DISCOUNT) < 0.06 + 0.01
    AND l.L_QUANTITY < 24
RETURN sum(l.L_EXTENDEDPRICE * toFloat(l.L_DISCOUNT)) AS revenue;


// Q7
MATCH (l:LINEITEM)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:BELONG_TO]->(n1:NATION)
MATCH (l:LINEITEM)-[:IS_PART_OF]->(o:ORDER)-[:MADE_BY]->(c:CUSTOMER)-[:BELONG_TO]->(n2:NATION) 
WHERE (n1.N_NAME = 'FRANCE'AND n2.N_NAME = 'GERMANY')
    OR (n1.N_NAME = 'GERMANY' AND n2.N_NAME = 'FRANCE')
    AND date(l.L_SHIPDATE) > date('1995-01-01')
    AND date(l.L_SHIPDATE) < date('1996-12-31')
RETURN n1.N_NAME AS supp_nation, 
    n2.N_NAME AS cust_nation, 
    date(l.L_SHIPDATE).year AS l_year, 
    sum(l.L_EXTENDEDPRICE * (1-toFloat(l.L_DISCOUNT))) AS volume
ORDER BY supp_nation, cust_nation, l_year;


// Q8
MATCH (l:LINEITEM)-[:COMPOSED_BY]->(p:PART)
MATCH (l:LINEITEM)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:BELONG_TO]->(n2:NATION)
MATCH (l:LINEITEM)-[:IS_PART_OF]->(o:ORDER)-[:MADE_BY]->(c:CUSTOMER)-[:BELONG_TO]->(n1:NATION)
MATCH (n2:NATION)-[:IS_FROM]->(r:REGION)
MATCH (n1:NATION)-[:IS_FROM]->(r:REGION)
WHERE r.R_NAME = 'AMERICA'
    AND date(o.O_ORDERDATE) > date('1995-01-01')
    AND date(o.O_ORDERDATE) < date('1996-12-31')
    AND p.P_TYPE = 'ECONOMY ANODIZED STEEL'
WITH o, 
    l, 
    n2, 
    date(o.O_ORDERDATE) AS o_year, 
    sum(l.L_EXTENDEDPRICE * (1-toFloat(l.L_DISCOUNT))) AS volume, 
    n2.N_NAME AS nation
RETURN distinct o_year,
sum(CASE WHEN n2.N_NAME = 'BRAZIL'
    THEN volume
    ELSE 0 END) /volume AS mkt_share
ORDER BY o_year;


// Q9
MATCH (li:LINEITEM)-[:SUPPLIED_BY]->(s:SUPPLIER) 
MATCH (ps:PARTSUPP)-[:SUPPLIED_BY]->(s:SUPPLIER)
MATCH (ps:PARTSUPP)-[:COMPOSED_BY]->(p:PART)
MATCH (s:SUPPLIER)-[:BELONG_TO]->(n:NATION)
MATCH (li:LINEITEM)-[:IS_PART_OF]->(o:ORDER)
WHERE p.P_NAME contains 'green'
WITH li, ps, n, o 
ORDER BY n.N_NAME DESC, date(o.O_ORDERDATE).year
RETURN n.N_NAME AS nation, 
    date(o.O_ORDERDATE).year AS year,
    sum(toFloat(li.L_EXTENDEDPRICE) * (1 -toFloat(li.L_DISCOUNT))-ps.PS_SUPPLYCOST * li.L_QUANTITY) AS amount;


// Q10
MATCH (l:LINEITEM)-[:IS_PART_OF]->(o:ORDER)-[:MADE_BY]->(c:CUSTOMER)-[:BELONG_TO]->(n:NATION)
WHERE date(o.O_ORDERDATE) >= date('1993-10-01')
    AND date(o.O_ORDERDATE) < date('1994-01-01')
    AND l.L_RETURNFLAG = 'R'
RETURN c.id,c.C_NAME, c.C_ACCTBAL, n.N_NAME, c.C_ADDRESS, c.C_PHONE, c.C_COMMENT, sum(toFloat(l.L_EXTENDEDPRICE) * (1-toFloat(l.L_DISCOUNT))) AS revenue
ORDER BY revenue DESC
LIMIT 20;


// Q11
MATCH (p:PARTSUPP)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:BELONG_TO]->(n:NATION)
WHERE n.N_NAME = 'GERMANY'
WITH p, 
    sum(toFloat(p.PS_SUPPLYCOST) * toFloat(p.PS_AVAILQTY)) * 0.0001 AS subquery, 
    sum(toFloat(p.PS_SUPPLYCOST) * toFloat(p.PS_AVAILQTY)) AS value 
MATCH (p:PARTSUPP)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:BELONG_TO]->(n:NATION)
WHERE n.N_NAME = 'GERMANY' AND value > subquery
RETURN p.PS_PARTKEY, value
ORDER BY value DESC;


// Q12
MATCH (l:LINEITEM)-[:IS_PART_OF]->(o:ORDER)
WHERE toUpper(l.L_SHIPMODE) in ['MAIL','SHIP'] 
    AND date(l.L_COMMITDATE) < date(l.L_RECEIPTDATE)
    AND date(l.L_SHIPDATE) < date(l.L_COMMITDATE)
    AND date(l.L_RECEIPTDATE) >= date('1994-01-01')
    AND date(l.L_RECEIPTDATE) < date('1995-01-01')
RETURN 
	l.L_SHIPMODE,
    sum(CASE WHEN o.O_ORDERPRIORITY IN ['1-URGENT','2-HIGH']
    THEN 1
    ELSE 0 END) AS high_line_count,
    sum(CASE WHEN o.O_ORDERPRIORITY IN ['3-MEDIUM','4-NOT SPECIFIED','5-LOW']
    THEN 1
    ELSE 0 END) AS low_line_count
 ORDER BY l.L_SHIPMODE;


// Q13
MATCH (o:ORDER)
OPTIONAL MATCH (o:ORDER)-[:MADE_BY]->(c:CUSTOMER)
WHERE NOT (o.O_COMMENT =~ '.*special.*.*requests.*')
WITH c.id AS c_id, COUNT(o.id) AS c_count 
RETURN c_count, COUNT(c_id) AS custdist
ORDER BY custdist DESC, c_count DESC;


// Q14
WITH date('1995-09-01') + duration('P1M') AS my_date
MATCH (l:LINEITEM)-[:COMPOSED_BY]->(p:PART)
WHERE date(l.L_SHIPDATE) >= date('1995-09-01') AND date(l.L_SHIPDATE) < date(my_date)
RETURN 100 * SUM(CASE WHEN p.P_TYPE =~ '.*PROMO.*' THEN l.L_EXTENDEDPRICE*(1 - l.L_DISCOUNT) ELSE 0 END) / SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS PROMO_REVENUE;


// Q15


// Q16
MATCH (s:SUPPLIER) WHERE s.S_COMMENT =~ '.*Customer.*.*Complaints.*' WITH s, s.id AS p_id
MATCH (ps:PARTSUPP)-[:COMPOSED_BY]->(p:PART)
WHERE p.P_BRAND <> 'Brand#45' 
    AND NOT (p.P_TYPE =~ '.MEDIUM POLISHED.*')
    AND p.P_SIZE in [49,14,23,45,19,3,36,9]
    AND NOT ps.PS_SUPPKEY IN p_id
RETURN p.P_BRAND,p.P_TYPE, p.P_SIZE, count(distinct ps.PS_SUPPKEY) AS supplier_cnt
ORDER BY supplier_cnt DESC, p.P_BRAND, p.P_TYPE, p.P_SIZE;


// Q17
MATCH (l:LINEITEM)-[:COMPOSED_BY]->(p:PART)
WITH 0.2 * AVG(l.L_QUANTITY) AS L_QUANTITY
MATCH (l:LINEITEM)-[:COMPOSED_BY]->(p:PART)
WHERE p.P_BRAND = 'Brand#23' AND p.P_CONTAINER = 'MED BOX' AND l.L_QUANTITY < L_QUANTITY
RETURN SUM(l.L_EXTENDEDPRICE)/7.0 AS avg_yearly;


// Q18
MATCH (l:LINEITEM)
WITH l.L_ORDERKEY AS l_orderkey, SUM(l.L_QUANTITY) AS sum_lquantity
WHERE sum_lquantity > 300
MATCH (l:LINEITEM)-[:IS_PART_OF]->(o:ORDER)-[:MADE_BY]->(c:CUSTOMER)
WHERE o.id IN [l_orderkey]
RETURN c.C_NAME, c.id, o.id, o.O_ORDERDATE, o.O_TOTALPRICE, SUM(l.L_QUANTITY)
ORDER BY o.O_TOTALPRICE DESC, o.O_ORDERDATE
LIMIT 100;


// Q19
MATCH (l:LINEITEM)-[:COMPOSED_BY]->(p:PART)
WHERE (p.P_BRAND = 'Brand#12'
		AND p.P_CONTAINER in ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']
		AND l.L_QUANTITY >= 1 AND l.L_QUANTITY <= 1 + 10
		AND p.P_SIZE > 1 AND p.P_SIZE < 5
		AND l.L_SHIPMODE in ['AIR', 'AIR REG']
		AND l.L_SHIPINSTRUCT = 'DELIVER IN PERSON')
OR (p.P_BRAND = 'Brand#23'
		AND p.P_CONTAINER in ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']
		AND l.L_QUANTITY >= 10 AND l.L_QUANTITY <= 10 + 10
		AND p.P_SIZE > 1 AND p.P_SIZE < 10
		AND l.L_SHIPMODE in ['AIR', 'AIR REG']
		AND l.L_SHIPINSTRUCT = 'DELIVER IN PERSON')
OR (p.P_BRAND = 'Brand#34'
		AND p.P_CONTAINER in ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']
		AND l.L_QUANTITY >= 20 AND l.L_QUANTITY <= 20 + 10
		AND p.P_SIZE > 1 AND p.P_SIZE < 15
		AND l.L_SHIPMODE in ['AIR', 'AIR REG']
		AND l.L_SHIPINSTRUCT = 'DELIVER IN PERSON')
RETURN SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue;