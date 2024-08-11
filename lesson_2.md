## Assignment

### Question 1:

```
SELECT 
    l_returnflag,
    l_linestatus,
    sum(l_extendedprice) as sum_extendedprice,
    sum(l_quantity) as sum_quantity,
    sum(l_extendedprice * (1 - l_discount)) as sum_discountedprice,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_discounted_with_tax,
    avg(l_quantity) as avg_quantity,
    avg(l_extendedprice) as avg_extprice,
    avg(l_discount) as avg_discount,
    count(*) as count_order
FROM lineitem
WHERE l_shipdate <= date('1998-12-01', '-90 day')
GROUP BY 
    l_returnflag,
    l_linestatus
ORDER BY l_returnflag, l_linestatus
```

### Question 2:
```
SELECT
    s.s_acctbal as acc_balance, 
    s.s_name,
    n.n_name as nation, 
    p.p_partkey,
    p.p_mfgr,
    s.s_address,
    s.s_phone,
    s.s_comment
    
FROM part as p
JOIN partsupp as ps ON p.p_partkey = ps.ps_partkey
JOIN supplier as s ON s.s_suppkey = ps.ps_suppkey
JOIN nation as n ON n.n_nationkey = s.s_nationkey
JOIN region as r ON r.r_regionkey = n.n_regionkey
WHERE --p.p_size = 15
    --and p.p_type LIKE '%BRASS'
    --and r.r_name = 'EUROPE'
    --and 
    ps.ps_supplycost = (
        SELECT
            MIN(pss.ps_supplycost)
        FROM partsupp as pss
        JOIN supplier as ss ON ss.s_suppkey = pss.ps_suppkey
        JOIN nation as ns ON ns.n_nationkey = ss.s_nationkey
        JOIN region as rs ON rs.r_regionkey = ns.n_regionkey
        WHERE rs.r_name = 'EUROPE'
    )
ORDER BY s.s_acctbal DESC, n.n_name, s.s_name, p.p_partkey
```

### Question 3:

```
SELECT 
    o_shippriority,
    sum(l_extendedPrice * (1 - l_discount)) as revenue,
    l_orderkey,
    o_orderdate
FROM lineitem as l 
JOIN orders as o ON o.o_orderkey = l.l_orderkey
JOIN customer as c ON c.c_custkey = o.o_custkey
WHERE 
    c_mktsegment = 'BUILDING'
    and o_orderdate < date('1995-03-15')
    and l_shipdate > date('1995-03-15')
ORDER BY revenue DESC, o_orderdate
```
### Question 4:

```
SELECT 
    count(*) as count_orders,
    o_orderpriority
FROM orders 
WHERE o_orderdate >= date('1993-07-01')
    and o_orderdate < date('1993-07-01', '+3 month')
    and exists (
        SELECT * 
        FROM lineitem
        where l_shipdate > l_commitdate
        and l_orderkey = o_orderkey
    )
    
GROUP BY o_orderpriority
ORDER BY o_orderpriority
```
### Question 5:

```


SELECT 
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM region as r
JOIN nation as n on r.r_regionkey = n.n_regionkey
JOIN supplier as s on s.s_nationkey = n.n_nationkey
JOIN customer as c on c.c_nationkey = n.n_nationkey
JOIN orders as o on o.o_custkey = c.c_custkey
JOIN lineitem as l on l.l_orderkey = o.o_orderkey
WHERE o_orderdate >= date('1994-01-01')
    and o_orderdate < date('1994-01-01', '+1 year')
    and r_name = 'ASIA'
GROUP BY n_name
ORDER BY revenue DESC, n_name

```

### Question 6:

```
SELECT 
    sum(l_extendedprice * l_discount) as revenue_increase
FROM lineitem as l
WHERE l_shipdate >= date('1994-01-01')
    and l_shipdate < date('1994-01-01', '+1 year')
    and l_discount between 0.06 - 0.01 and 0.06 + 0.01
    and l_quantity < 24
```

### Question 7:

```
SELECT 
s_nation,
c_nation,
l_year,
sum(volume) as revenue
FROM (
    SELECT 
        n1.n_name as s_nation,
        n2.n_name as c_nation,
        strftime('%Y', l_shipdate) as l_year,
        l_extendedprice * (1 - l_discount) as volume
    FROM lineitem as l 
    JOIN orders as o on o.o_orderkey = l.l_orderkey
    JOIN customer as c on c.c_custkey = o.o_custkey
    JOIN supplier as s on s.s_suppkey = l.l_suppkey
    JOIN nation as n1 on n1.n_nationkey = s.s_nationkey
    JOIN nation as n2 on n2.n_nationkey = c.c_nationkey
    WHERE l_shipdate between date('1995-01-01') and date('1996-12-31')
    and ((n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n2.n_name = 'FRANCE' and n1.n_name = 'GERMANY'))
)
GROUP BY s_nation, c_nation, l_year
ORDER BY s_nation, c_nation, l_year
```
### Question 8:

```

SELECT 
    sum(CASE 
            WHEN nation = 'BRAZIL'
            THEN volume
            else 0
        end
    ) / sum(volume) as mkt_share,
    o_year
FROM (
    SELECT 
    l_extendedprice * (1 - l_discount) as volume,
    n2.n_name as nation,
    strftime('%Y', o_orderdate) as o_year
    FROM lineitem as l 
    JOIN part as p on p.p_partkey = l.l_partkey
    JOIN supplier as s on s.s_suppkey = l.l_suppkey
    JOIN orders as o on l.l_orderkey = o.o_orderkey
    JOIN customer as c on c.c_custkey = o.o_custkey
    JOIN nation as n1 on n1.n_nationkey = c.c_nationkey
    JOIN region as r1 on r1.r_regionkey = n1.n_regionkey
    JOIN nation as n2 on n2.n_nationkey = s.s_nationkey

    WHERE r_name = 'AMERICA'
    and p_type = 'ECONOMY ANODIZED STEEEL'
    and o_orderdate between date('1995-01-01') and date('1996-12-31')
)
GROUP BY 
    o_year
ORDER BY 
    o_year
```

### Question 9:

```
SELECT 
    SUM(profit) as sum_profit,
    o_year,
    nation
FROM (
    SELECT 
        (l_extendedprice * (1 - l_discount)) - (ps_supplycost * l_quantity) as profit,
        strftime('%Y', o_orderdate) as o_year,
        n1.n_name as nation
    FROM lineitem as l 
    JOIN orders as o on o.o_orderkey = l.l_orderkey
    JOIN partsupp as ps on l.l_partkey = ps.ps_partkey and l.l_suppkey = ps.ps_suppkey
    JOIN part as p on p.p_partkey = ps.ps_partkey
    JOIN customer as c on c.c_custkey = o.o_custkey
    JOIN nation as n1 on n1.n_nationkey = c.c_nationkey
    JOIN supplier as s on s.s_suppkey = l.l_suppkey 
    JOIN nation as n2 on n2.n_nationkey = s.s_nationkey
    WHERE p_name LIKE '%green%'
    and n1.n_name = n2.n_name 
)
GROUP BY o_year, nation
ORDER BY nation, o_year DESC
```

### Question 10:
```

SELECT
    c_name,
    c_address,
    n_name,
    c_phone,
    c_acctbal,
    c_comment,
    sum(l_extendedprice * (1 - l_discount)) as lost_revenue
FROM customer as c 
JOIN nation as n on n.n_nationkey = c.c_nationkey
JOIN orders as o on o.o_custkey = c.c_custkey
JOIN lineitem as l on l.l_orderkey = o.o_orderkey

WHERE l_returnflag = 'R' 
    and o_orderdate > date('1993-10-01') and o_orderdate <= date('1993-10-01', '+3 months')

GROUP BY 
    c_name,
    c_address,
    n_name,
    c_phone,
    c_acctbal,
    c_comment

ORDER BY lost_revenue desc
LIMIT 20
```

### Question 11:
### Question 12:
### Question 13:
### Question 14:
### Question 15:
### Question 16:
### Question 17:
