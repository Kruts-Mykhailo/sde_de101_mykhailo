from etl_factory import FactoryETL
from sqlite3 import Connection
import argparse
import sys
import duckdb

class DuckDBETL(FactoryETL):
    def extract(self, conn):
        conn.execute("INSTALL sqlite; LOAD sqlite;")
        conn.execute("CALL sqlite_attach('./tpch.db')")

    def transform(self, conn, partition_key, query):
        query.format(partition_key=partition_key)
        conn.execute(query)

    def load(self, conn):
        pass 
        
    def get_connection(self) -> Connection:
        return duckdb.connect()
    

# Query strategy:

def dim_parts_supplier():
    return """
    COPY (
        SELECT 
            p.p_partkey AS partkey, 
            p.p_name AS part_name, 
            p.p_mfgr AS part_manufacturer, 
            p.p_brand AS part_brand, 
            p.p_type AS part_type, 
            p.p_size AS part_size, 
            s.s_name AS supplier_name, 
            p.p_container AS part_container, 
            p.p_retailprice AS part_retailprice, 
            ps.ps_availqty AS supplier_parts_availqty, 
            ps.ps_supplycost AS supplier_supplycost, 
            s.s_address AS supplier_address, 
            s.s_phone AS supplier_phone, 
            s.s_acctbal AS supplier_acctbal, 
            s.s_comment AS supplier_comment,
            n.n_name AS nation_name, 
            r.r_name AS region_name, 
        FROM partsupp ps
        JOIN part p ON ps.ps_partkey = p.p_partkey
        JOIN supplier s ON ps.ps_suppkey = s.s_suppkey
        JOIN nation n ON s.s_nationkey = n.n_nationkey
        JOIN region r ON n.n_regionkey = r.r_regionkey
    ) 
    TO './processed_data/dim_parts_supplier/'{partition_key}'.csv' (FORMAT csv, HEADER, DELIMITER ',')
    """

def one_big_table():
    return """
    COPY (
         SELECT 
            l.*,
            o.o_orderstatus,
            o.o_totalprice,
            o.o_orderdate,
            o.o_orderpriority,
            o.o_clerk,
            o.o_shippriority,
            o.o_comment AS order_comment,
            c.c_name AS customer_name,
            c.c_address AS customer_address,
            c.c_phone AS customer_phone,
            c.c_acctbal AS customer_acctbal,
            c.c_mktsegment AS customer_mktsegment,
            c.c_comment AS customer_comment,
            n_cust.n_name AS customer_nation_name,
            n_cust.n_regionkey AS customer_nation_regionkey,
            n_cust.n_comment AS customer_nation_comment,
            r_cust.r_name AS customer_region_name,
            r_cust.r_comment AS customer_region_comment,
            p.p_name AS part_name,
            p.p_mfgr AS part_mfgr,
            p.p_brand AS part_brand,
            p.p_type AS part_type,
            p.p_size AS part_size,
            p.p_container AS part_container,
            p.p_retailprice AS part_retailprice,
            p.p_comment AS part_comment,
            s.s_name AS supplier_name,
            s.s_address AS supplier_address,
            s.s_phone AS supplier_phone,
            s.s_acctbal AS supplier_acctbal,
            s.s_comment AS supplier_comment,
            n_supp.n_name AS supplier_nation_name,
            n_supp.n_regionkey AS supplier_nation_regionkey,
            n_supp.n_comment AS supplier_nation_comment,
            r_supp.r_name AS supplier_region_name,
            r_supp.r_comment AS supplier_region_comment,
            ps.ps_availqty,
            ps.ps_supplycost,
            ps.ps_comment AS partsupp_comment
        FROM 
            lineitem l
        LEFT JOIN 
            orders o ON l.l_orderkey = o.o_orderkey
        LEFT JOIN 
            customer c ON o.o_custkey = c.c_custkey
        LEFT JOIN 
            nation n_cust ON c.c_nationkey = n_cust.n_nationkey
        LEFT JOIN 
            region r_cust ON n_cust.n_regionkey = r_cust.r_regionkey
        LEFT JOIN 
            part p ON l.l_partkey = p.p_partkey
        LEFT JOIN 
            supplier s ON l.l_suppkey = s.s_suppkey
        LEFT JOIN 
            nation n_supp ON s.s_nationkey = n_supp.n_nationkey
        LEFT JOIN 
            region r_supp ON n_supp.n_regionkey = r_supp.r_regionkey
        LEFT JOIN 
            partsupp ps ON l.l_partkey = ps.ps_partkey AND l.l_suppkey = ps.ps_suppkey) TO './processed_data/wide_lineitem/'{partition_key}'.csv' (FORMAT csv, HEADER, DELIMITER ',')
    """


def wide_month_supplier_metrics():
    return """
    COPY (
         WITH wide_lineitem as (SELECT 
            l.*,
            o.o_orderkey,
            o.o_orderstatus,
            o.o_totalprice,
            o.o_orderdate,
            o.o_orderpriority,
            o.o_clerk,
            o.o_shippriority,
            o.o_comment AS order_comment,
            c.c_name AS customer_name,
            c.c_address AS customer_address,
            c.c_phone AS customer_phone,
            c.c_acctbal AS customer_acctbal,
            c.c_mktsegment AS customer_mktsegment,
            c.c_comment AS customer_comment,
            n_cust.n_name AS customer_nation_name,
            n_cust.n_regionkey AS customer_nation_regionkey,
            n_cust.n_comment AS customer_nation_comment,
            r_cust.r_name AS customer_region_name,
            r_cust.r_comment AS customer_region_comment,
            p.p_name AS part_name,
            p.p_mfgr AS part_mfgr,
            p.p_brand AS part_brand,
            p.p_type AS part_type,
            p.p_size AS part_size,
            p.p_container AS part_container,
            p.p_retailprice AS part_retailprice,
            p.p_comment AS part_comment,
            s.s_name AS supplier_name,
            s.s_address AS supplier_address,
            s.s_phone AS supplier_phone,
            s.s_acctbal AS supplier_acctbal,
            s.s_comment AS supplier_comment,
            n_supp.n_name AS supplier_nation_name,
            n_supp.n_regionkey AS supplier_nation_regionkey,
            n_supp.n_comment AS supplier_nation_comment,
            r_supp.r_name AS supplier_region_name,
            r_supp.r_comment AS supplier_region_comment,
            ps.ps_availqty,
            ps.ps_supplycost,
            ps.ps_comment AS partsupp_comment
        FROM 
            lineitem l
        LEFT JOIN 
            orders o ON l.l_orderkey = o.o_orderkey
        LEFT JOIN 
            customer c ON o.o_custkey = c.c_custkey
        LEFT JOIN 
            nation n_cust ON c.c_nationkey = n_cust.n_nationkey
        LEFT JOIN 
            region r_cust ON n_cust.n_regionkey = r_cust.r_regionkey
        LEFT JOIN 
            part p ON l.l_partkey = p.p_partkey
        LEFT JOIN 
            supplier s ON l.l_suppkey = s.s_suppkey
        LEFT JOIN 
            nation n_supp ON s.s_nationkey = n_supp.n_nationkey
        LEFT JOIN 
            region r_supp ON n_supp.n_regionkey = r_supp.r_regionkey
        LEFT JOIN 
            partsupp ps ON l.l_partkey = ps.ps_partkey AND l.l_suppkey = ps.ps_suppkey),
        base AS (
        SELECT
            month(CAST(o_orderdate as date)) AS month,
            part_name,
            COUNT(*) AS order_count,
            SUM(l_quantity) AS total_quantity,
            SUM(l_extendedprice) AS total_extendedprice,
            SUM(l_discount) AS total_discount,
            SUM(l_tax) AS total_tax,
            AVG(l_quantity) AS avg_quantity,
            AVG(l_extendedprice) AS avg_extendedprice,
            AVG(l_discount) AS avg_discount,
            AVG(l_tax) AS avg_tax,
            MIN(l_quantity) AS min_quantity,
            MIN(l_extendedprice) AS min_extendedprice,
            MIN(l_discount) AS min_discount,
            MIN(l_tax) AS min_tax,
            MAX(l_quantity) AS max_quantity,
            MAX(l_extendedprice) AS max_extendedprice,
            MAX(l_discount) AS max_discount,
            MAX(l_tax) AS max_tax,
            COUNT(DISTINCT o_orderkey) AS unique_orders,
            COUNT(DISTINCT customer_name) AS unique_customers,
            COUNT(DISTINCT supplier_name) AS unique_suppliers,
            SUM(ps_availqty) AS total_available_quantity,
            SUM(ps_supplycost) AS total_supply_cost
        FROM
            wide_lineitem
        GROUP BY
            month(CAST(o_orderdate as date)),
            part_name
            )
            SELECT
                *,
                AVG(order_count) OVER (PARTITION BY part_name ORDER BY month ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_order_count_last_2_months,
                AVG(total_quantity) OVER (PARTITION BY part_name ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS avg_quantity_last_6_months,
                SUM(total_extendedprice) OVER (PARTITION BY part_name ORDER BY month ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING) AS total_extendedprice_last_4_months,
                MAX(total_discount) OVER (PARTITION BY part_name ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND 3 PRECEDING) AS max_discount_up_to_last_4_months,
                MIN(total_tax) OVER (PARTITION BY part_name ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS min_tax_next_3_months
            FROM
                base
            ORDER BY
                month,
                part_name
    ) TO './processed_data/wide_month_supplier_metrics/'{partition_key}'.csv' (FORMAT csv, HEADER, DELIMITER ',', per_thread_output)
    """

def query_factory(value):
    factory = {
        'dim_parts_supplier': dim_parts_supplier,
        'one_big_table': one_big_table,
        'wide_month_supplier_metrics': wide_month_supplier_metrics
    }
    if value not in factory:
        print(f"Error: '{value}' is not a valid transformation. Available transformations: {list(factory.keys())}")
        sys.exit(1)  # Exit the program with a non-zero status to indicate an error
    
    return factory[value]


if __name__ == "__main__":
    # Argument parser for timestamp input
    parser = argparse.ArgumentParser(description="Create table from duckDB")
    parser.add_argument("timestamp", type=str, help="Timestamp for the folder name")
    parser.add_argument("transformation", type=str, help="Transformation query for duckdb")
    args = parser.parse_args()

    folder_name = args.timestamp
    transformation = args.transformation
    
    etl = DuckDBETL()
    etl.run_pipeline(
        partition_key=folder_name,
        query=query_factory(transformation)
    )
