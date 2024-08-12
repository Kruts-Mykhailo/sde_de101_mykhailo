print(
    "################################################################################"
)
print("Use standard python libraries to do the transformations")
print(
    "################################################################################"
)
import csv
data = []
# Question: How do you read data from a CSV file at ./data/sample_data.csv into a list of dictionaries?

with open('./data/sample_data.csv', mode ='r') as file:    
       csvFile = csv.DictReader(file)
       for row in csvFile:
              data.append(row)
# Question: How do you remove duplicate rows based on customer ID?
result = []
customer_ids = []
for row in data:
    if row.get('Customer_ID') not in customer_ids:
        result.append(row)
        customer_ids.append(row.get('Customer_ID'))

print(result)
# Question: How do you handle missing values by replacing them with 0?
data = result
for row in data:
    if not row["Age"]:
        row["Age"] = 0
    if not row["Purchase_Amount"]:
        row["Purchase_Amount"] = 0.0
# Question: How do you remove outliers such as age > 100 or purchase amount > 1000?
data = [row for row in data if row["Age"] > 100 or row["Purchase_Amount"] > 1000]
# Question: How do you convert the Gender column to a binary format (0 for Female, 1 for Male)?
for row in data:
    row["Gender"] = 0 if 'Female' else 1
# Question: How do you split the Customer_Name column into separate First_Name and Last_Name columns?
for row in data:
    row["First_Name"] = row["Customer_Name"].split(" ")[0]
    row["Last_Name"] = row["Last_Name"].split(" ")[1]
# Question: How do you calculate the total purchase amount by Gender?

print('Male purchase amount')
print(sum([row['Purchase_Amount'] for row in data if row["Gender"] == 'Male']))
print('Female purchase amount')
print(sum([row['Purchase_Amount'] for row in data if row["Gender"] == 'Female']))
# Question: How do you calculate the average purchase amount by Age group?
# assume age_groups is the grouping we want
# hint: Why do we convert to float?
import statistics


def condition_mean(entries: list[float], condition: function):
     return statistics.mean([e for e in entries if condition])

lst = [float(row["Purchase_Amount"]) for row in data]

age_groups = {"18-30": condition_mean(lst, lambda x: 18 <= x <= 30), 
              "31-40": condition_mean(lst, lambda x: 31 <= x <= 40),
              "41-50": condition_mean(lst, lambda x: 41 <= x <= 50), 
              "51-60": condition_mean(lst, lambda x: 51 <= x <= 60), 
              "61-70": condition_mean(lst, lambda x: 61 <= x <= 70)}

# Question: How do you print the results for total purchase amount by Gender and average purchase amount by Age group?
your_total_purchase_amount_by_gender = {} # your results should be assigned to this variable
average_purchase_by_age_group = {} # your results should be assigned to this variable

your_total_purchase_amount_by_gender = {
     'Male': sum([row['Purchase_Amount'] for row in data if row["Gender"] == 'Male']),
     'Female': sum([row['Purchase_Amount'] for row in data if row["Gender"] == 'Female'])
}
average_purchase_by_age_group = age_groups

print(f"Total purchase amount by Gender: {your_total_purchase_amount_by_gender}")
print(f"Average purchase amount by Age group: {average_purchase_by_age_group}")

print(
    "################################################################################"
)
print("Use DuckDB to do the transformations")
print(
    "################################################################################"
)

# Question: How do you connect to DuckDB and load data from a CSV file into a DuckDB table?
# Connect to DuckDB and load data
import duckdb

conn = duckdb.connect(database=":memory:", read_only=False)
conn.execute(
    "CREATE TABLE data (Customer_ID INTEGER, Customer_Name VARCHAR, Age INTEGER, Gender VARCHAR, Purchase_Amount FLOAT, Purchase_Date DATE)"
)

# Read data from CSV file into DuckDB table
conn.execute("COPY data FROM './data/sample_data.csv' WITH HEADER CSV")

# Question: How do you remove duplicate rows based on customer ID in DuckDB?
conn.execute("CREATE TABLE data_unique AS SELECT DISTINCT * FROM data")


# Question: How do you handle missing values by replacing them with 0 in DuckDB?
conn.execute("""
    CREATE TABLE data_remove_missing AS \
             SELECT Customer_ID,
             Customer_Name,
             COALESCE(Age, 0),
             Gender,
             COALESCE(Purchase_Amount, 0),
             Purchase_Date
             FROM data_unique
""")
# Question: How do you remove outliers (e.g., age > 100 or purchase amount > 1000) in DuckDB?
conn.execute("""
    CREATE TABLE data_remove_outliers AS \
             SELECT *
             FROM data_remove_missing
             WHERE Age <= 100 AND Purchase_Amount <= 1000
""")
# Question: How do you convert the Gender column to a binary format (0 for Female, 1 for Male) in DuckDB?
conn.execute("""
    CREATE TABLE data_gender_binary AS \
             SELECT *
             CASE
             WHEN Gender = 'Female'
             THEN 0
             ELSE 1
             END as Gender_Binary
             FROM data_remove_outliers
             
""")

# Question: How do you split the Customer_Name column into separate First_Name and Last_Name columns in DuckDB?

conn.execute("""
    CREATE TABLE data_split_name AS \
             SELECT 
             SPLIT_PART(Customer_Name, ' ', 1) as First_Name,
             SPLIT_PART(Customer_Name, ' ', 2) as Last_Name.
             Age,
             Gender,
             Purchase_Amount,
             Purchase_Date
             FROM data_gender_binary
             
""")
# Question: How do you calculate the total purchase amount by Gender in DuckDB?
conn.execute("""
    CREATE TABLE total_purch_by_gender AS 
             SELECT 
             Gender,
             SUM(Purchase_Amount)
             FROM data_gender_binary
             GROUP BY Gender

             
""")
# Question: How do you calculate the average purchase amount by Age group in DuckDB?

conn.execute("""
    CREATE TABLE avg_purch_by_age_group AS 
             SELECT 
             CASE 
             WHEN Age >= 18 AND Age <=30 THEN '18-30'
             WHEN Age >= 31 AND Age <=40 THEN '31-40'
             WHEN Age >= 41 AND Age <=50 THEN '41-50'
             WHEN Age >= 51 AND Age <=60 THEN '51-60'
             WHEN Age >= 61 AND Age <=70 THEN '61-70'
             ELSE 'Other'
             END as Age_Group
             AVG(Purchase_Amount)
             FROM data_gender_binary
             GROUP BY Age_Group

             
""")
# Question: How do you print the results for total purchase amount by Gender and average purchase amount by Age group in DuckDB?
print("====================== Results ======================")
print("Total purchase amount by Gender:")
print("Average purchase amount by Age group:")
