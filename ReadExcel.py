from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pymysql.cursors
from model.Student import Student


spark = SparkSession.builder.appName('ExcelToMySQL').getOrCreate()

excel_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("username", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("email", StringType(), True)
])

excel_df = spark.read.format('com.crealytics.spark.excel') \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .load('sparktest.xlsx')

excel_df.printSchema()
student_rdd = excel_df.rdd.map(lambda row: Student(row[0], row[1], row[2], row[3]))


table_name = "students"
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='1111',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

cursor = connection.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS excel_db")
cursor.execute(f"USE excel_db")

# fields = [f"{column.name} VARCHAR(255)" for column in excel_df.schema[1:]]
fields = [f"username VARCHAR(255)", f"age INT", f"email VARCHAR(255)"]
fields.insert(0, "id INT PRIMARY KEY")

print(fields)

mysql_schema = ', '.join(fields)
cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({mysql_schema})")

for student in student_rdd.collect():
    sql_query = f"INSERT INTO {table_name} (id,username,age,email) VALUES ({student.id}, '{student.username}'," \
                f" {student.age}, '{student.email}')"
    cursor.execute(sql_query)
connection.commit()

excel_df.write\
    .option("driver", "com.mysql.cj.jdbc.Driver")\
    .jdbc("jdbc:mysql://localhost:3306/excel_db", table_name, \
          properties={"user": "root", "password": "1111"}, \
          mode="overwrite")

connection.close()
spark.stop()

