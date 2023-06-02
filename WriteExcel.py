from pyspark.sql import SparkSession
from pyspark.sql import Row
import pymysql
spark = SparkSession.builder.appName('MySQLtoExcel').getOrCreate()

table_name = "students"

try:
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='1111',
        db='excel_db',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    print("Connected to MySQL successfully!")
except Exception as e:
    print("Unable to connect to MySQL: ", str(e))

cursor = connection.cursor()

cursor.execute(f"SELECT * FROM {table_name}")

result = cursor.fetchall()

excel_path = "excel_files/students_excel.xlsx"

rdd = spark.sparkContext.parallelize([Row(**x) for x in result])

df = spark.createDataFrame(rdd)

df = df.toPandas()

df.to_excel(excel_path, index=False)

connection.close()

spark.stop()


