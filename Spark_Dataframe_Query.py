import json
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
file = spark.read.csv('/Users/nishant/Desktop/Python_Spark_Assignment/csv/*.csv', sep=',',
                    inferSchema=True, header=True)
df= file.toPandas()
df.rename(columns={'Adj Close':'Adj_Close'}, inplace= True)
df = spark.createDataFrame(df)
df.createOrReplaceTempView("temptable")
# sqldf= spark.sql("select * from temptable where Stock_Name = 'AAN' ")
# sqldf.show()

class Query:
    
    def task_1(self):
        spark.sql("create temporary view temp1 as (select Date,max(((High-Open)/Open)*100) as Positive "
                  "from temptable group by Date)")
        spark.sql("create temporary view temp2 as (select Date,min(((Low-Open)/Open)*100) as Negative "
                  "from temptable group by Date)"),
        #spark.sql("select * from temp2").show()
        spark.sql("create temporary view temp3 as (select Date,Stock_Name as Max_Stock_Name,((High-Open)/Open)*100 as Positive "
                  "from temptable where ((High-Open)/Open)*100 in ( select Positive from temp1 "
                  "where temptable.Date = temp1.Date)) ")
        spark.sql("create temporary view temp4 as (select Date,Stock_Name as Min_Stock_Name,((Low-Open)/Open)*100 as Negative "
                  "from temptable where ((Low-Open)/Open)*100 in ( select Negative from temp2 "
                  "where temptable.Date = temp2.Date)) ")
        sqldf=spark.sql("select temp3.Date,Max_Stock_Name,Positive,Min_Stock_Name,Negative from temp3 inner join temp4 on temp3.Date=temp4.Date")
        sqldf.show()
        return json.loads(sqldf.toPandas().to_json(orient="table", index=False))

    def task_2(self):
        sqldf=spark.sql("select Date,Stock_Name,Volume from temptable as s where Volume = ( select Max(Volume) from "
                        "temptable where temptable.Date=s.Date)")
        sqldf.show()
        return json.loads(sqldf.toPandas().to_json(orient="table", index=False))

    def task_3(self):

        spark.sql("create temporary view temp3_1 as (select Stock_Name,Open,Date,lag(Close,1,0) over "
                        "( partition by Stock_Name order by Date ASC ) as Adj_Close from temptable)")
        spark.sql("create temporary view temp3_2 as (select Max(Open-Adj_Close) as Max_Gap, "
                  "Min(Open-Adj_Close) as Min_Gap from temp3_1)")
        sqldf= spark.sql("select Stock_Name, (Open-Adj_Close) as Max_Min_Gap from temp3_1 where exists "
                  "( select * from temp3_2 where (temp3_1.Open -temp3_1.Adj_Close) = temp3_2.Max_Gap"
                         " or (temp3_1.Open -temp3_1.Adj_Close) = temp3_2.Min_Gap)")

        sqldf.show()
        return json.loads(sqldf.toPandas().to_json(orient="table", index=False))

    def task_4(self):
        spark.sql("create temporary view temp4_1 as (select Stock_Name, min(Date) as Min_Date from temptable "
                  "group by Stock_Name)")

        spark.sql("create temporary view temp4_2 as (select Stock_Name, max(High) as High from temptable "
                  "group by Stock_Name )")
        spark.sql("create temporary view temp4_3 as ( select Stock_Name, Open from temptable "
                  "where exists ( select * from temp4_1 where temp4_1.Stock_Name = temptable.Stock_name and "
                  " temp4_1.Min_Date = temptable.Date ))")
        spark.sql("select * from temp4_3").show()
        spark.sql("create temporary view temp4_4 as ( select max(temp4_2.High - temp4_3.Open) from  "
                  "temp4_2 inner join temp4_3 "
                  "on temp4_2.Stock_Name = temp4_3.Stock_Name )")
        sqldf= spark.sql("select Temp4_2.Stock_Name,  (temp4_2.High - temp4_3.Open) as Max_Move from "
                         " temp4_2 inner join temp4_3 "
                         " on temp4_2.Stock_Name = temp4_3.Stock_Name "
                         " where (temp4_2.High - temp4_3.Open) in ( select * from temp4_4)")
        sqldf.show()
        return json.loads(sqldf.toPandas().to_json(orient="table", index=False))

    def task_5(self):
        sqldf = spark.sql("select Stock_Name , std(Open) from temptable group by Stock_Name")
        sqldf.show()
        return json.loads(sqldf.toPandas().to_json(orient="table", index=False))
    def task_6(self):
        spark.sql("create temporary view temp6_1 as (select Stock_Name , avg(Open) as mean_ from temptable "
                         "group by Stock_Name)")
        spark.sql("create temporary view temp6_2 as (select Stock_Name , percentile_approx(Open,0.5) as median "
                  "from temptable group by Stock_Name)")
        sqldf= spark.sql("select temp6_1.Stock_Name,mean_, median from temp6_1 inner join temp6_2 "
                         "on temp6_1.Stock_Name = Temp6_2.Stock_Name")
        sqldf.show()
        return json.loads(sqldf.toPandas().to_json(orient="table", index=False))

    def task_7(self):
        sqldf = spark.sql("select Stock_Name,avg(Volume) as Avg_Value from temptable group by Stock_Name")
        sqldf.show()
        return json.loads(sqldf.toPandas().to_json(orient="table",index=False))
    def task_8(self):
        spark.sql("create temporary view temp8_1 as (select Stock_Name, avg(Volume) as Avg_Volume from temptable "
                  "group by Stock_Name)")
        sqldf = spark.sql("select * from (temp8_1) where Avg_Volume = (select max(Avg_Volume) from temp8_1)")
        sqldf.show()
        return json.loads(sqldf.toPandas().to_json(orient="table", index=False))

    def task_9(slef):
        sqldf = spark.sql("select Stock_Name,Max(High) as Max_Value, Min(Low) as Min_Value "
                          "from temptable group by Stock_Name")
        sqldf.show()
        return json.loads(sqldf.toPandas().to_json(orient="table", index=False))


# obj = Query()
# obj.task_3()
