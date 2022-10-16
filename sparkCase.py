from tempfile import tempdir
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.functions import regexp_replace,to_date,substring,col,month,dayofmonth,substring_index,row_number,last_day,year,count,last
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType,IntegerType,BooleanType,DateType

spark = SparkSession.builder.master("local[*]").appName("SparkCase").config("spark.some.config.option","some-value").getOrCreate()

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

df  = spark.read.format("csv").option("header","true").option("delimiter",";").load("spark-case-raw.csv")

tempDf2 = df.withColumn("account_no",regexp_replace("Account No","'",""))\
    .withColumn("valueDate",to_date(col("VALUE DATE"),"dd-MMM-yy"))\
    .withColumn("withdrawal_amt",substring("WITHDRAWAL AMT",3,12))\
    .withColumn("deposit_amt",substring("DEPOSIT AMT",3,12))\
    .withColumn("balance_amt",substring("BALANCE AMT",3,12))\
    .withColumn("TRX",col("TRANSACTION DETAILS"))\
    .withColumn("valueYear",year("valueDate"))\
    .withColumn("valueMonth",month("valueDate"))\
    .withColumn("valueDay",dayofmonth("valueDate"))\
    .withColumn("withdrawal_amt",regexp_replace("withdrawal_amt","[.]",""))\
    .withColumn("withdrawal_amt",regexp_replace("withdrawal_amt","[,]","."))\
    .withColumn("withdrawal_amt",col("withdrawal_amt").cast(DoubleType()))\
    .withColumn("deposit_amt", regexp_replace("deposit_amt", "[.]", ""))\
    .withColumn("deposit_amt", regexp_replace("deposit_amt", "[,]", "."))\
    .withColumn("deposit_amt", col("deposit_amt").cast(DoubleType()))\
    .withColumn("balance_amt", regexp_replace("balance_amt", "[.]", ""))\
    .withColumn("balance_amt", regexp_replace("balance_amt", "[,]", "."))\
    .withColumn("balance_amt", col("balance_amt").cast(DoubleType()))\
    .withColumn("TRX1",substring_index("TRX"," ",1))\
    .withColumn("TRX4",substring_index("TRX"," ",2))\
    .withColumn("TRX3",substring_index("TRX"," ",-2))\
    .withColumn("TRX2",substring_index("TRX"," ",-1))
    
#tempDf2.show(10)



#Data Based- Monthly - Daily - TRX,SUM,AVG
# table1 = tempDf2.groupBy("valueMonth").agg(count("withdrawal_amt"),count("deposit_amt")).orderBy(col("valueMonth").asc())
# table1.show()
#val table2 = tempDf2.groupBy("valueDay").agg(count("withdrawal_amt"), count("deposit_amt")).orderBy(col("valueDay").asc())
#table2.show()
#val table3= tempDf2.groupBy("valueMonth").sum("withdrawal_amt","deposit_amt").orderBy(col("valueMonth").asc())
#table3.show()
#val table4= tempDf2.groupBy("valueDay").sum("withdrawal_amt","deposit_amt").orderBy(col("ValueDay").asc())
#table4.show()
#val table5= tempDf2.groupBy("valueMonth").avg("withdrawal_amt","deposit_amt").orderBy(col("valueMonth").asc())
#table5.show()
#val table6 = tempDf2.groupBy("valueDay").avg("withdrawal_amt","deposit_amt").orderBy(col("valueDay").asc())
#table6.show()


#Account Based - Monthly - Daily TRX,SUM,AVG
#val table7 = tempDf2.groupBy("account_no").agg(count("withdrawal_amt"),count("deposit_amt")).orderBy($"account_no".asc())
#table7.show()

#val table8 = tempDf2.groupBy("account_no").sum("withdrawal_amt", "deposit_amt").orderBy(col("account_no").asc())
#table8.show()

#table9 = tempDf2.groupBy("account_no","valueMonth").agg(count("withdrawal_amt"), count("deposit_amt")).orderBy($"valueMonth".asc())
#table9.show(100)

#table10 = tempDf2.groupBy("account_no","valueMonth").sum("withdrawal_amt", "deposit_amt").orderBy(col("valueMonth").asc())
#table10.show()

#table11 = tempDf2.groupBy("account_no","valueMonth").avg("withdrawal_amt", "deposit_amt").orderBy(col("valueMonth").asc())
#table11.show()

#table12= tempDf2.select(col("account_no"),col("balance_amt"),col("deposit_amt"),col("valueMonth"),col("withdrawal_amt"),col("valueDate")).filter(col("valueMonth")===6 && col("valueYear") === 2018 && col("account_no")==="409000611074").orderBy($"valueDate".asc)
#table12.show(100)
# tempDf2.select("account_no","valueDate").filter((col("valueYear") == 2017) & ((col("account_no")=="409000611074")) ).show(10)



#How much money in balance at every account at the end of the month?
windowSpec= Window.partitionBy("account_no","valueMonth","valueYear",).orderBy("account_no")
balanceAtEndOfMonth =tempDf2.withColumn("rowNum",row_number().over(windowSpec))\
    .withColumn("last",last("rowNum").over(windowSpec))\
    .withColumn("lastDay",last_day("valueDate"))\
    .filter((col("rowNum")==col("last")))
mBalanceAtEndOfMonth = balanceAtEndOfMonth.select("account_no","valueDate","balance_amt","withdrawal_amt","deposit_amt").orderBy("valueDate","account_no").show()
# mBalanceAtEndOfMonth = balanceAtEndOfMonth.select("account_no","balance_amt").filter(tmpbalanceAtEndOfMonth.maxRow == col("rowNum")).show()
#filter(col("valueDate")==col("Last_Day") & col("account_no")=="409000611074" & col("valueMonth")==6)


#val table16 = table15.groupBy("account_no","balance_amt").agg(last(col("Last_Day")))
#table16.show(100)

#val table16=tempDf2.groupBy("account_no","valueMonth","balance_amt").agg(max("valueDate"))
#table16.show(1000)

spark.stop()