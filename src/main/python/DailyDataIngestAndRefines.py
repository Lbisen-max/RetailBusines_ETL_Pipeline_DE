from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,FloatType,TimestampType
from pyspark.sql import functions as psf
from datetime import datetime,timedelta,date, time
import configparser
from src.main.python.schemaFunction import read_schema

# creating Spark Session
spark= SparkSession.builder.appName("DatIngestAndRefine").master("local").getOrCreate()

# Defining the config parser to read schema from config.ini file
config = configparser.ConfigParser()
config.read(r'../ProjectConfig/config.ini')

# reading input paths/data and schema
inputLocation = config.get('paths', 'inputLocation')
outputLocation = config.get('paths', 'outputLocation')
landingFileSchemaConfig = config.get('schema', 'landingFileSchema')
holdFileSchemaConfig = config.get('schema', 'holdFileSchema')


# reading schemas
landingFileSchema = read_schema(landingFileSchemaConfig)
holdFileSchema = read_schema(holdFileSchemaConfig)


# Defining current_date , previous date landing zone
datetoday = datetime.now()
yesterdaydate = datetoday - timedelta(1)

# currDayZoneSuffix = "_" + datetoday.strftime("%d%m%Y")
# prevDayZoneSuffix = "_" + yesterdaydate.strftime("%d%m%Y")

currDayZoneSuffix = '_02102023' #'_01102023'
prevDayZoneSuffix = '_01102023' #_30092023'
print(currDayZoneSuffix)
print(prevDayZoneSuffix)

# reading data from landing file zone
landingFileDF = spark.read\
    .schema(landingFileSchema)\
    .option("delimiter", "|")\
    .csv(inputLocation+"Sales_landing\\SalesDump"+currDayZoneSuffix)

# landingFileDF.show()

landingFileDF.createOrReplaceTempView("landingFileDF")

# reading previous day data
previousHoldDF = spark.read\
    .schema(holdFileSchema)\
    .option("delimiter", "|")\
    .option("header", True)\
    .csv(outputLocation+"Hold\\HoldData"+prevDayZoneSuffix)

previousHoldDF.createOrReplaceTempView("previousHoldDF")
# joining the landingFileDF of current date and previousHoldDF data to check if any new records
# received for missing values which we have hold

refreshedLandingData = spark.sql("SELECT a.Sale_ID , a.Product_ID, "
                                 "CASE "
                                 "WHEN (a.Quantity_Sold IS NULL ) THEN b.Quantity_Sold "
                                 "ELSE a.Quantity_Sold "
                                 "END AS Quantity_Sold, "
                                 "CASE "
                                 "WHEN (a.Vendor_ID IS NULL) THEN b.Vendor_ID "
                                 "ELSE a.Vendor_ID "
                                 "END AS Vendor_ID, "
                                 "a.Sale_Date, a.Sale_Amount,a.Sale_Currency "
                                 "FROM landingFileDF a LEFT OUTER JOIN "
                                 "previousHoldDF b ON a.Sale_ID = b.Sale_ID ")

refreshedLandingData.show()

refreshedLandingData.createOrReplaceTempView("refreshedLandingData")

# Here dividing landing data into valid data and invalid data based on qty_sold / vendor_id missing

# invalidLandingDF = refreshedLandingData.filter(psf.col("Quantity_Sold").isNull() | psf.col("Vendor_ID").isNull())\
#     .withColumn("Hold_Reason",psf.when(psf.col("Quantity_Sold").isNull(), "Qty Sold Missing")
#                 .otherwise(psf.when(psf.col("Vendor_ID").isNull(), "Vendor ID Missing")))

validLandingDF = refreshedLandingData.filter(psf.col("Quantity_Sold").isNotNull() & psf.col("Vendor_ID").isNotNull())

validLandingDF.createOrReplaceTempView("validLandingDF")
# printing the valid and invalid data
# invalidLandingDF.show()
# validLandingDF.show()

# creating ReleasDF so that such data which received an update will be released from hold

releasedFromHoldDF = spark.sql("SELECT vd.Sale_ID FROM validLandingDF vd "
                               "INNER JOIN previousHoldDF phd "
                               "ON vd.Sale_ID = phd.Sale_ID ")

releasedFromHoldDF.createOrReplaceTempView("releasedFromHoldDF")

notReleasedFromHoldDF = spark.sql("SELECT * FROM previousHoldDF "
                                  "WHERE Sale_ID NOT IN (SELECT Sale_ID FROM releasedFromHoldDF)")

notReleasedFromHoldDF.createOrReplaceTempView("notReleasedFromHoldDF")


# Now we have to concatinate the notReleasedFromHoldDF with invalidLandingDF (hold data)

invalidLandingDF = refreshedLandingData.filter(psf.col("Quantity_Sold").isNull() | psf.col("Vendor_ID").isNull())\
    .withColumn("Hold_Reason", psf.when(psf.col("Quantity_Sold").isNull(), "Qty Sold Missing")
                .otherwise(psf.when(psf.col("Vendor_ID").isNull(), "Vendor ID Missing")))\
                .union(notReleasedFromHoldDF)

invalidLandingDF.show()

# writing/saving valid/invalid into valid/invalid folder
invalidLandingDF.write\
    .mode("overwrite")\
    .option("delimiter", "|")\
    .option("header",True)\
    .csv(outputLocation+"Hold\\HoldData"+currDayZoneSuffix)

validLandingDF.write\
    .mode("overwrite")\
    .option("delimiter", "|")\
    .option("header",True)\
    .csv(outputLocation+"Valid\\ValidData"+currDayZoneSuffix)





