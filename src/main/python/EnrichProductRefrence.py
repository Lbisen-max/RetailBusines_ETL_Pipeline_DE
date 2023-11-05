from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,FloatType,TimestampType
from pyspark.sql import functions as psf
from datetime import datetime,timedelta,date, time
import configparser
from src.main.python.schemaFunction import read_schema

spark = SparkSession.builder.appName("DatIngestAndRefine").master("local").getOrCreate()

# Defining the config parser to read schema from config.ini file
config = configparser.ConfigParser()
config.read(r'../ProjectConfig/config.ini')

# reading input paths/data and schema
inputLocation = config.get('paths', 'inputLocation')
outputLocation = config.get('paths', 'outputLocation')
landingFileSchemaConfig = config.get('schema', 'landingFileSchema')
holdFileSchemaConfig = config.get('schema', 'holdFileSchema')

productPriceReferenceConfig = config.get('schema', 'productPriceReferenceSchema')
validFileSchemaConfig = config.get('schema', 'validFileSchema')

currDayZoneSuffix = '_02102023' #'_01102023'
prevDayZoneSuffix = '_01102023' #_30092023'
print(currDayZoneSuffix)
print(prevDayZoneSuffix)


# reading schemas
validFileSchema = read_schema(validFileSchemaConfig)
productPriceReferenceFileSchema = read_schema(productPriceReferenceConfig)

#reading valid data

validDataDF = spark.read\
    .schema(validFileSchema)\
    .option("delimiter", "|")\
    .option("header", True)\
    .csv(outputLocation+"Valid\\ValidData"+currDayZoneSuffix)

validDataDF.createOrReplaceTempView("validDataDF")

# reading the file data which is provided by company to update price

projectPriceReferenceDF = spark.read\
    .schema(productPriceReferenceFileSchema)\
    .option("delimiter", "|")\
    .option("header", True)\
    .csv(inputLocation+"Products")

projectPriceReferenceDF.createOrReplaceTempView("projectPriceReferenceDF")

# fetching the all updated Enrichdata with price

productEnricDF = spark.sql("SELECT a.Sale_ID , b.Product_ID , b.Product_Name , "
                           "a.Quantity_Sold , a.Vendor_ID , a.Sale_Date, "
                           "b.Product_Price * a.Quantity_Sold as Sale_Amount, "
                           "a.Sale_Currency "
                           "FROM validDataDF a INNER JOIN projectPriceReferenceDF b "
                           "ON a.Product_ID = b.Product_ID")


# writing /saving productEnricDF data int Saleamountenrichment folder


productEnricDF.write\
    .option("header", True)\
    .option("delimiter","|")\
    .mode("overwrite")\
    .csv(outputLocation+"Enriched\\SaleAmountEnrichment\\SaleAmountEnrichment"+currDayZoneSuffix)


