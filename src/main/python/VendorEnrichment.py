from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,FloatType,TimestampType
from pyspark.sql import functions as psf
from datetime import datetime,timedelta,date, time
import configparser
from schemaFunction import read_schema

spark = SparkSession.builder.appName("DatIngestAndRefine").getOrCreate()

# Defining the config parser to read schema from config.ini file
config = configparser.ConfigParser()
config.read(r'../ProjectConfig/config.ini')

# reading input paths/data and schema
inputLocation = config.get('paths', 'inputLocation')
outputLocation = config.get('paths', 'outputLocation')
landingFileSchemaConfig = config.get('schema', 'landingFileSchema')
productEnrichmentSchemaConfig = config.get('schema', 'productEnrichmentSchema')
usdReferenceSchemaConfig = config.get('schema', 'usdReferenceSchema')
vendorReferenceSchemaConfig = config.get('schema', 'vendorReferenceSchema')

productEnrichmentSchema = read_schema(productEnrichmentSchemaConfig)
usdReferenceSchema = read_schema(usdReferenceSchemaConfig)
vendorReferenceSchema = read_schema(vendorReferenceSchemaConfig)

currDayZoneSuffix = '_02102023' #'_01102023'
prevDayZoneSuffix = '_01102023' #_30092023'
print(currDayZoneSuffix)
print(prevDayZoneSuffix)


# reading productEnrichedDF data which we got from Enrichproduct file
productEnrichedDF = spark.read\
    .schema(productEnrichmentSchema)\
    .option("delimiter", "|")\
    .option("header", True)\
    .csv(outputLocation+"Enriched\\SaleAmountEnrichment\\SaleAmountEnrichment"+currDayZoneSuffix)

productEnrichedDF.createOrReplaceTempView("productEnrichedDF")

# reading data of USD Rates which is provided by company

usdReferencedDF = spark.read\
    .schema(usdReferenceSchema)\
    .option("delimiter", "|")\
    .csv(inputLocation+"USD_Rates")

usdReferencedDF.createOrReplaceTempView("usdReferencedDF")

# reading data of vendors which is provided by company

vendorReferenceDF = spark.read\
    .schema(vendorReferenceSchema)\
    .option("delimiter", "|")\
    .option("header", False)\
    .csv(inputLocation+"Vendors")

vendorReferenceDF.createOrReplaceTempView("vendorReferenceDF")

# joining the product data and vendors data
vendorEnrichDF = spark.sql("SELECT a.*,b.Vendor_Name FROM "
                           "productEnrichedDF a INNER JOIN vendorReferenceDF b "
                           "ON a.Vendor_ID = b.Vendor_ID")

vendorEnrichDF.createOrReplaceTempView("vendorEnrichDF")

# joining the vendorEnrichDF data and USED rates data

usdEnrichDF = spark.sql("SELECT *, ROUND((a.Sale_Amount / b.Exchange_Rate),2) as Amount_in_USD FROM "
                        "vendorEnrichDF a JOIN usdReferencedDF b "
                        "ON a.Sale_Currency = b.Currency_Code")

usdEnrichDF.show()

# Got the final DF as usdEnrichDF and saving it to VemdorUSDEnriched folder

usdEnrichDF.write\
    .option("delimiter", "|")\
    .option("header", True)\
    .mode("overwrite")\
    .csv(outputLocation+"Enriched\\VemdorUSDEnriched\\VemdorUSDEnriched"+currDayZoneSuffix)


# # MySql connectivity
usdEnrichDF.write.format('jdbc').options(
      url='jdbc:mysql://localhost:3306/retailpipeline',
      driver='com.mysql.jdbc.Driver',
      dbtable='readydetail',
      user='root',
      password='root').mode('append').save()
