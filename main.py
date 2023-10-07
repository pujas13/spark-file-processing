from pyspark.context import SparkContext, SparkConf
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import when, count
from jproperties import Properties
import logging
logging.basicConfig(level=logging.INFO)

# spark configurations
# extra comment
spark_conf = SparkConf().setAppName("large-file-processing") \
    .setMaster("local[4]") \
    .set("spark.driver.extraClassPath", "/dependency_jars/postgresql-42.2.19.jar")

sc = SparkContext(conf=spark_conf)
sqlContext = SQLContext(sc)
spark = SparkSession(sc)


class LargeFileProcessing(object):
    """
    This class is defined to read, transform, and load the large csv files
    """
    def __init__(self):
        self.jdbc_url = ""
        self.connection_properties = ""
        self.jdbc_batchsize = ""

    @staticmethod
    def read_from_csv(file_path):
        """
        This static method reads a csv file and returns that as a spark dataframe
        :param file_path: string
        :return dataframe:
        """
        try:
            src_df = sqlContext.read.csv(file_path, header=True)
            logging.info('source file read successfully')
            return src_df
        except FileNotFoundError as fe:
            logging.error('the source file could not be found ' + str(fe))
        except Exception as e:
            logging.error('error while reading the source file' + str(e))

    def write_prod_desc(self, df, prod_desc_tbl):
        """
        This function is processing the source dataframe to replace the null skus with unidentified-skus
        and drops the duplicates considering sku and name columns.
        It returns the final dataframe which gets written into the table.
        :param df:
        :param prod_desc_tbl:
        :return final_df:
        """
        try:
            src_df = df.withColumn("sku", when(df.sku.isNull(), "undefined-sku").otherwise(df.sku))
            final_df = src_df.drop_duplicates(subset=['sku', 'name'])
            final_df.write.mode("overwrite") \
                    .option("batchsize", self.jdbc_batchsize) \
                    .option("truncate", "true") \
                    .jdbc(url=self.jdbc_url, table=prod_desc_tbl, properties=self.connection_properties)
            logging.info('product description written into the ' + str(prod_desc_tbl) + ' successfully')
            return final_df
        except Exception as e:
            logging.error("Error while writing the product description dataframe into table " + str(e))

    def write_prod_count(self, df, prod_count_tbl):
        """
        This function is processing the final dataframe to get the count of products
        by aggregating them by product names.
        It returns the aggregated dataframe which gets written into the table.
        :param df:
        :param prod_count_tbl:
        :return aggregated_df:
        """
        try:
            aggregated_df = df.groupBy(['name']).agg(count('name').alias('no_of_products'))
            aggregated_df.write.mode("overwrite") \
                         .option("batchsize", self.jdbc_batchsize) \
                         .option("truncate", "true") \
                         .jdbc(url=self.jdbc_url, table=prod_count_tbl, properties=self.connection_properties)
            logging.info('product name and count written into the ' + str(prod_count_tbl) + ' successfully')
            return aggregated_df
        except Exception as e:
            logging.error("Error while writing the product count dataframe into table " + str(e))

    def get_db_properties(self, prop_file):
        """
        This function is to read the jdbc properties from the properties file
        :param prop_file:
        :return declares the connection_properties, batchsize, and jdbc_url:
        """
        configs = Properties()
        with open(prop_file, 'rb') as config_file:
            configs.load(config_file)
        # jdbc connection properties
        logging.info('reading the jdbc connection properties')
        jdbc_host = configs.get("DB_HOST").data
        jdbc_port = configs.get("DB_PORT").data
        jdbc_db_name = configs.get("DB_NAME").data
        jdbc_username = configs.get("DB_USER").data
        jdbc_password = configs.get("DB_PASSWORD").data
        jdbc_driver = configs.get("DB_DRIVER").data
        self.jdbc_batchsize = configs.get("BATCH_SIZE").data

        self.jdbc_url = "jdbc:postgresql://{0}:{1}/{2}".format(jdbc_host, jdbc_port, jdbc_db_name)

        self.connection_properties = {
            "user": jdbc_username,
            "password": jdbc_password,
            "driver": jdbc_driver
        }


def main():
    """
    This is the main function that incorporates the entire process
    :return:
    """
    file_path = 'source_files/products.csv'
    prop_file = 'db-config.properties'
    prod_desc_tbl = 'prod_desc'
    prod_count_tbl = 'prod_count'
    file_processor = LargeFileProcessing()
    # read from csv
    src_df = file_processor.read_from_csv(file_path)
    if src_df is not None:
        # get the jdbc properties
        file_processor.get_db_properties(prop_file)
        # write product description in the prod_desc table
        final_df = file_processor.write_prod_desc(src_df, prod_desc_tbl)
        final_df.show()
        # write aggregated product count
        aggregated_df = file_processor.write_prod_count(final_df, prod_count_tbl)
        aggregated_df.show()


if __name__ == '__main__':
    main()
