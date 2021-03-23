# Large File Processor

<b>Project Structure:</b>.  
 .    
    ├── dependency_jars                     # contains the dependency jars        
    ├── source_files                        # contains the csv source files.     
    ├── db-config.properties                # database configuration properties file.   
    ├── main.py                             # main python script to process the files and write them into the database.   
    ├── Pipfile                             # manage the requirements (libraries and dependencies)    
    ├── table_creation_ddl.sql              # sql script to create the required tables.   
    └── README.md                           # README file for execution instructions.   

## Steps to set up and run the project

### Steps to set up the postgresql database:
1. `docker pull postgres`
2. `docker run -e POSTGRES_USER=some_user -e POSTGRES_PASSWORD=root@pwd -e POSTGRES_DB=product_db library/postgres`

### Steps to create the prod_desc and prod_count tables
1. `psql -h localhost -d product_db -U some_user -p 5432 -a -q -f table_creation_ddl.sql`

### Steps to run the code:
1. `pip3 install pipenv`
2. `pipenv install`
3. `pipenv run python main.py`

## Details of the tables and their schemas

### prod_desc - product description table
sku - varchar(255)  
name - varchar(255)  
description - varchar(255)   
composite primary key (sku, name).  

### prod_count - product count table
name - varchar(255)  
no_of_products - integer

The table creation commands are available in `table_creation_ddl.sql` script.

## What is done from “Points to achieve” and number of entries in the tables with sample 10 rows from each
### Achievements:
1. The code follows the concept of OOPS
2. Apache Spark has been used for parallel ingestion of file
3. Spark also takes less time and it's taking less than a minute to load the data from the file in both the tables.
4. All product details are being ingested in a single table, in this case `prod_desc`
5. An aggregated table `prod_count` on the given rows with `name` and `no_of_products` as the columns have been created.
6. Anytime that there is a upsertion of data in the `prod_desc` table, it also upserts the `prod_count` table.
7. The SQL queries can be run easily.

### Number of entries in the prod_desc table with 10 sample rows:
No. of entries = 861680 (6 rows were removed during the processing cause of duplication in the subset, sku and name).  

Sample rows:
![image](https://user-images.githubusercontent.com/27270128/112167198-46354f00-8c16-11eb-8daa-92c35ff16c5f.png)

### Number of entries in the prod_count table with 10 sample rows:
No. of entries = 583710

Sample rows:
![image](https://user-images.githubusercontent.com/27270128/112167657-9f04e780-8c16-11eb-9a77-e72878d3fe2a.png)

## What is not done from “Points to achieve" -  Reasons and Workaround
1. The sku has not been used as a primary key individually. I have created a composite key with sku and name. It needs more data cleaning and understanding. Maybe, some skus can be assigned to a few of them.
2. The nulls in the sku column has been replaced by "unidentified-sku" because nulls were there in many rows and they had to be handled.

## Possible Improvements:
1. It can be made scalable. For now, it fulfills the purpose, but it can be made more scalable and flexible based in the input data present.
2. Better deployment structure can be adapted. If given more time, the entire procedure to setup and run this project can be managed by docker.
3. It requires unit testing. Unit test cases can be written, along with some integration and regrssion tests.
