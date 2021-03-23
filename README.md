### Steps to set up the postgresql database:
1. `docker pull postgres`
2. `docker run -e POSTGRES_USER=some_user -e POSTGRES_PASSWORD=root@pwd -e POSTGRES_DB=product_db library/postgres`

### Steps to create the prod_desc and prod_count tables
1. `psql -h localhost -d product_db -U some_user -p 5432 -a -q -f table_creation_ddl.sql`

### Steps to run the code:
1. `pip3 install pipenv`
2. `pipenv install`
3. `pipenv run python main.py`
