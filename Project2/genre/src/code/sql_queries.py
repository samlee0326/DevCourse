import configparser
import os

#현재 파일의 상위 폴더 위치
PARENT_DIR = os.path.dirname(os.getcwd())

config = configparser.ConfigParser()
config.read(os.path.join(PARENT_DIR,'config','conf.cfg'))


create_raw_data_schema = "CREATE SCHEMA raw_data2"
create_analysis_schema = "CREATE SCHEMA analysis"

daily_boxoffice_table_create = """
CREATE TABLE raw_data2.daily_boxoffice (
    voting_date DATE,
    ranking INTEGER,
    title VARCHAR(150),
    released_date DATE,
    sales INT8,
    sales_market_portion FLOAT8,
    sales_comp_yesterday INT8,
    sales_fluctuation_rate FLOAT8,
    total_sales INT8,
    audience_num INT8,
    audience_num_delta INT8,
    audience_num_fluctuatation_rate FLOAT8,
    total_audience INT8,
    screen_num INT8,
    total_played_time INT8,
    country_of_origin VARCHAR(150),
    country VARCHAR(225),
    production_comp_name VARCHAR(225),
    distributor_name VARCHAR(225),
    film_ratings VARCHAR(225),
    genre TEXT,
    director TEXT,
    performers VARCHAR(10000)
);
"""

create_genre_table = """
CREATE TABLE analysis.genre_data (
    voting_date DATE,
    genre TEXT,
    audience_num INT8
);
"""


staging_box_office_table = (f"""
COPY raw_data2.daily_boxoffice 
FROM {config['BUCKET']['raw_s3']}
CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['arn']}'
delimiter ',' dateformat 'auto' timeformat 'auto' IGNOREHEADER 1 removequotes;
""")


staging_genre_table= (f"""
COPY analysis.genre_data 
FROM {config['BUCKET']['genre_s3']}
CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['arn']}'
delimiter ',' dateformat 'auto' timeformat 'auto' IGNOREHEADER 1;
""")

create_schema_queries = [create_raw_data_schema,create_analysis_schema]
create_table_queries = [daily_boxoffice_table_create,create_genre_table]
staging_table_queries = [staging_box_office_table,staging_genre_table]