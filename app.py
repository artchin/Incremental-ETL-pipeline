import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import json

with open("cred.json", "r") as f:
    cred = json.load(f)

url = f"postgresql://{cred['user']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['database']}"
engine = create_engine(url)
conn = psycopg2.connect(**cred)
cursor = conn.cursor()


def csv2sql(path):
    df = pd.read_csv(path)
    df.to_sql(name="tmp_auto", con=engine, schema="sbx", if_exists="replace", index=False)


def create_auto_hist():
    cursor.execute(""" 
        create table if not exists sbx.auto_hist(
            id serial primary key,
            model varchar(128),
            transmission varchar(128),
            body_type varchar(128),
            drive_type varchar(128),
            color varchar(128),
            production_year integer,
            auto_key integer,
            engine_capacity numeric(3,1),
            horsepower integer,
            engine_type varchar(128),
            price integer,
            milage integer,
            deleted_flg integer default 0,
            start_dttm timestamp default current_timestamp,
            end_dttm timestamp default ('5999-12-31 23:59:59'::timestamp)
        )
    """)
    conn.commit()


def create_v_auto():
    cursor.execute("drop view if exists sbx.v_auto")
    cursor.execute(""" 
        create view sbx.v_auto as 
            select 
                id,
                model,
                transmission,
                body_type,
                drive_type,
                color,
                production_year,
                auto_key,
                engine_capacity,
                horsepower,
                engine_type,
                price,
                milage
            from sbx.auto_hist
            where deleted_flg = 0
            and current_timestamp between start_dttm and end_dttm
    """)
    conn.commit()

def create_new_rows():
    cursor.execute(""" 
        create table sbx.tmp_new_rows as 
            select 
                t1.model,
                t1.transmission,
                t1.body_type,
                t1.drive_type,
                t1.color,
                t1.production_year,
                t1.auto_key,
                t1.engine_capacity,
                t1.horsepower,
                t1.engine_type,
                t1.price,
                t1.milage
            from sbx.tmp_auto t1 
            left join sbx.v_auto t2 
            on t1.auto_key = t2.auto_key
            where t2.auto_key is null      
    """)
    conn.commit()

def create_deleted_rows():
    cursor.execute(""" 
        create table sbx.tmp_deleted_rows as 
            select 
                t1.model,
                t1.transmission,
                t1.body_type,
                t1.drive_type,
                t1.color,
                t1.production_year,
                t1.auto_key,
                t1.engine_capacity,
                t1.horsepower,
                t1.engine_type,
                t1.price,
                t1.milage
            from sbx.v_auto t1 
            left join sbx.tmp_auto t2 
            on t1.auto_key = t2.auto_key
            where t2.auto_key is null      
    """)
    conn.commit()


def create_updated_rows():
    cursor.execute(""" 
        create table sbx.tmp_updated_rows as 
            select 
                t1.model,
                t1.transmission,
                t1.body_type,
                t1.drive_type,
                t1.color,
                t1.production_year,
                t1.auto_key,
                t1.engine_capacity,
                t1.horsepower,
                t1.engine_type,
                t1.price,
                t1.milage
            from sbx.tmp_auto t1 
            inner join sbx.v_auto t2 
            on t1.auto_key = t2.auto_key
            where  (t1.model           <> t2.model            
                or (t1.model is null and t2.model is not null) 
                or (t1.model is not null and t2.model is null)
            )
            or (t1.transmission    <> t2.transmission     
                or (t1.transmission is null and t2.transmission is not null) 
                or (t1.transmission is not null and t2.transmission is null)
            )
            or (t1.body_type       <> t2.body_type        
                or (t1.body_type is null and t2.body_type is not null) 
                or (t1.body_type is not null and t2.body_type is null)
            )
            or (t1.drive_type      <> t2.drive_type       
                or (t1.drive_type is null and t2.drive_type is not null) 
                or (t1.drive_type is not null and t2.drive_type is null)
            )
            or (t1.color           <> t2.color            
                or (t1.color is null and t2.color is not null) 
                or (t1.color is not null and t2.color is null)
            )
            or (t1.production_year <> t2.production_year  
                or (t1.production_year is null and t2.production_year is not null) 
                or (t1.production_year is not null and t2.production_year is null)
            )
            or (t1.engine_capacity <> t2.engine_capacity  
                or (t1.engine_capacity is null and t2.engine_capacity is not null) 
                or (t1.engine_capacity is not null and t2.engine_capacity is null)
            )
            or (t1.horsepower      <> t2.horsepower       
                or (t1.horsepower is null and t2.horsepower is not null) 
                or (t1.horsepower is not null and t2.horsepower is null)
            )
            or (t1.engine_type     <> t2.engine_type      
                or (t1.engine_type is null and t2.engine_type is not null) 
                or (t1.engine_type is not null and t2.engine_type is null)
            )
            or (t1.price           <> t2.price            
                or (t1.price is null and t2.price is not null) 
                or (t1.price is not null and t2.price is null)
            )
            or (t1.milage          <> t2.milage           
                or (t1.milage is null and t2.milage is not null) 
                or (t1.milage is not null and t2.milage is null)
            )
    """)
    conn.commit()

def update_auto_hist():
    # написать процесс, который добавляет все записи из tmp_new_rows
    # в auto_hist
    cursor.execute(""" 
        insert into sbx.auto_hist (
                model,
                transmission,
                body_type,
                drive_type,
                color,
                production_year,
                auto_key,
                engine_capacity,
                horsepower,
                engine_type,
                price,
                milage
        )
        select 
            model,
            transmission,
            body_type,
            drive_type,
            color,
            production_year,
            auto_key,
            engine_capacity,
            horsepower,
            engine_type,
            price,
            milage
        from sbx.tmp_new_rows
    """)

    cursor.execute(""" 
        update sbx.auto_hist
        set end_dttm = current_timestamp - interval '1 second'
        where auto_key in (select auto_key from sbx.tmp_updated_rows)
        and end_dttm = '5999-12-31 23:59:59'::TIMESTAMP
    """)

    cursor.execute(""" 
        insert into sbx.auto_hist (
                model,
                transmission,
                body_type,
                drive_type,
                color,
                production_year,
                auto_key,
                engine_capacity,
                horsepower,
                engine_type,
                price,
                milage
        )
        select 
            model,
            transmission,
            body_type,
            drive_type,
            color,
            production_year,
            auto_key,
            engine_capacity,
            horsepower,
            engine_type,
            price,
            milage
        from sbx.tmp_updated_rows
    """)

    cursor.execute(""" 
        update sbx.auto_hist
        set end_dttm = current_timestamp - interval '1 second'
        where auto_key in (select auto_key from sbx.tmp_deleted_rows)
        and end_dttm = '5999-12-31 23:59:59'::TIMESTAMP
    """)

    cursor.execute(""" 
        insert into sbx.auto_hist (
                model,
                transmission,
                body_type,
                drive_type,
                color,
                production_year,
                auto_key,
                engine_capacity,
                horsepower,
                engine_type,
                price,
                milage,
                deleted_flg
        )
        select 
            model,
            transmission,
            body_type,
            drive_type,
            color,
            production_year,
            auto_key,
            engine_capacity,
            horsepower,
            engine_type,
            price,
            milage,
            1
        from sbx.tmp_deleted_rows
    """)

    conn.commit()


def drop_tmp_tables():
    cursor.execute("drop table if exists sbx.tmp_auto")
    cursor.execute("drop table if exists sbx.tmp_deleted_rows")
    cursor.execute("drop table if exists sbx.tmp_new_rows")
    cursor.execute("drop table if exists sbx.tmp_updated_rows")
    conn.commit()

drop_tmp_tables()

csv2sql("store/data_3.csv")
create_auto_hist()
create_v_auto()

create_new_rows()
create_deleted_rows()
create_updated_rows()
update_auto_hist()







