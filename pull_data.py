import pandas as pd
from sqlalchemy import create_engine
import datetime
import schedule
import time


def main():
    date = datetime.date.today()
    path = "./"
    uri = 'mysql+pymysql://local_data:123456@IF-ABE-D162F.ad.ufl.edu/local_data'
    query = """
            select * from sensor_data
            where date(Time_EST) = CURDATE() - INTERVAL 1 DAY

            """
    df = pd.read_sql(query, con=uri)
    df.to_csv(path + f'{date}.csv',index=False)


if __name__ == '__main__':
    schedule.every().day.at("08:00").do(main)
    while True:
        # Checks whether a scheduled task
        # is pending to run or not
        schedule.run_pending()
        time.sleep(60)