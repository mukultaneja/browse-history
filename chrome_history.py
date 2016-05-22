
try:
    import sqlite3 as sql
    import os
    import pandas as pd
    import logging
    from datetime import datetime, timedelta
    from urlparse import urlparse
except ImportError as e:
    print e.__str__()

TABLES = []
# setting to ignore SettingWithCopyWarning
pd.options.mode.chained_assignment = None


def get_connection(os_name, db='History'):
    '''
        Function : get_connection
        It takes one default input variable
        named db which refers to db name for
        the browser. It makes the connection
        to the database and retuns a connection
        object.
    '''
    try:
        if 'Windows' in os_name:
            PAR_DIR = os.getenv('APPDATA')
            G_PATH = 'Local\Google\Chrome\User Data\Default'
            DIR_PATH = os.path.join(os.path.dirname(PAR_DIR), G_PATH)
            logging.info('Establishing connection with the database')
            # setting a db connection
            conn = sql.connect(os.path.join(DIR_PATH, db))
            # text factory = str so that results comes in ascii format only
            conn.text_factory = str
            return conn
    except sql.OperationalError as e:
        print e.message


def get_tables(conn):
    '''
        Function get_tables
        It is a helping function to get to know the tables names
        implemented in database.
    '''
    c = conn.cursor()
    query = 'SELECT * from sqlite_master where type="table"'
    result = c.execute(query)
    logging.info('Fetching table names from database')
    for row in result:
        TABLES.append(str(row[1]))
    c.close()


def get_date(visit_time):
    '''
        Function get_date
        It takes a timestamp value and returns a datetime object
        respective to that timestamp.
        Google calculated a timestamp value since 1601 so this
        function takes care for this difference while converting
        timestamp to datetime object.
    '''
    return pd.Timestamp(datetime(1601, 1, 1) +
                        timedelta(microseconds=int(visit_time)))


def get_contents(conn, args):
    '''
        Function get_contents
        It fetches the history from the table.
        As currently we are targetting only
        chrome browser so it by default fetch
        history from its urls table and store
        the result into a pandas dataframe from
        where we are manipulating results
        for user options. It returns a final
        output which comes after applying
        all the operation to fetched result
        from database.
    '''
    try:
        # making a cursor object to access tables in db
        c = conn.cursor()
        query = 'SELECT * from urls'
        result = c.execute(query)

    except sql.OperationalError as e:
        print e.message

    else:
        # fetching all rows and columns first
        df = pd.DataFrame(row for row in result)
        # storing data and columns into dataframe
        df.columns = list(map(lambda x: x[0], result.description))
        # filtering columns according to need
        data = df[['url', 'title', 'visit_count']]
        # adding hostname for website link to filter result basis on it
        data['host'] = data['url'].apply(lambda x: urlparse(x).hostname).fillna('')
        data['last_visit_date'] = df['last_visit_time'].apply(lambda x: get_date(x))

        if args.startdate:
            # filter based on start date
            sd = datetime.strptime(args.startdate, '%d/%m/%Y')
            ed = args.enddate
            data = data[(data['last_visit_date'] > pd.Timestamp(sd)) &
                        (data['last_visit_date'] <= pd.Timestamp(ed))]

        if args.address:
            # filter based on domain name
            data = data[data['host'].str.contains(args.address)]

        # sort the result
        data = data.sort_values(by='last_visit_date')
        print data.tail(int(args.limit))

    finally:
        c.close()


def main(os_name, args):
    # main function
    # logging to notify user for events
    logging.basicConfig(level=logging.INFO)
    # establishing a db connection here
    conn = get_connection(os_name)
    # getting results from chrome's table as of now
    get_contents(conn, args)
    # closing db connection
    conn.close()
