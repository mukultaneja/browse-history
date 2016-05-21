
try:
    import sqlite3 as sql
    import os
    import pandas as pd
    import logging
    import argparse
    from datetime import datetime, timedelta
    # import timeit
    from urlparse import urlparse
except ImportError as e:
    print e.__str__()

TABLES = []
# setting to ignore SettingWithCopyWarning
pd.options.mode.chained_assignment = None


def add_parse_arguments(parser):
    '''
        Function : add_parse_arguments
        It takes a parser object as an
        arguments and add multiple arguments
        to it which take inputs from user.
    '''
    # short name for arguments
    s_name = ['-b', '-tn', '-sd', '-ed', '-l', '-a', '-t']
    # long name for arguments
    l_name = ['--browser', '--table', '--startdate', '--enddate',
              '--limit', '--address', '--title']
    # help messages for arguments
    help_msgs = [
        '(b) Browser name which history to be fetched.',
        '(tn) Table name from which data needs to be fetched.',
        '(sd) Start date From where to fetch history.',
        '(ed) End date to fetch history.',
        '(l) Limits of results.',
        '(a) Address for a domain to search for.',
        '(t) Specific title of a web page to search for.'
    ]
    # default values for arguments
    default = ['chrome', 'urls', None, datetime.now(), 20, '', '']
    # zippling all the value to add into parser object
    for arg in zip(s_name, l_name, help_msgs, default):
        parser.add_argument(arg[0],
                            arg[1],
                            help=arg[2],
                            default=arg[3])
    return parser


def get_connection(browser_name, db='History'):
    '''
        Function : get_connection
        It takes one default input variable
        named db which refers to db name for
        the browser. It makes the connection
        to the database and retuns a connection
        object.

        For future version we have added multiple
        browser option which we will be implemened
        in next release.

        Now by default it is targetting to chrome
        browser.
    '''
    if browser_name == 'chrome':
        PAR_DIR = os.getenv('APPDATA')
        G_PATH = 'Local\Google\Chrome\User Data\Default'
        DIR_PATH = os.path.join(os.path.dirname(PAR_DIR), G_PATH)
        print DIR_PATH

    try:
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
        logging.info('Table {}'.format(args.table))
        # making a cursor object to access tables in db
        c = conn.cursor()
        query = 'SELECT * from {}'.format(args.table)
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
        # closing the cursor
        c.close()

if __name__ == '__main__':
    # main function
    # logging to notify user for events
    logging.basicConfig(level=logging.INFO)
    # creating a argument parser object
    parser = argparse.ArgumentParser()
    # adding multiple option to parser
    parser = add_parse_arguments(parser)
    args = parser.parse_args()
    # establishing a db connection here
    conn = get_connection(args.browser)
    # getting results from chrome's table as of now
    get_contents(conn, args)
    # closing db connection
    conn.close()
