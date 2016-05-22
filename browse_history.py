
try:
    import platform
    import logging
    import chrome_history
    import argparse
    from datetime import datetime

except ImportError as e:
    print e.__str__()


def add_parse_arguments(parser):
    '''
        Function : add_parse_arguments
        It takes a parser object as an
        arguments and add multiple arguments
        to it which take inputs from user.
    '''
    # short name for arguments
    s_name = ['-b', '-sd', '-ed', '-l', '-a', '-t']
    # long name for arguments
    l_name = ['--browser', '--startdate', '--enddate',
              '--limit', '--address', '--title']
    # help messages for arguments
    help_msgs = [
        '(b) Browser name which history to be fetched.',
        '(sd) Start date From where to fetch history.',
        '(ed) End date to fetch history.',
        '(l) Limits of results.',
        '(a) Address for a domain to search for.',
        '(t) Specific title of a web page to search for.'
    ]
    # default values for arguments
    default = ['chrome', None, datetime.now(), 20, '', '']
    # zippling all the value to add into parser object
    for arg in zip(s_name, l_name, help_msgs, default):
        parser.add_argument(arg[0],
                            arg[1],
                            help=arg[2],
                            default=arg[3])
    return parser

if __name__ == '__main__':
    # main function
    # logging to notify user for events
    logging.basicConfig(level=logging.INFO)
    intro_text = '''\nWelcome to Browse History, A tool
    to watch and analyse browser history.'''
    logging.info(intro_text)
    logging.info('OS Name : %s', platform.system())
    # creating a argument parser object
    parser = argparse.ArgumentParser()
    # adding multiple option to parser
    parser = add_parse_arguments(parser)
    args = parser.parse_args()
    logging.info('Browser Name : %s', args.browser)
    logging.info('Loading your browing history')
    if args.browser == 'chrome':
        chrome_history.main(platform.system(), args)
