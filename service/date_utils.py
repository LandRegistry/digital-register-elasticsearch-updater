from datetime import datetime


def format_date_with_millis(date):
    # Convert from microseconds to milliseconds by removing the last 3 chars
    time = date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
    zone = '+0000'
    return '{}{}'.format(time, zone)


def string_to_date(datetime_string):
    return datetime.strptime(datetime_string, '%Y-%m-%dT%H:%M:%S.%f%z')
