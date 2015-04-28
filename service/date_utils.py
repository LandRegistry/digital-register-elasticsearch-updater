
def format_date_with_millis(date):
    # In order to get milliseconds, the last three characters are removed
    time = date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
    zone = '00'
    return '{}+{}'.format(time, zone)
