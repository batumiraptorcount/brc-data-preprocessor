"""
The fetcher checks whether data has been preprocessed and stored in Dropbox. If that's not the case for either of the
stations for a certain date, it downloads the remaining data from Trektellen.

Author: Bart Hoekstra
Email: bart.hoekstra@batumiraptorcount.org
"""

import logging
import os
import io
import tempfile
from datetime import datetime

import requests
import dropbox
import dropbox.exceptions
import dropbox.files

import preprocessor as prep

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def start_trektellen_session():
    """
    Logs in to Trektellen and returns the session object if logged in successfully.

    :return: session object
    """
    session = requests.Session()
    r = session.post(os.environ['TREKTELLEN_LOGIN_URL'], {'identity': os.environ['TREKTELLEN_USERNAME'],
                                                          'password': os.environ['TREKTELLEN_PASSWORD']})

    if r.url != os.environ['TREKTELLEN_SUCCESSFUL_LOGIN_URL']:
        raise ValueError('Cannot login. Trektellen login credentials are probably incorrect.')
    else:
        return session


def check_data_availability_trektellen(session, date, both_stations=True):
    """
    Checks if data is available for a given date.

    :param session: Trektellen (Requests) session
    :param date: datetime object
    :param both_stations: Bool indicating whether both stations need to have data available for the given date
    :return: True if data is available for a given date, False if no data is available.
    """
    date_string = date.strftime('%Y%m%d')

    station1_url = '{}/{}/{}'.format(os.environ['TREKTELLEN_COUNT_URL'],
                                     os.environ['TREKTELLEN_STATION1_ID'],
                                     date_string)
    station2_url = '{}/{}/{}'.format(os.environ['TREKTELLEN_COUNT_URL'],
                                     os.environ['TREKTELLEN_STATION2_ID'],
                                     date_string)

    r_station1_url = session.get(station1_url).url
    r_station2_url = session.get(station2_url).url

    station1_availability, station2_availability = False, False

    if station1_url == r_station1_url:
        station1_availability = True

    if station2_url == r_station2_url:
        station2_availability = True

    station_availability = [station1_availability, station2_availability]

    if both_stations:
        return all(station_availability), station_availability
    else:
        return any(station_availability), station_availability


def start_dropbox_session():
    try:
        dbx = dropbox.Dropbox(os.environ['DROPBOX_ACCESS_TOKEN'])
        dbx.users_get_current_account()
        return dbx
    except dropbox.exceptions.BadInputError as e:
        logging.critical('Authentication error: {}'.format(e))
        print(e)


def check_data_exists_dropbox(dbx, date, both_stations=True):
    """
    Check if data already exists in Dropbox for a given date

    :param dbx: Dropbox session
    :param date: datetime object
    :param both_stations: Bool indicating whether both stations need to have data in Dropbox for the given date
    :return: True if data is available for a given date, False if no data is available.
    """
    date_string = date.strftime('%Y%m%d')

    # First check if the root data folder exists
    try:
        dbx.files_get_metadata(os.environ['DROPBOX_ROOT_DATA_FOLDER'])
    except dropbox.exceptions.ApiError as e:
        logging.critical(e)

    raw_path_station1 = '{}/raw/{}_S1.xlsx'.format(os.environ['DROPBOX_ROOT_DATA_FOLDER'], date_string)
    raw_path_station2 = '{}/raw/{}_S2.xlsx'.format(os.environ['DROPBOX_ROOT_DATA_FOLDER'], date_string)

    station1_raw_exists, station2_raw_exists = False, False

    # Now check individual files
    try:
        dbx.files_get_metadata(raw_path_station1)
        station1_raw_exists = True
    except dropbox.exceptions.ApiError:
        pass

    try:
        dbx.files_get_metadata(raw_path_station2)
        station2_raw_exists = True
    except dropbox.exceptions.ApiError:
        pass

    raw_data_exists = [station1_raw_exists, station2_raw_exists]

    if both_stations:
        return all(raw_data_exists), raw_data_exists
    else:
        return any(raw_data_exists), raw_data_exists


def download_trektellen_data(session, date=None):
    if date is None:
        season = datetime.strptime(os.environ['CURRENT_SEASON_START'], '%Y-%m-%d')
    else:
        season = date.year

    data_url = '{}{}'.format(os.environ['TREKTELLEN_DOWNLOAD_URL'], season)
    r = session.get(data_url)
    return io.StringIO(r.content.decode('utf-8'))


def upload_file(dbx, filename, remote_folder_path, overwrite=False):
    if overwrite:
        mode = dropbox.files.WriteMode.overwrite
    else:
        mode = dropbox.files.WriteMode.add

    with open(filename, 'rb') as f:
        data = f.read()

    try:
        r = dbx.files_upload(data, remote_folder_path, mode)
    except dropbox.exceptions.ApiError as e:
        logger.error(e)
        return None

    return r


def create_html_response(message):
    body = '<html><head><title>BRC Data Preprocessor - Results</title></head><body>{}</body></html>'.format(message)
    response = {
        'statusCode': 200,
        'body': body,
        'headers': {'Content-Type': 'text/html'}
    }
    return response


def main(event, context):
    dbx = start_dropbox_session()

    if 'queryStringParameters' in event:

        if event['queryStringParameters'] is not None:
            if 'date' not in event['queryStringParameters']:
                date = datetime.now()
            else:
                date = datetime.strptime(event['queryStringParameters']['date'], '%Y%m%d')

            if 'forced' not in event['queryStringParameters']:
                forced = False
            elif event['queryStringParameters']['forced'] == 'yes':
                forced = True
            else:
                forced = False
        else:
            date = datetime.now()
            forced = False
    else:
        date = datetime.now()
        forced = False

    both_stations_processed, separate_stations_processed = check_data_exists_dropbox(dbx, date, both_stations=True)

    if both_stations_processed:
        message = 'Data for {} is processed already and stored in Dropbox.'.format(date.strftime('%d-%m-%Y'))
        response = create_html_response(message)
        return response

    s = start_trektellen_session()
    both_stations_uploaded, separate_stations_uploaded = check_data_availability_trektellen(s, date, both_stations=True)

    if not both_stations_uploaded and not forced:
        message = 'Data for {} for both stations is not uploaded to Trektellen yet.'.format(date.strftime('%d-%m-%Y'))
        response = create_html_response(message)
        return response

    data = download_trektellen_data(s, date=date)

    raw_all, raw_station1, raw_station2 = prep.preprocess_raw_trektellen_data(data, date=date, split_by_station=True)
    checked_all, checked_station1, checked_station2 = prep.preprocess_trektellen_data(raw_all, split_by_station=True)

    s1_path = '{}_S1.xlsx'.format(date.strftime('%Y%m%d'))
    s2_path = '{}_S2.xlsx'.format(date.strftime('%Y%m%d'))

    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)

        remote_raw_s1 = '{}/raw/{}'.format(os.environ['DROPBOX_ROOT_DATA_FOLDER'], s1_path)
        remote_raw_s2 = '{}/raw/{}'.format(os.environ['DROPBOX_ROOT_DATA_FOLDER'], s2_path)

        raw_station1.to_excel(s1_path, index=False)
        raw_station2.to_excel(s2_path, index=False)

        upload_file(dbx, s1_path, remote_raw_s1, overwrite=False)
        upload_file(dbx, s2_path, remote_raw_s2, overwrite=False)

    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)

        remote_checked_s1 = '{}/inprogress/{}'.format(os.environ['DROPBOX_ROOT_DATA_FOLDER'], s1_path)
        remote_checked_s2 = '{}/inprogress/{}'.format(os.environ['DROPBOX_ROOT_DATA_FOLDER'], s2_path)

        checked_station1.to_excel(s1_path, index=False)
        checked_station2.to_excel(s2_path, index=False)

        upload_file(dbx, s1_path, remote_checked_s1, overwrite=False)
        upload_file(dbx, s2_path, remote_checked_s2, overwrite=False)

    message = 'Data for {} has finished processing and is uploaded to Dropbox'.format(date.strftime('%d-%m-%Y'))
    response = create_html_response(message)
    return response


if __name__ == "__main__":
    # main({'date': '20180827', 'forced': 'yes'}, None)
    # main({'date': '20190917', 'forced': 'no'}, None)
    main({'queryStringParameters': {'date': '20181006', 'forced': 'yes'}}, None)
