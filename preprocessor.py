"""
The data preprocessor checks the raw BRC data coming from the Trektellen database. It flags records containing possibly
erroneous or at least suspicious information.

Author: Bart Hoekstra
Email: bart.hoekstra@batumiraptorcount.org
"""

import os
import datetime

import pandas as pd

season_start = os.environ['CURRENT_SEASON_START']
season_end = os.environ['CURRENT_SEASON_END']
hb_focus_start = os.environ['HB_FOCUS_START']
hb_focus_end = os.environ['HB_FOCUS_END']
window_minutes = int(os.environ['TIME_WINDOW_MINUTES'])  # window used to check total number of birds with aged numbers

# Overlapping zones. For both stations the distance codes are keys and the corresponding overlapping distance codes from
# the other station are values
overlapping_zones = {
    '1. Sakhalvasho': {
        'W3': 'W3',
        'W2': 'W3',
        'W1': 'W3',
        'O': 'W3',
        'E1': 'W3',
        'E2': ['W3', 'W2'],
        'E3': ['W2', 'W1', 'O', 'E1', 'E2', 'E3']
    },
    '2. Shuamta': {
        'W3': ['W3', 'W2', 'W1', 'O', 'E1', 'E2'],
        'W2': ['E3', 'E2'],
        'W1': 'E3',
        'O': 'E3',
        'E1': 'E3',
        'E2': 'E3',
        'E3': 'E3'
    }
}

# Expected species and sex and age combinations.
# - None indicates an age or sex is not expected to be set
# - A list indicates the expected options for both age and/or sex. If a list contains a None value, it can also remain
#   remain empty.
expected_combinations = {
    'BK': {'age': None, 'sex': None},
    'BK_JUV': {'age': None, 'sex': None},
    'BK_NONJUV': {'age': None, 'sex': None},
    'BlackV': {'age': ['J', 'I', 'A', 'Non-Juv', None], 'sex': None},
    'BlaStork': {'age': ['J', 'I', 'A', 'Non-Juv', None], 'sex': None},
    'BootedE': {'age': ['J', 'Non-Juv', None], 'sex': None},
    'CrestedHB': {'age': ['J', 'A'], 'sex': ['M', 'F']},
    'DalPel': {'age': ['J', 'I', 'A', 'Non-Juv', None], 'sex': None},
    'DemCrane': {'age': ['J', 'A', 'I', 'Non-Juv', None], 'sex': None},
    'EgyptianV': {'age': ['J', 'I', 'A', 'Non-Juv'], 'sex': None},
    'EuCrane': {'age': ['J', 'A', 'I', 'Non-Juv', None], 'sex': None},
    'GoldenE': {'age': ['J', 'I', 'A', 'Non-Juv'], 'sex': None},
    'GreaterSE': {'age': ['J', 'I', 'A', 'Non-Juv'], 'sex': None},
    'GriffonV': {'age': ['J', 'I', 'A', 'Non-Juv', None], 'sex': None},
    'HB': {'age': None, 'sex': None},
    'HB_JUV': {'age': None, 'sex': None},
    'HB_NONJUV': {'age': None, 'sex': ['M', 'F', None]},
    'Hen': [('J', None), ('I', 'M'), ('A', 'M'), ('Non-Juv', 'M'), ('I', 'F'), ('A', 'F'), ('Non-Juv', 'F'),
            (None, 'FC'), (None, None)],
    'ImperialE': {'age': ['J', 'I', 'A', 'Non-Juv'], 'sex': None},
    'Lanner': {'age': ['J', 'I', 'A', 'Non-Juv', None], 'sex': None},
    'Large EAGLE': {'age': ['J', 'I', 'A', 'Non-Juv', None], 'sex': None},
    'large FALCON': {'age': None, 'sex': None},
    'LesserSE': {'age': ['J', 'I', 'A', 'Non-Juv'], 'sex': None},
    'Marsh': [('J', None), ('I', 'M'), ('A', 'M'), ('Non-Juv', 'M'), ('I', 'F'), ('A', 'F'), ('Non-Juv', 'F'),
              (None, 'FC'), (None, None)],
    'Mon': [('J', None), ('I', 'M'), ('A', 'M'), ('Non-Juv', 'M'), ('I', 'F'), ('A', 'F'), ('Non-Juv', 'F')],
    'MonPalHen': [('J', None), ('Non-Juv', 'M'), ('Non-Juv', 'F'), (None, 'FC'), (None, None)],
    'Osprey': {'age': ['J', 'I', 'A', 'Non-Juv', None], 'sex': ['M', 'F', None]},
    'Pal': [('J', None), ('I', 'M'), ('A', 'M'), ('Non-Juv', 'M'), ('I', 'F'), ('A', 'F'), ('Non-Juv', 'F')],
    'Peregrine': {'age': ['J', 'I', 'A', 'Non-Juv', None], 'sex': None},
    'Roller': {'age': None, 'sex': None},
    'SakerF': {'age': ['J', 'I', 'A', 'Non-Juv'], 'sex': None},
    'ShortTE': {'age': ['J', 'I', 'A', 'Non-Juv', None], 'sex': None},
    'StepBuz': {'age': ['J', 'I', 'A', 'Non-Juv', None], 'sex': None},
    'SteppeE': {'age': ['J', 'I', 'A', 'Non-Juv'], 'sex': None},
    'StockD': {'age': None, 'sex': None},
    'TurtleD': {'age': None, 'sex': None},
    'WhitePel': {'age': ['J', 'I', 'A', 'Non-Juv', None], 'sex': None},
    'WhiStork': {'age': ['J', 'A', 'Non-Juv', None], 'sex': None},
    'WhiteTE': {'age': ['J', 'I', 'A', 'Non-Juv'], 'sex': None},
    'WoodP': {'age': None, 'sex': None},
}


def preprocess_raw_trektellen_data(data_csv, times, date=None, split_by_station=False):
    data = pd.read_csv(data_csv)

    # Change timestamp to 00:00 if timestamp was missing
    timestamp_missing = data['timestamp'].isnull()
    data.loc[timestamp_missing, 'timestamp'] = '00:00:00.00'

    # Create a new datetime column combining both the original date and timestamp columns
    data['datetime'] = pd.to_datetime(data.date, format='%Y-%m-%d') + pd.to_timedelta(data.timestamp)

    # Remove unused columns, including the date and timestamp columns, which we can regenerate later on
    data.drop(columns=['date', 'timestamp', 'countid', 'speciesid', 'year', 'yday'], inplace=True)

    # Add start and end times
    time_records = [[times['s1_start'], 1047, 'START', 1, 0, 0, 'O'],
                    [times['s1_end'], 1047, 'END', 1, 0, 0, 'O'],
                    [times['s2_start'], 1048, 'START', 1, 0, 0, 'O'],
                    [times['s2_end'], 1048, 'END', 1, 0, 0, 'O']]

    count_times = pd.DataFrame(time_records, columns=['datetime', 'telpost', 'speciesname', 'count',
                                                      'countback', 'local', 'location'])
    data = pd.concat([data, count_times], sort=True)

    # Change column order
    column_order = ['datetime', 'telpost', 'speciesname', 'count', 'countback', 'local', 'age', 'sex', 'plumage',
                    'remark', 'location', 'migtype', 'counttype']
    data = data[column_order]

    # Replace station numbers with names
    data.loc[(data.telpost == 1047), 'telpost'] = "1. Sakhalvasho"
    data.loc[(data.telpost == 1048), 'telpost'] = "2. Shuamta"

    # Replace HB_AD with HB_NONJUV
    data.loc[(data.speciesname == 'HB_AD'), 'speciesname'] = 'HB_NONJUV'

    # Sort file by newly created dates and telpost names
    data.sort_values(by=['datetime', 'telpost'], inplace=True)

    # Remove all records from counts outside of the predetermined season or date
    if date is None:
        season = (data['datetime'] > season_start) & (data['datetime'] <= season_end)
    else:
        date_string = date.strftime('%Y-%m-%d')
        date_next = date + datetime.timedelta(days=1)
        date_next_string = date_next.strftime('%Y-%m-%d')
        season = (data['datetime'] >= date_string) & (data['datetime'] < date_next_string)

    data = data[season]

    # Now reset the index to start off fresh
    data.reset_index(drop=True, inplace=True)

    if split_by_station:
        mask_station1 = data['telpost'] == '1. Sakhalvasho'
        mask_station2 = data['telpost'] == '2. Shuamta'
        data_station1 = data[mask_station1]
        data_station2 = data[mask_station2]
        return data, data_station1, data_station2
    else:
        return data


def preprocess_trektellen_data(data, split_by_station=False):
    # Check doublecounts
    doublecount_records = data[data['counttype'] == 'D']
    doublecount_records.reset_index(inplace=True)
    nr_doublecounts = doublecount_records.shape[0]

    suspicious_dc_records = []

    iter_doublecounts = doublecount_records.iterrows()

    for index, row in iter_doublecounts:
        suspicious = False

        if index == nr_doublecounts - 1:
            break

        next_row = doublecount_records.iloc[index + 1]  # index is 0-based

        # Compare times. Do the double counts fall within a 10 minute window from each other?
        minutes_diff = (next_row['datetime'] - row['datetime']).total_seconds() / 60.0

        if minutes_diff > 10:
            suspicious = True

        # Are the species the same?
        if row['speciesname'] != next_row['speciesname']:
            suspicious = True

        # Age the same?
        if not pd.isna(row['age']) and pd.isna(next_row['age']):
            if row['age'] != next_row['age']:
                suspicious = True

        # Sex the same?
        if not pd.isna(row['sex']) and pd.isna(next_row['sex']):
            if row['sex'] != next_row['sex']:
                suspicious = True

        # Count the same?
        if row['count'] != next_row['count'] or row['countback'] != next_row['countback']:
            suspicious = True

        # Compare distance codes
        # Consecutive doublecount records cannot be from the same station
        if row['telpost'] == next_row['telpost']:
            suspicious = True

        # Distance codes are not overlapping
        if not next_row['location'] in overlapping_zones[row['telpost']][row['location']]:
            suspicious = True

        if not suspicious:
            next(iter_doublecounts)
        else:
            suspicious_dc_records.extend([row['index']])

    # Check number of migtype birds in groups
    many_migtype = (pd.notna(data['migtype'])) & (data['count'] > 1)
    suspicious_migtype_records = data[many_migtype].index.values.tolist()

    # Check whether obligatory columns actually contain information
    obligatory_columns = ['datetime', 'telpost', 'speciesname', 'count', 'location']
    gap_records = data[data[obligatory_columns].isnull().any(axis=1)].index.values.tolist()

    # Check which records are in >E3
    suspicious_location_records = data[data['location'] == '>E3'].index.values.tolist()

    # Check which records contain morphs for species other than Booted Eagles
    nonstandard_morph = ~data['speciesname'].isin(['BootedE']) & data['plumage'].isin(['D', 'L'])
    suspicious_morphs = data[nonstandard_morph].index.values.tolist()

    # Check if records had a missing timestamp which should now be set to 00:00:00
    timestamps = data['datetime'].dt.strftime('%H:%M:%S')
    missing_timestamps = timestamps == '00:00:00'
    missing_timestamps = data[missing_timestamps].index.values.tolist()

    # Check if numbers of aged HBs matches with the number of total HBs
    HBs = data['speciesname'].isin(['HB', 'HB_NONJUV', 'HB_JUV'])
    HBs = data[HBs]

    count_age_mismatch_records_hb = []

    for index, row in HBs.iterrows():
        if row['speciesname'] == 'HB':
            continue

        if (row['telpost'] == '2. Shuamta') & \
           (row['counttype'] != 'S') & \
           (row['datetime'] >= pd.Timestamp(hb_focus_start)) and \
           (row['datetime'] <= pd.Timestamp(hb_focus_end)):
            continue

        window_starttime = row['datetime'] - pd.Timedelta(window_minutes, 'm')
        window_endtime = row['datetime'] + pd.Timedelta(window_minutes, 'm')

        HBs_window = (HBs['datetime'] >= window_starttime) & \
                     (HBs['datetime'] <= window_endtime) & \
                     (HBs['location'] == row['location']) & \
                     (HBs['telpost'] == row['telpost'])
        HBs_window = HBs[HBs_window]

        HB_window = HBs_window[HBs_window['speciesname'] == 'HB']
        total_HB = HB_window['count'].sum()

        HB_NONJUV_window = HBs_window[HBs_window['speciesname'] == 'HB_NONJUV']
        total_HB_NONJUV = HB_NONJUV_window['count'].sum()

        HB_JUV_window = HBs_window[HBs_window['speciesname'] == 'HB_JUV']
        total_HB_JUV = HB_JUV_window['count'].sum()

        if total_HB_NONJUV + total_HB_JUV > total_HB:
            count_age_mismatch_records_hb.extend([index])

    # Check if numbers of aged BKs matches with the number of total BKs
    BKs = data['speciesname'].isin(['BK', 'BK_NONJUV', 'BK_JUV'])
    BKs = data[BKs]

    count_age_mismatch_records_bk = []

    for index, row in BKs.iterrows():
        if row['speciesname'] == 'BK':
            continue

        window_starttime = row['datetime'] - pd.Timedelta(window_minutes, 'm')
        window_endtime = row['datetime'] + pd.Timedelta(window_minutes, 'm')

        BKs_window = (BKs['datetime'] >= window_starttime) & \
                     (BKs['datetime'] <= window_endtime) & \
                     (BKs['location'] == row['location']) & \
                     (BKs['telpost'] == row['telpost'])
        BKs_window = BKs[BKs_window]

        BK_window = BKs_window[BKs_window['speciesname'] == 'BK']
        total_BK = BK_window['count'].sum()

        BK_NONJUV_window = BKs_window[BKs_window['speciesname'] == 'BK_NONJUV']
        total_BK_NONJUV = BK_NONJUV_window['count'].sum()

        BK_JUV_window = BKs_window[BKs_window['speciesname'] == 'BK_JUV']
        total_BK_JUV = BK_JUV_window['count'].sum()

        if total_BK_NONJUV + total_BK_JUV > total_BK:
            count_age_mismatch_records_bk.extend([index])

    # Check whether HBs of Station 2 are singlecounted when they probably should
    non_singlecount_hb = (data['speciesname'] == 'HB') & \
                         (data['counttype'] != 'S') & \
                         (data['datetime'] >= pd.Timestamp(hb_focus_start)) & \
                         (data['datetime'] <= pd.Timestamp(hb_focus_end)) & \
                         (data['telpost'] == '2. Shuamta')

    non_singlecount_hb = data[non_singlecount_hb]
    non_singlecount_hb_records = non_singlecount_hb.index.values.tolist()

    # Check if species are aged outside of expected distances
    ageing_outside_permitted_distances = data['speciesname'].isin(['HB_JUV', 'HB_NONJUV', 'BK_JUV', 'BK_NONJUV']) & \
                                         data['location'].isin(['W3', 'W2', 'E2', 'E3', '>E3'])

    ageing_outside_permitted_distances = data[ageing_outside_permitted_distances]
    ageing_outside_permitted_distances_records = ageing_outside_permitted_distances.index.values.tolist()

    # Check whether age and sex information for all records are within expected combinations
    unexpected_age_records = []
    unexpected_sex_records = []
    unexpected_harrier_records = []

    harriers = ['MonPalHen', 'Mon', 'Pal', 'Hen', 'Marsh']

    for speciesname, details in expected_combinations.items():
        species_records = data[data['speciesname'] == speciesname]

        if speciesname in harriers:
            indexes = species_records.index.tolist()
            expected_harrier_combinations = []

            for combination in details:
                expected_harrier_combination = None

                if combination[0] is not None and combination[1] is not None:
                    expected_harrier_combination = (species_records['age'] == combination[0]) & (
                            species_records['sex'] == combination[1])
                elif combination[0] is None and combination[1] is not None:
                    expected_harrier_combination = (species_records['age'].isna()) & (
                            species_records['sex'] == combination[1])
                elif combination[0] is not None and combination[1] is None:
                    expected_harrier_combination = (species_records['age'] == combination[0]) & (
                        species_records['sex'].isna())
                elif combination[0] is None and combination[1] is None:
                    expected_harrier_combination = (species_records['age'].isna()) & (species_records['sex'].isna())

                expected_harrier_combinations.extend(species_records[expected_harrier_combination].index.tolist())

            unexpected_harrier_records_temp = [index for index in indexes if index not in expected_harrier_combinations]
            unexpected_harrier_records.extend(unexpected_harrier_records_temp)
            continue

        if details['age'] is None:
            unexpected_age = ~species_records['age'].isna()
        else:
            if None in details['age']:
                unexpected_age = ~species_records['age'].isin(details['age']) & ~species_records['age'].isna()
            else:
                unexpected_age = ~species_records['age'].isin(details['age'])

        unexpected_age_records.extend(species_records[unexpected_age].index.tolist())

        if details['sex'] is None:
            unexpected_sex = ~species_records['sex'].isna()
        else:
            if None in details['sex']:
                unexpected_sex = ~species_records['sex'].isin(details['sex']) & ~species_records['sex'].isna()
            else:
                unexpected_sex = ~species_records['sex'].isin(details['sex'])

        unexpected_sex_records.extend(species_records[unexpected_sex].index.tolist())

    # Check if juvenile harriers are identified in W3 or E3
    unreliable_juvenile_harriers = (data['speciesname'].isin(['Mon', 'Pal', 'Hen', 'Marsh'])) & \
                                   (data['location'].isin(['W3', 'E3'])) & (data['age'] == 'J')
    unreliable_juvenile_harriers_records = data[unreliable_juvenile_harriers].index.values.tolist()

    # Add flags to check column
    data['check'] = ""
    data.loc[unexpected_age_records, 'check'] = data.loc[unexpected_age_records, 'check'] + 'unexpected age, '
    data.loc[unexpected_sex_records, 'check'] = data.loc[unexpected_sex_records, 'check'] + 'unexpected sex, '
    data.loc[unexpected_harrier_records, 'check'] = data.loc[unexpected_harrier_records, 'check'] + 'unexpected age + sex combination, '
    data.loc[ageing_outside_permitted_distances_records, 'check'] = data.loc[ageing_outside_permitted_distances_records, 'check'] + 'ageing distance, '
    data.loc[non_singlecount_hb_records, 'check'] = data.loc[non_singlecount_hb_records, 'check'] + 'singlecount missing? (leave as is), '
    data.loc[count_age_mismatch_records_hb, 'check'] = data.loc[count_age_mismatch_records_hb, 'check'] + 'mismatch number of counted and aged birds, '
    data.loc[count_age_mismatch_records_bk, 'check'] = data.loc[count_age_mismatch_records_bk, 'check'] + 'mismatch number of counted and aged birds, '
    data.loc[suspicious_morphs, 'check'] = data.loc[suspicious_morphs, 'check'] + 'unexpected morph, '
    data.loc[missing_timestamps, 'check'] = data.loc[missing_timestamps, 'check'] + 'incorrect timestamp, '
    data.loc[suspicious_location_records, 'check'] = data.loc[suspicious_location_records, 'check'] + 'unusual location, '
    data.loc[gap_records, 'check'] = data.loc[gap_records, 'check'] + 'gaps in essential columns, '
    data.loc[suspicious_dc_records, 'check'] = data.loc[suspicious_dc_records, 'check'] + 'erroneous doublecount, '
    data.loc[suspicious_migtype_records, 'check'] = data.loc[suspicious_migtype_records, 'check'] + 'unusual nr of killed/injured birds, '
    data.loc[unreliable_juvenile_harriers_records, 'check'] = data.loc[unreliable_juvenile_harriers_records, 'check'] + 'unreliable ageing, '
    data['check'] = data['check'].str[:-2]

    if split_by_station:
        mask_station1 = data['telpost'] == '1. Sakhalvasho'
        data_station1 = data[mask_station1]
        data_station2 = data[~mask_station1]
        return data, data_station1, data_station2
    else:
        return data


if __name__ == "__main__":
    data = preprocess_raw_trektellen_data('data/2019.csv')
    data = preprocess_trektellen_data(data)
    data.to_csv('data/2019-checked.csv', index=False)
