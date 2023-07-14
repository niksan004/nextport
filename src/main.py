import time

import mysql.connector
import sqlite3

from cell import Cell

import numpy as np
import configparser
import argparse
from pathlib import Path
from multiprocessing import Process
import os


parser = argparse.ArgumentParser()
config_file_path = Path(__file__).resolve().parents[1] / 'nextport.ini'
parser.add_argument('-c', '--config', help='set config file', nargs='?', default=config_file_path)
config_file_name = parser.parse_args().config

config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
config.read(config_file_name)

SHIP_SAMPLE_SIZE = int(config['Constants']['ship_sample_size'])
SD_COEFFICIENT = int(config['Constants']['sd_coefficient'])
BULK_INSERT = int(config['Constants']['bulk_insert'])


def get_all_imos(cursor):
    query = f"SELECT DISTINCT IMO FROM event_log"
    imos = []

    res = cursor.execute(query)
    row = res.fetchone()
    while row:
        imos.append(row[0])

        row = res.fetchone()

    return imos


def get_data_events_report(cur, imo):
    query = f"SELECT " \
            f"  event_log.IMO, " \
            f"  event_log.LOCODE, " \
            f"  zones.LOCODE, " \
            f"  event_log.OLD_STATE, " \
            f"  event_log.STATE, " \
            f"  event_log.VALUE_INT," \
            f"  event_log.TSTAMP, " \
            f"  event_log.EVENT_TYPE," \
            f"  zones.TYPE " \
            f"FROM event_log " \
            f"LEFT JOIN zones ON event_log.VALUE_INT = zones.id " \
            f"WHERE " \
            f"  event_log.EVENT_TYPE IN ('ENTER_ZONE', 'EXIT_ZONE', 'STATE_CHANGED') AND " \
            f"  event_log.IMO == {imo} " \
            f"ORDER BY event_log.EVENT_TS"

    return cur.execute(query)


def create_graph(data):
    # initialize weighed graph
    weighed_graph_voyage = dict(dict())
    dict_stay = dict()

    # create weighed graph with dictionary representation
    prev_locode = ''
    prev_tst_voyage = 0

    curr_locode = ''
    curr_tst_voyage = 0

    has_long_stop = False
    long_stop_locode = ''

    # variables used for stay logic
    locode_stay = ''
    tst_stay = 0
    tst_last_long_stop_moving = 0

    row = data.fetchone()
    while row:
        # voyage logic
        if row[3] == 'LONG_STOP' or row[4] == 'LONG_STOP':
            has_long_stop = True
            long_stop_locode = row[1]

        if row[7] == 'EXIT_ZONE' and has_long_stop and long_stop_locode == row[1]:
            prev_locode = row[1]
            prev_tst_voyage = row[6]

        if row[7] == 'ENTER_ZONE' and prev_locode:
            if prev_locode != row[2] and row[1] != row[2]:
                curr_locode = row[2]
                curr_tst_voyage = row[6]

        if (row[3] == 'LONG_STOP' or row[4] == 'LONG_STOP') and curr_locode and curr_tst_voyage != 0:
            if curr_tst_voyage - prev_tst_voyage > 0:
                if prev_locode not in weighed_graph_voyage:
                    weighed_graph_voyage[prev_locode] = {}

                if curr_locode not in weighed_graph_voyage[prev_locode]:
                    weighed_graph_voyage[prev_locode][curr_locode] = Cell()

                weighed_graph_voyage[prev_locode][curr_locode].incr_counter()

                if 'all_arrivals' not in weighed_graph_voyage[prev_locode]:
                    weighed_graph_voyage[prev_locode]['all_arrivals'] = Cell()
                weighed_graph_voyage[prev_locode]['all_arrivals'].incr_counter()

            prev_locode = ''
            prev_tst_voyage = 0

            curr_locode = ''
            curr_tst_voyage = 0

            has_long_stop = True
            long_stop_locode = curr_locode

        # stay time logic
        if row[7] == 'ENTER_ZONE' and row[8] == 'PORT':
            locode_stay = row[2]

        if row[3] == 'NOT_MOVING' and row[4] == 'LONG_STOP' and locode_stay and tst_stay == 0:
            tst_stay = row[5]

        if row[3] == 'LONG_STOP' and row[4] == 'MOVING' and tst_stay != 0:
            tst_last_long_stop_moving = row[6]

        if row[7] == 'EXIT_ZONE' and tst_stay != 0:
            if locode_stay not in dict_stay:
                dict_stay[locode_stay] = Cell()

            if row[3] == 'LONG_STOP' and row[4] == 'LONG_STOP':
                dict_stay[locode_stay].add_stay_time(row[6] - tst_stay)
            else:
                if tst_last_long_stop_moving == 0:
                    dict_stay[locode_stay].add_stay_time(row[6] - tst_stay)
                else:
                    dict_stay[locode_stay].add_stay_time(tst_last_long_stop_moving - tst_stay)

            locode_stay = ''
            tst_stay = 0
            tst_last_long_stop_moving = 0

        row = data.fetchone()

    return weighed_graph_voyage, dict_stay


def insert_into_stay(data, cursor):
    if not data:
        return

    head = f"INSERT INTO {config['Table Names']['stay_time_table']} " \
           f"(imo, locode, stay_time, data_points, standard_dev) VALUES "

    values = ''
    cnt = 0
    for row in data:
        if cnt > 0:
            values += ', '
        values += f"({row[0]}, '{row[1]}', {row[2]}, {row[3]}, {row[4]})"
        cnt += 1

        if cnt == BULK_INSERT:
            cursor.execute(head + values)
            cnt = 0
            values = ''

    if cnt > 0:
        cursor.execute(head + values)


def insert_into_voyage(data, cursor):
    if not data:
        return

    head = f"INSERT INTO {config['Table Names']['next_port_percent']} " \
           f"(imo, from_locode, to_locode, percentage, data_points) VALUES "

    values = ''
    cnt = 0
    for row in data:
        if cnt > 0:
            values += ', '
        values += f"({row[0]}, '{row[1]}', '{row[2]}', {row[3]}, {row[4]})"
        cnt += 1

        if cnt == BULK_INSERT:
            cursor.execute(head + values)
            cnt = 0
            values = ''

    if cnt > 0:
        cursor.execute(head + values)


def calc_median_time(data):
    data = np.array(data)

    sd = np.std(data)
    lower_bound = np.median(data) - sd * SD_COEFFICIENT
    upper_bound = np.median(data) + sd * SD_COEFFICIENT
    data = data[(data >= lower_bound) &
                (data <= upper_bound)]

    avg = np.median(data)
    data_points = data.size

    if data.size < 2:
        return avg, data_points, int(np.std(data, ddof=0))
    return avg, data_points, int(np.std(data, ddof=1))


def get_data_for_database(imo, weighed_graph, dict_stay):
    data_stay = []
    data_voyage = []

    # create dataset for stay time
    for key in dict_stay:
        avg_stay_time, data_points_stay, sd_stay = calc_median_time(dict_stay[key].stay_time)
        data_stay.append((imo,
                          key,
                          int(avg_stay_time),
                          data_points_stay,
                          sd_stay))

    # create data set for voyage %
    for from_port in weighed_graph:
        for to_port in weighed_graph[from_port]:
            if to_port == 'all_arrivals':
                continue

            percentage = weighed_graph[from_port][to_port].count / weighed_graph[from_port]['all_arrivals'].count
            data_voyage.append((imo,
                                from_port,
                                to_port,
                                percentage,
                                weighed_graph[from_port][to_port].count))

    return data_stay, data_voyage


def create_sqlite_cursor():
    if (Path(config['Files']['data_file_name'])).exists():
        conn_sqlite = sqlite3.connect(database='file:' + config['Files']['data_file_name'], uri=True)
        cur_sqlite = conn_sqlite.cursor()
    else:
        raise FileNotFoundError('no database found')

    return conn_sqlite, cur_sqlite


def create_mysql_cursor():
    conn_mysql = mysql.connector.connect(user=config['Database Connection']['user'],
                                         password=config['Database Connection']['password'],
                                         host=config['Database Connection']['host'],
                                         database=config['Database Connection']['database'])
    cur_mysql = conn_mysql.cursor()

    return conn_mysql, cur_mysql


def divide_imos_per_core(imos, num_cores):
    imos_per_process = round(len(imos) / num_cores)

    imos_divided = []
    counter = 0
    for i in range(num_cores):
        if i < num_cores - 1:
            imos_divided.append(list(imos[counter:counter + imos_per_process]))
            counter += imos_per_process
        else:
            imos_divided.append(list(imos[counter:]))

    return imos_divided


def main_processing(imos):
    conn_sqlite, cur_sqlite = create_sqlite_cursor()
    conn_mysql, cur_mysql = create_mysql_cursor()

    cnt = 0

    data_bulk_insert_stay = []
    data_bulk_insert_voyage = []

    for imo in imos:
        data = get_data_events_report(cur_sqlite, imo)
        weighed_graph_voyage, dict_stay = create_graph(data)
        data_stay, data_voyage = get_data_for_database(imo, weighed_graph_voyage, dict_stay)

        # fill tables with data
        data_bulk_insert_stay += data_stay
        data_bulk_insert_voyage += data_voyage

        if len(data_bulk_insert_stay) >= BULK_INSERT:
            insert_into_stay(data_bulk_insert_stay, cur_mysql)
            data_bulk_insert_stay = []

        if len(data_bulk_insert_voyage) >= BULK_INSERT:
            insert_into_voyage(data_bulk_insert_voyage, cur_mysql)
            data_bulk_insert_voyage = []

        cnt += 1
        print(cnt)

    if len(data_bulk_insert_stay) > 0:
        insert_into_stay(data_bulk_insert_stay, cur_mysql)

    if len(data_bulk_insert_voyage) > 0:
        insert_into_voyage(data_bulk_insert_voyage, cur_mysql)

    conn_sqlite.commit()
    cur_sqlite.close()
    conn_sqlite.close()

    conn_mysql.commit()
    cur_mysql.close()
    conn_mysql.close()


if __name__ == '__main__':
    num_cores = os.cpu_count()

    conn_sqlite, cur_sqlite = create_sqlite_cursor()

    imos = get_all_imos(cur_sqlite)

    cur_sqlite.close()
    conn_sqlite.close()

    imos_divided = divide_imos_per_core(imos, num_cores)

    for i in range(num_cores):
        process = Process(target=main_processing, args=((imos_divided[i],)))

        process.start()
