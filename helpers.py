from datetime import timedelta
import pyspark.sql.functions as func
import numpy as np
import pandas as pd
import pickle


def euclidean_dist(row1, row2, col1r1, col2r1, col1r2, col2r2):
    dx = row1[col1r1] - row2[col1r2]
    dy = row1[col2r1] - row2[col2r2]
    return np.sqrt(dx*dx + dy*dy)

def euclidean_dist(x1, y1, x2, y2):
    dx = x1 - x2
    dy = y1 - y2
    return np.sqrt(dx*dx + dy*dy)


def columns_slice(df, start, stop):
    df.select([c for c in df.columns[start:stop]]).show(5)


# builds path for file given date as parameter
def format_path(path, year, month, day):
        d = day
        m = month
        if d < 10:
            d = '0' + str(d)
        if m < 10:
            m = '0' + str(m)
        return path.format(year, m, year, m, d)


# imports files for given day and the day following the parameter given
# returns rdd
def import_today_tomorrow(today, spark):
    dummy_path = '/datasets/sbb/{}/{}/{}-{}-{}istdaten.csv.bz2'
    tomorrow = today + timedelta(days=1)
    path_today = format_path(dummy_path, today.year, today.month, today.day)
    path_tomorrow = format_path(dummy_path, tomorrow.year, tomorrow.month, tomorrow.day)

    raw = spark.read.load([path_today, path_tomorrow],
                          format='csv',
                          header=True,
                          sep=';')
    return raw

# new names for the existing features
new_columns = ['trip_date', 'trip_id', 'op_id', 'op_abk', 'op_name', 'transport_type', 'train_number', 'service_type_1',\
               'umlauf_id', 'service_type_2', 'is_additional', 'trip_failed', 'bpuic', 'stop_name', 'sch_arr_time', \
              'real_arr_time', 'on_time_arr', 'sch_dep_time', 'real_dep_time', 'on_time_dep', 'stop_at_station']

def print_dict_entry_i(d, i):
    key = list(d.keys())[i]
    print('{} : {}'.format(key, d[key]))
    
def get_maps(stop_bpuic_list):
    stop_bpuic_map = {}
    bpuic_stop_map = {}
    bpuic_index_map = {}
    index_bpuic_map = {}
    for elem in stop_bpuic_list:
        if elem.stop_name not in stop_bpuic_map and elem.bpuic not in bpuic_stop_map:
            stop_bpuic_map[elem.stop_name] = elem.bpuic
            bpuic_stop_map[elem.bpuic] = elem.stop_name
        
    for i, elem in enumerate(bpuic_stop_map):
        bpuic_index_map[elem] = i
        index_bpuic_map[i] = elem
    return stop_bpuic_map, bpuic_stop_map, bpuic_index_map, index_bpuic_map

def find_connections(zip_list, id_):
    t_connection = []
    for i in range(len(zip_list)-1):
        t_connection.append((zip_list[i][1], zip_list[i+1][1], zip_list[i][0], zip_list[i+1][2], id_))
    return t_connection

def load_connections_to_pickle(dep_name, arr_name, df):
    sample_by_id = df.select('trip_id','bpuic','sch_dep_time','sch_arr_time')\
        .groupBy('trip_id')\
        .agg(func.collect_list('sch_dep_time').alias('dep'), func.collect_list('bpuic').alias('bpuic'),\
            func.collect_list('sch_arr_time').alias('arr'), df.trip_id.alias('trip_id2'))

    connections = map(lambda l: find_connections(l[0],l[1]), sample_by_id.rdd.map(lambda x: \
                                                            (sorted(zip(x.dep, x.bpuic, x.arr)),x.trip_id2)).collect())

    connections_list = [val for sub in list(connections) for val in sub]
    connections_dep_sorted = connections_list[:]
    connections_arr_sorted = connections_list[:]
    connections_dep_sorted.sort(key = lambda x: x[2])
    connections_arr_sorted.sort(key = lambda x: x[3])

    save_pkl(connections_dep_sorted, "{}".format(dep_name))
    save_pkl(connections_arr_sorted[::-1], "{}".format(arr_name))

def save_pkl(d, name):
    with open('{}.pkl'.format(name), 'wb') as handle:
        pickle.dump(d, handle, protocol=pickle.HIGHEST_PROTOCOL)

def load_pkl(name):
    with open('{}.pkl'.format(name), 'rb') as handle:
        return pickle.load(handle)
