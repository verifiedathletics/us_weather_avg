import pandas as pd
from numpy import cos, sin, arcsin, sqrt
from math import radians
from tqdm import tqdm

temp = pd.read_csv('Temperature Data.csv')
pop = pd.read_csv('Population Data.csv')
ws = pd.read_csv('weather_station_map.csv')
ws_list = ['KATL', 'KBDL', 'KBNA',
       'KBOI', 'KBOS', 'KBUF', 'KBUR', 'KBWI', 'KCMH', 'KCQT', 'KCVG', 'KDCA',
       'KDEN', 'KDFW', 'KDTW', 'KFAT', 'KGEG', 'KIAD', 'KIAH', 'KLAS', 'KLGA',
       'KLIT', 'KMEM', 'KMSP', 'KMSY', 'KORD', 'KPDX', 'KPHL', 'KPHX', 'KPIT',
       'KPWM', 'KRDU', 'KRIC', 'KSAC', 'KSEA', 'KSFO', 'KSLC', 'KSTL', 'KALB']
# Find closest station (mix of stations)

def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * arcsin(sqrt(a))
    km = 6367 * c
    return km

rows = []
for index, row in tqdm(pop.iterrows()):
    x = 0
    for station_code in ws['station_code'].to_list():
        row[station_code] = haversine(row['Lon'], row['Lat'], ws.loc[ws['station_code'] == station_code, 'Lon'], ws.loc[ws['station_code'] == station_code, 'Lat'])
        if row['Lon'] > ws.loc[ws['station_code'] == station_code, 'Lon'][x]:
            row[f'{station_code}_ew_dir'] = 1
        else:
            row[f'{station_code}_ew_dir'] = 2
        if row['Lat'] > ws.loc[ws['station_code'] == station_code, 'Lat'][x]:
            row[f'{station_code}_ns_dir'] = 3
        else:
            row[f'{station_code}_ns_dir'] = 4
            # 1 = E, #2 = W, #3 = N, #4 = S
        x = x + 1
    rows.append(row)
pop_enhanced = pd.DataFrame(rows)
pop_enhanced['closest_1'] = pop_enhanced[ws_list].idxmin(axis=1)
pop_enhanced['closest_1_dist'] = pop_enhanced.lookup(pop_enhanced.index, pop_enhanced['closest_1'])
pop_enhanced['closest_1_ew'] = pop_enhanced.lookup(pop_enhanced.index, pop_enhanced['closest_1']+'_ew_dir')
pop_enhanced['closest_1_ns'] = pop_enhanced.lookup(pop_enhanced.index, pop_enhanced['closest_1']+'_ns_dir')
pop_enhanced = pop_enhanced.sort_values('closest_1_dist').reset_index(drop=True)
for y in range(0, len(pop_enhanced)):
    pop_enhanced[pop_enhanced['closest_1'][y]][y] = 9999999999
pop_enhanced['closest_2'] = pop_enhanced[ws_list].idxmin(axis=1)
pop_enhanced['closest_2_dist'] = pop_enhanced.lookup(pop_enhanced.index, pop_enhanced['closest_2'])
pop_enhanced['closest_2_ew'] = pop_enhanced.lookup(pop_enhanced.index, pop_enhanced['closest_2']+'_ew_dir')
pop_enhanced['closest_2_ns'] = pop_enhanced.lookup(pop_enhanced.index, pop_enhanced['closest_2']+'_ns_dir')
pop_enhanced['closest_1_perc'] = 1
pop_enhanced['closest_2_perc'] = 0
pop_enhanced.loc[(pop_enhanced['closest_1_dist'] > 200) & ((pop_enhanced['closest_1_ew'] != pop_enhanced['closest_2_ew']) & (pop_enhanced['closest_1_ns'] != pop_enhanced['closest_2_ns'])), 'closest_1_perc'] = pop_enhanced['closest_2_dist'] / (pop_enhanced['closest_1_dist'] + pop_enhanced['closest_2_dist'])
pop_enhanced.loc[(pop_enhanced['closest_1_dist'] > 200) & ((pop_enhanced['closest_1_ew'] != pop_enhanced['closest_2_ew']) & (pop_enhanced['closest_1_ns'] != pop_enhanced['closest_2_ns'])), 'closest_2_perc'] = pop_enhanced['closest_1_dist'] / (pop_enhanced['closest_1_dist'] + pop_enhanced['closest_2_dist'])

# get us pop weighted weather stations

pop_enhanced['closest_1_pop'] = pop_enhanced['population'] * pop_enhanced['closest_1_perc']
pop_enhanced['closest_2_pop'] = pop_enhanced['population'] * pop_enhanced['closest_2_perc']
pop_enhanced['closest_1_pop'] = pop_enhanced['closest_1_pop'].fillna(0)
pop_enhanced['closest_2_pop'] = pop_enhanced['closest_2_pop'].fillna(0)
for station_code in ws['station_code'].to_list():
    try:
        ws.loc[ws['station_code'] == station_code, 'weighted_pop'] = pop_enhanced.groupby('closest_1').sum()['closest_1_pop'][station_code] + pop_enhanced.groupby('closest_1').sum()['closest_1_pop'][station_code]
    except:
        ws.loc[ws['station_code'] == station_code, 'weighted_pop'] = 0
total_pop = sum(ws['weighted_pop'])
ws['weighted_pop'] = ws['weighted_pop'] / total_pop


# clean time series for temps
temp['location_date'] = pd.to_datetime(temp['location_date'])
temp['year'] = pd.DatetimeIndex(temp['location_date']).year
check = temp.groupby(['year', 'station_code']).count()  # shows that it is mostly full

new_temp_mean = temp.pivot(index='location_date', columns='station_code', values='temp_mean_c')
new_temp_mean_fix = new_temp_mean.copy()
for station_code in ws['station_code'].to_list():
    for z in range(0, len(new_temp_mean_fix)):
        if pd.isna(new_temp_mean_fix[station_code][z]):
            if z == len(new_temp_mean_fix) - 1:
                new_temp_mean_fix[station_code][z] = new_temp_mean_fix[station_code][z - 1]
            else:
                new_temp_mean_fix[station_code][z] = (new_temp_mean_fix[station_code][z+1] + new_temp_mean_fix[station_code][z-1]) / 2
new_temp_mean_fix['pop_weighted_avg_mean'] = 0
for station_code in ws['station_code'].to_list():
    new_temp_mean_fix['pop_weighted_avg_mean'] = new_temp_mean_fix['pop_weighted_avg_mean'] + new_temp_mean_fix[station_code] * ws.loc[ws['station_code'] == station_code,'weighted_pop'].reset_index(drop=True)[0]
    
new_temp_min = temp.pivot(index='location_date', columns='station_code', values='temp_min_c')
new_temp_min_fix = new_temp_min.copy()
for station_code in ws['station_code'].to_list():
    for z in range(0, len(new_temp_min_fix)):
        if pd.isna(new_temp_min_fix[station_code][z]):
            if z == len(new_temp_min_fix) - 1:
                new_temp_min_fix[station_code][z] = new_temp_min_fix[station_code][z - 1]
            else:
                new_temp_min_fix[station_code][z] = (new_temp_min_fix[station_code][z+1] + new_temp_min_fix[station_code][z-1]) / 2
new_temp_min_fix['pop_weighted_avg_min'] = 0
for station_code in ws['station_code'].to_list():
    new_temp_min_fix['pop_weighted_avg_min'] = new_temp_min_fix['pop_weighted_avg_min'] + new_temp_min_fix[station_code] * ws.loc[ws['station_code'] == station_code,'weighted_pop'].reset_index(drop=True)[0]
    
new_temp_max = temp.pivot(index='location_date', columns='station_code', values='temp_max_c')
new_temp_max_fix = new_temp_max.copy()
for station_code in ws['station_code'].to_list():
    for z in range(0, len(new_temp_max_fix)):
        if pd.isna(new_temp_max_fix[station_code][z]):
            if z == len(new_temp_max_fix) - 1:
                new_temp_max_fix[station_code][z] = new_temp_max_fix[station_code][z - 1]
            else:
                new_temp_max_fix[station_code][z] = (new_temp_max_fix[station_code][z+1] + new_temp_max_fix[station_code][z-1]) / 2
new_temp_max_fix['pop_weighted_avg_max'] = 0
for station_code in ws['station_code'].to_list():
    new_temp_max_fix['pop_weighted_avg_max'] = new_temp_max_fix['pop_weighted_avg_max'] + new_temp_max_fix[station_code] * ws.loc[ws['station_code'] == station_code,'weighted_pop'].reset_index(drop=True)[0]

final_temp_table = pd.concat([new_temp_mean_fix['pop_weighted_avg_mean'], new_temp_min_fix['pop_weighted_avg_min'], new_temp_max_fix['pop_weighted_avg_max']],axis=1)

final_temp_table_seasonal = final_temp_table.copy()
final_temp_table_seasonal['year'] = pd.DatetimeIndex(final_temp_table_seasonal.index).year
final_temp_table_seasonal['month'] = pd.DatetimeIndex(final_temp_table_seasonal.index).month
final_temp_table_seasonal['day'] = pd.DatetimeIndex(final_temp_table_seasonal.index).day
final_temp_table_seasonal_group = final_temp_table_seasonal.groupby(['month', 'day']).mean().reset_index()
final_temp_table_seasonal_group.columns = final_temp_table_seasonal_group.columns + '_seasonal'
final_temp_table_seasonal = final_temp_table_seasonal.merge(final_temp_table_seasonal_group, how='left', left_on=['month', 'day'], right_on=['month_seasonal', 'day_seasonal'])
final_temp_table_seasonal['pop_weighted_avg_mean_diff'] = final_temp_table_seasonal['pop_weighted_avg_mean'] - final_temp_table_seasonal['pop_weighted_avg_mean_seasonal']
final_temp_table_seasonal['pop_weighted_avg_min_diff'] = final_temp_table_seasonal['pop_weighted_avg_min'] - final_temp_table_seasonal['pop_weighted_avg_min_seasonal']
final_temp_table_seasonal['pop_weighted_avg_max_diff'] = final_temp_table_seasonal['pop_weighted_avg_max'] - final_temp_table_seasonal['pop_weighted_avg_max_seasonal']
final_temp_table_seasonal = final_temp_table_seasonal[['pop_weighted_avg_mean_diff', 'pop_weighted_avg_min_diff', 'pop_weighted_avg_max_diff']]

final_temp_table_monthly = final_temp_table.copy()
final_temp_table_monthly['year'] = pd.DatetimeIndex(final_temp_table_monthly.index).year
final_temp_table_monthly['month'] = pd.DatetimeIndex(final_temp_table_monthly.index).month
final_temp_table_monthly = final_temp_table_monthly.groupby(['year', 'month']).mean()

final_temp_table_monthly_seasonal = final_temp_table_monthly.copy()
final_temp_table_monthly_seasonal = final_temp_table_monthly_seasonal.reset_index()
final_temp_table_monthly_seasonal_group = final_temp_table_monthly_seasonal.groupby(['month']).mean().reset_index()
final_temp_table_monthly_seasonal_group.columns = final_temp_table_monthly_seasonal_group.columns + '_seasonal'
final_temp_table_monthly_seasonal = final_temp_table_monthly_seasonal.merge(final_temp_table_monthly_seasonal_group, how='left', left_on=['month'], right_on=['month_seasonal'])
final_temp_table_monthly_seasonal['pop_weighted_avg_mean_diff'] = final_temp_table_monthly_seasonal['pop_weighted_avg_mean'] - final_temp_table_monthly_seasonal['pop_weighted_avg_mean_seasonal']
final_temp_table_monthly_seasonal['pop_weighted_avg_min_diff'] = final_temp_table_monthly_seasonal['pop_weighted_avg_min'] - final_temp_table_monthly_seasonal['pop_weighted_avg_min_seasonal']
final_temp_table_monthly_seasonal['pop_weighted_avg_max_diff'] = final_temp_table_monthly_seasonal['pop_weighted_avg_max'] - final_temp_table_monthly_seasonal['pop_weighted_avg_max_seasonal']
final_temp_table_monthly_seasonal = final_temp_table_monthly_seasonal[['pop_weighted_avg_mean_diff', 'pop_weighted_avg_min_diff', 'pop_weighted_avg_max_diff']]



md_new_temp_max = new_temp_max.add_suffix('_max')
md_new_temp_min = new_temp_min.add_suffix('_min')
md_new_temp_mean = new_temp_mean.add_suffix('_mean')
missing_data = new_temp_max.copy().reset_index()
missing_data = pd.melt(missing_data, id_vars='location_date')
missing_data = missing_data[pd.isna(missing_data['value'])]
missing_data['year'] = pd.DatetimeIndex(missing_data['location_date']).year
missing_data['month'] = pd.DatetimeIndex(missing_data['location_date']).month
missing_data['day'] = pd.DatetimeIndex(missing_data['location_date']).day
missing_data_simple = missing_data.groupby(['station_code', 'year', 'month', 'day']).count()[['location_date']].rename(columns={'location_date': 'count'})
missing_data_station = missing_data.groupby(['station_code']).count()[['location_date']].rename(columns={'location_date': 'count'})
missing_data_year = missing_data.groupby(['year']).count()[['location_date']].rename(columns={'location_date': 'count'})
missing_data_month = missing_data.groupby(['month']).count()[['location_date']].rename(columns={'location_date': 'count'})
missing_data_day = missing_data.groupby(['year', 'month', 'day']).count()[['location_date']].rename(columns={'location_date': 'count'})

plot = final_temp_table.plot(grid=True, title='full time series')
plot.set_xlabel('date')
plot.set_ylabel('temp')
plot2 = final_temp_table_seasonal.plot(grid=True, title='seasonal time series')
plot2.set_xlabel('date')
plot2.set_ylabel('temp diff from annual average')
plot3 = final_temp_table_monthly.plot(grid=True, title='monthly time series')
plot3.set_xlabel('month')
plot3.set_ylabel('temp')
plot4 = final_temp_table_monthly_seasonal.plot(grid=True, title='seasonal monthly time series')
plot4.set_xlabel('month')
plot4.set_ylabel('temp diff from annual average')
plot5 = missing_data_day.plot.bar(grid=True, title='days with missing data')
plot5.set_ylabel('number of stations with missing data')
plot6 = missing_data_station.plot.bar(grid=True, title='stations with missing data')
plot6.set_ylabel('number of days with missing data')
