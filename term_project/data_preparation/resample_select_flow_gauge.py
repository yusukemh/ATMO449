import pandas as pd
base_dir = f'/mnt/lustre/koa/class/atmo449_class/students/team_1_flood_risk/'
from tqdm import tqdm
import os


def resample_filter(station_id):
    save_filename = f"{base_dir}/preprocessed_data/selected_flowgauge_15mins/{station_id}.csv"
    if os.path.exists(save_filename):
        print(f'File Already Exists. Skipping: {station_id}')
        return
    df_gauge = pd.read_csv(
        f"{base_dir}/raw_data/gauge_observations/{station_id}.csv", dtype={'measurement': str}
        ).query('DQF == "A"').drop(columns=['DQF', 'station_id'])
    df_gauge['hst_timestamp'] = pd.to_datetime(df_gauge['hst_timestamp'])
    df_gauge['measurement'] = df_gauge['measurement'].astype(float)

    # drop any measurements taken at timestamps not divisible by 5 mins.
    df_gauge['is_divisible_by_5mins'] = (df_gauge['hst_timestamp'] - df_gauge['hst_timestamp'].dt.floor(freq='5min')) == pd.Timedelta(seconds=0)
    if len(df_gauge) == 0:
        print(f"No data. Skipping {station_id}")
        return
    if ~df_gauge['is_divisible_by_5mins'].sum() > 10:
        print(f"More than 10 timestamps at non-5-min-resolution timestamps. Skipping {station_id}")
        return
    df_gauge_5min_resolution = df_gauge[df_gauge['is_divisible_by_5mins']].drop(columns=['is_divisible_by_5mins'])

    df_gauge_5min_resolution['interval'] = df_gauge_5min_resolution['hst_timestamp'] - df_gauge_5min_resolution['hst_timestamp'].shift(1)
    assert df_gauge_5min_resolution['interval'].min() >= pd.Timedelta(minutes=5)
    df_resampled = df_gauge_5min_resolution.resample('15min', on='hst_timestamp').mean().dropna().drop(columns=['interval'])
    
    # Calculate the data coverage and save only if enough coverage
    start, end = pd.Timestamp('2008-01-01', tz="HST"), pd.Timestamp('2024-12-31', tz="HST")
    df_resampled_filtered = df_resampled[(df_resampled.index >= start) & (df_resampled.index <= end)]

    recovery_rate = df_resampled_filtered.shape[0] / pd.date_range(start, end, freq='15min').shape[0]
    if recovery_rate > 0.85:
        print(f"Saving data for {station_id}")
        df_resampled_filtered.to_csv(save_filename)
    else:
        print(f"Coverage is low: {recovery_rate:.3f}. Skipping {station_id}")

def main():
    df_metadata = pd.read_csv(f'{base_dir}/raw_data/station_metadata.csv')
    for station_id in tqdm(df_metadata['station_id']):
        resample_filter(station_id)

if __name__ == '__main__':
    main()