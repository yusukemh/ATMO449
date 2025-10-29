import netCDF4 as nc
import xarray as xr
import numpy as np
import argparse
import os
base_dir = f'/mnt/lustre/koa/class/atmo449_class/students/team_1_flood_risk/'

def process_year(year):
    filename = f"{base_dir}/preprocessed_data/hourly_ERA5/ERA5_{year}.nc"
    if os.path.exists(filename): return
    
    ds = xr.open_dataset(f"{base_dir}/raw_data/ERA5_1974_2025_hourly_pr.nc", decode_timedelta=True) 
    ds = ds.drop_duplicates('time').drop_vars(['number', 'valid_time', 'surface'])
    # ds = ds.sel(latitude=hawaii_lat_bound, longitude=hawaii_lon_bound)

    year_ds = ds.sel(time=slice(f'{year-1}-12-31 12:00:00', f'{year+1}-01-01 12:00:00'))# give buffer
    all_times = np.concatenate([(t + year_ds.step.values) for t in year_ds.time.values])

    lat, lon = ds.latitude, ds.longitude
    # Create an empty dataset with matching vars
    template = xr.Dataset(
        {
            var: (("time", "latitude", "longitude"),
                np.full((len(all_times), len(lat), len(lon)), np.nan, dtype=ds[var].dtype))
            for var in ds.data_vars
        },
        coords={"time": all_times, "latitude": lat, "longitude": lon}
    ).sel(time=slice(f"{year}-01-01 00:00", f"{year}-12-31 23:00")) # filter out the buffer

    template.to_netcdf(filename, mode='w')

    root = nc.Dataset(filename, mode="r+")

    for t in year_ds.time.values:
        sub = ds.sel(time=t)
        sub_time = t + sub.step
        sub = sub.assign_coords(time=sub_time).swap_dims({"step": "time"}).drop_vars("step")
        sub = sub.sel(time=slice(f"{year}-01-01 00:00", f"{year}-12-31 23:00")) # filter out the buffer

        # Find the matching indices in the full timeline
        idx = np.searchsorted(template.time, sub.time.values)
        if len(idx) == 0: continue
        for var in ds.data_vars:
            root[var][idx, :, :] = sub[var].values
    root.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', '-y', type=int, required=True)
    args = parser.parse_args()
    process_year(args.year)


if __name__ == '__main__':
    main()