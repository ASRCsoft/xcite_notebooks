"""Converting data to netcdf

- Reading yesterday's data from lidar/mwr csv files
- Writing them to netCDF files

Most of this code is just navigating through files. Converting to
netcdf (the `process_lidar` function) is the easy part.

"""

import os, re
import datetime as dt
import numpy as np
import pandas as pd
import rasppy.convert as rasp
from sqlalchemy import create_engine

# setting up the postgres connection
engine = create_engine('postgresql:///files')

# resample argument
period = '5T'

# path to the new files
# path = '/home/xcite/netcdf/data/'
path = '/farm1/mesonet/data/'

# helpful functions
def make_path(instr, site, date):
    return path + site + date.strftime('/%Y/%m/')

def process_lidar(radial_file, scan_file, wind_file, site, period, netcdf_path):
    """Reorganize xarray object a bit for netcdf files"""
    lidar = rasp.lidar_from_csv(radial_file, scan_file, wind=wind_file)
    # remove status==0 data (if we have the whole data)
    if 'Status' in lidar.data_vars:
        lidar['CNR'] = lidar['CNR'].where(lidar['Status'])
        lidar['DRWS'] = lidar['DRWS'].where(lidar['Status'])
    # remove unneeded variables if they exist
    to_drop = list(set(lidar.data_vars) & set(['Status', 'Error', 'Confidence', 'RWS']))
    lidar = lidar.drop(to_drop)
    lidar = lidar.rasp.cf_compliant()
    lidar.to_netcdf(netcdf_path)

def process_mwr(lv2_file, site):
    mwr = rasp.mwr_from_csv(lv2_file, resample=period)
    mwr = mwr.sel(**{'LV2 Processor': 'Zenith'}).drop('LV2 Processor')
    mwr.coords['hpascals'] = ('Range', 1013.25 * np.exp(-mwr.coords['Range'] / 7))
    mwr['cape'] = mwr.rasp.estimate_cape()
    mwr_nc = app_base + '_'.join([site, 'mwr.nc'])
    mwr.to_netcdf(mwr_nc)

# get only the files that don't have netcdf files already
q1 = 'select lidar_csv.* from lidar_csv left join lidar_netcdf on lidar_csv.site=lidar_netcdf.site and lidar_csv.date=lidar_netcdf.date where netcdf is null order by lidar_csv.date'
lidar_files = pd.read_sql(q1, con=engine)


lidar_dates = set(lidar_files['date'])
# mwr_files = pd.read_sql("select * from mwr where date(time)='%s'" % yesterday, con=engine)
# mwr_sites = set(mwr_files['site'].str.replace(r'CESTM_roof.*', 'CESTM_roof'))
# sites = lidar_sites | mwr_sites
# print(lidar_dates)

# prepare the folders

lidar_netcdf = 'lidar_netcdf'
lidar_path = path + lidar_netcdf + '/'
# create folder if needed
if lidar_netcdf not in os.listdir(path):
    os.mkdir(lidar_path)
# start_date = dt.date(2016, 4, 12) # when to start
for date in sorted(lidar_dates):
    # if date < start_date:
    #     continue
    lidars_on_date = lidar_files[lidar_files['date'] == date]
    sites = lidars_on_date['site']
    lidars_on_date.set_index('site', inplace=True)
    for site in sites:
        # print the site and date, for helpful logging
        print(', '.join([site, str(date)]))
        scan_file = lidars_on_date.loc[site].scan
        rws_file = lidars_on_date.loc[site].whole
        wind_file = lidars_on_date.loc[site].wind
        # use the radial wind file if the whole wind file isn't
        # available
        if rws_file is None:
            rws_file = lidars_on_date.loc[site].radial
        # create folders if needed, construct the path to the netcdf
        # file
        site_path = lidar_path + site + '/'
        if site not in os.listdir(lidar_path):
            os.mkdir(site_path)
        year = str(date.year)
        year_path = site_path + year + '/'
        if year not in os.listdir(site_path):
            os.mkdir(year_path)
        month = date.strftime('%m')
        month_path = year_path + month + '/'
        if month not in os.listdir(year_path):
            os.mkdir(month_path)
        netcdf_file = '_'.join([date.strftime('%Y%m%d'), site, 'lidar.nc'])
        netcdf_path = month_path + netcdf_file
        # make a data file, hopefully
        try:
            process_lidar(rws_file, scan_file, wind_file, site, period, netcdf_path)
        except rasp.MultipleScansException:
            # print('Multiple scans. Bah humbug.')
            pass
        except rasp.NoScansException:
            # print('No scans. Who cares.')
            pass
        except Exception as e:
            message = site + ', ' + str(date) + ': ' + str(e)
            print(message)
            

# for site in sites:
#     print(site)

#     # write the data to netcdf
#     if site in lidar_sites:
#         # fix CESTM site if needed
#         if site == 'CESTM_roof':
#             lidar_site = 'CESTM_roof-14'
#         else:
#             lidar_site = site
#             # get data files
#         scan_file = lidar_files.loc[lidar_site].scan
#         rws_file = lidar_files.loc[lidar_site].whole
#         if rws_file is None:
#             rws_file = lidar_files.loc[lidar_site].radial
#             # write netcdf file
#         try:
#             process_lidar(rws_file, scan_file, site)
#         except:
#             print('no luck-- ' + site + ' lidar')
            
#     if site in mwr_sites:
#         if site == 'CESTM_roof':
#             mwr_site = 'CESTM_roof-3223'
#         else:
#             mwr_site = site
#             lv2_files = mwr_files[mwr_files.site == mwr_site].lv2
#             lv2_file = lv2_files.iloc[0] # <-- fix this later!!
#         if lv2_file is not None:
#             try:
#                 process_mwr(lv2_file, site)
#             except:
#                 print('no luck-- ' + site + ' mwr')
