# coding=gbk
from __init__ import *

class ModisDownload:

    def __init__(self):
        self.__conf__()
        self.product = self.product_url.split('/')[-2]
        self.download_dir = self.download_path + 'HDF\\{}\\'.format(self.product)
        self.urls_dir = self.download_path + 'urls\\{}\\'.format(self.product)

        T.mk_dir(self.download_dir, force=True)
        T.mk_dir(self.urls_dir, force=True)
        pass

    def __conf__(self):
        '''
        tips: disable IPV6
        configuration
        '''
        ####### 下载路径 #######
        self.download_path = r'F:\modis_download\\'

        ####### 下载区域 #######
        self.region_shp = r'F:\shp\china.shp'

        ####### 要下载的产品地址 #######
        # self.product_url = 'https://e4ftl01.cr.usgs.gov/MOLT/MOD17A2H.006/'  # GPP
        self.product_url = 'https://e4ftl01.cr.usgs.gov/MOLT/MOD13A3.006/'  # NDVI

        ####### 开始和结束的日期 #######
        self.date_start = [2000, 2]  # 包含
        self.date_end = [2020, 5]  # 不包含

        ####### 并行下载线程数 #######
        self.thread = 10
        self.thread_url = 10
        # 并行数量太大容易被服务器拒绝

        ####### 账号密码 #######
        self.username = 'xxxxxx'
        self.password = 'xxxxxx'
        pass

    def run(self):
        # # 1. 获取要下载的hdf的url
        self.get_hdf_urls()
        # # 2. 下载
        self.download_hdf()


    def shp_to_Sinusoidal_tiles(self):
        daShapefile = self.region_shp
        driver = ogr.GetDriverByName('ESRI Shapefile')
        dataSource = driver.Open(daShapefile, 0)
        layer = dataSource.GetLayer(0)
        extent = layer.GetExtent()
        lon_start,lon_end,lat_start,lat_end = extent
        lon_list = np.linspace(lon_start,lon_end,36)
        lat_list = np.linspace(lat_start,lat_end,18)
        # print lon_list
        # print lat_list
        lon_lat_list = []
        for lon in lon_list:
            for lat in lat_list:
                lon_lat = [lon,lat]
                lon_lat_list.append(lon_lat)
        tiles = []
        for lon_lat in lon_lat_list:
            lon,lat = lon_lat
            vh = self.lon_lat_to_Sinusoidal_tiles(lon,lat)
            for v,h in vh:
                tiles.append((int(v),int(h)))
        tiles = set(tiles)
        print 'selected tiles:',list(tiles)
        return tiles
        # return (6, 27), (6, 28), (5, 28), (5, 27)

    def lon_lat_to_Sinusoidal_tiles(self,lon,lat):
        '''
        Code from https://www.earthdatascience.org/tutorials/convert-modis-tile-to-lat-lon/
        modis_tiles.txt originates from https://modis-land.gsfc.nasa.gov/pdf/sn_bound_10deg.txt
        :param lon: longitude
        :param lat: latitude
        :return: tiles: v and h
        '''
        data = np.genfromtxt('modis_tiles.txt',
                             skip_header=7,
                             skip_footer=3)

        vh = []
        for i in data:
            if lat >= i[4] and lat <= i[5] and lon >= i[2] and lon <= i[3]:
                vert = i[0]
                horiz = i[1]
                vh.append([vert,horiz])
        # print vh

        return vh


    def get_product_dates(self):
        '''
        :param product_url:'https://e4ftl01.cr.usgs.gov/MOLT/MOD17A2H.006/
        :return:
        '''
        print 'fetching dates...'
        product_url = self.product_url
        request = urllib2.Request(product_url)
        response = urllib2.urlopen(request)
        body = response.read()
        p = re.findall('/">.*?/</a>', body)
        date = []

        flag = 0
        for i in p:
            flag += 1
            date.append(i[3:-5])

        return date


    def pick_date(self,start,end):
        '''
        :param start: [yyyy,mm] 含
        :param end: [yyyy,mm] 不含
        :return:
        '''
        s_year,s_mon = start
        e_year,e_mon = end
        avail_date = self.get_product_dates()
        init_date = datetime.datetime(s_year,s_mon,1)
        end_date = datetime.datetime(e_year,e_mon,1)
        delta_day = end_date-init_date
        delta_day = delta_day.days
        date_list = []
        for d in range(delta_day):
            date_delta = datetime.timedelta(d)
            date = init_date+date_delta
            year,mon,day = date.year,date.month,date.day
            date_str = '{:d}.{:02d}.{:02d}'.format(year,mon,day)
            date_list.append(date_str)

        picked_date = []
        for d in date_list:
            if d in avail_date:
                picked_date.append(d)

        return picked_date

    def kernel_get_hdf_urls(self,params):
        dates,ii,date_urls = params
        url_text_file = self.urls_dir + dates[ii] + '.txt'
        if os.path.isfile(url_text_file):
            return None
        try:
            request = urllib2.Request(date_urls[ii])
            response = urllib2.urlopen(request)
            body = response.read()
            # print body
            p1 = re.findall('">.*?">', body)
            temp = []
            fw = open(url_text_file, 'w')
            for pi in p1:
                if '.hdf' in pi and not '.xml' in pi:
                    # if 0:
                    fname = pi[12:-2]
                    download_url = date_urls[ii] + fname
                    fw.write(download_url + '\n')
                    temp.append(download_url)
            fw.close()
        except Exception as e:
            time.sleep(5)
            print('sleep 5 seconds')
            print 'x' * 30
            print e

        pass


    def get_hdf_urls(self):
        '''
        :return:
        '''
        start = self.date_start
        end = self.date_end
        picked_date = self.pick_date(start,end)

        date_urls = []
        dates = []
        for date in picked_date:
            url = self.product_url+date+'/'
            date_urls.append(url)
            dates.append(date)

        params = []
        for ii in range(len(dates)):
            params.append([dates,ii,date_urls])

        M(self.kernel_get_hdf_urls,params).run(process=self.thread_url,process_or_thread='t',desc='fetching urls...')
        pass


    def kernel_download_hdf(self,url):
        '''
        :param urls: url 'https://e4ftl01.cr.usgs.gov/MOLT/MOD17A2H.006/2000.05.16/MOD17A2H.A2000137.h21v09.006.2015139130317.hdf'
        '''
        username = self.username
        password = self.password

        password_manager = urllib2.HTTPPasswordMgrWithDefaultRealm()
        password_manager.add_password(None, "https://urs.earthdata.nasa.gov", username, password)
        cookie_jar = CookieJar()
        opener = urllib2.build_opener(
            urllib2.HTTPBasicAuthHandler(password_manager),
            # urllib2.HTTPHandler(debuglevel=1),    # Uncomment these two lines to see
            # urllib2.HTTPSHandler(debuglevel=1),   # details of the requests/responses
            urllib2.HTTPCookieProcessor(cookie_jar))
        urllib2.install_opener(opener)
            # print 'downloading',url
        fname = url.split('/')[-1]
        save_path = self.download_dir + url.split('/')[-2] + '/'
        attempts = 0
        while 1:
            success = 0
            try:
                if not os.path.isfile(save_path + fname):
                    request = urllib2.Request(url)
                    response = urllib2.urlopen(request)
                    body = response.read()
                    with open(save_path + fname, 'wb') as f:
                        f.write(body)
                    # print url, 'done'
                    success = 1
                else:
                    print fname, 'is already existed'
                    success = 1
            except Exception as e:
                time.sleep(5)
                print 'sleep 5 seconds'
                print 'x' * 30
                print e
                attempts += 1
                print 'try ', attempts
                success = 0
            if success == 1 or attempts > 10:
                break

    pass

    def download_hdf(self):
        url_dir = self.urls_dir

        tiles = self.shp_to_Sinusoidal_tiles()
        tiles_str = []
        for t in tiles:
            v, h = t
            ts = '.h{:02d}v{:02d}.'.format(h,v)
            tiles_str.append(ts)

        selected_urls = []
        for f in os.listdir(url_dir):
            fr = open(url_dir+f,'r')
            lines = fr.readlines()
            fr.close()
            for tile in tiles_str:
                for line in lines:
                    line = line.split('\n')[0]
                    # print tile,line
                    if tile in line:
                        save_path = self.download_dir + line.split('/')[-2] + '/'
                        T.mk_dir(save_path, force=1)
                        selected_urls.append(line)

        #### debug ####
        # for i in selected_urls:
            # print i
            # self.kernel_download_hdf(i)
        #### debug ####

        M(self.kernel_download_hdf,selected_urls).run(process=self.thread,process_or_thread='t',desc='downloading..')

        pass



class HDF_process:

    def __init__(self):

        pass


    def run(self):
        pass


    def hdf_subdataset_extraction(self,hdf_file, dst_dir, subdataset):
        """unpack a single subdataset from a HDF5 container and write to GeoTiff"""
        # # # forked from https://gis.stackexchange.com/questions/174017/extract-scientific-layers-from-modis-hdf-dataeset-using-python-gdal
        # open the dataset
        # hdf_file = xx.hdf
        # dst_dir = 'd:/dir/'
        # subdataset = 0
        hdf_ds = gdal.Open(hdf_file, gdal.GA_ReadOnly)
        band_ds = gdal.Open(hdf_ds.GetSubDatasets()[subdataset][0], gdal.GA_ReadOnly)

        # read into numpy array
        band_array = band_ds.ReadAsArray().astype(np.int16)

        # convert no_data values
        band_array[band_array == -28672] = -32768

        # build output path
        band_path = os.path.join(dst_dir, os.path.basename(os.path.splitext(hdf_file)[0]) + "-sd" + str(
            subdataset + 1) + ".tif")

        # write raster
        out_ds = gdal.GetDriverByName('GTiff').Create(band_path,
                                                      band_ds.RasterXSize,
                                                      band_ds.RasterYSize,
                                                      1,  # Number of bands
                                                      gdal.GDT_Int16,
                                                      ['COMPRESS=LZW', 'TILED=YES'])
        out_ds.SetGeoTransform(band_ds.GetGeoTransform())
        out_ds.SetProjection(band_ds.GetProjection())
        out_ds.GetRasterBand(1).WriteArray(band_array)
        out_ds.GetRasterBand(1).SetNoDataValue(-999999)

        out_ds = None  # close dataset to write to disc


    def tif_mosaic(self,in_dir,out_tif):
        # forked from https://www.neonscience.org/merge-lidar-geotiff-py
        # GDAL mosaic
        anaconda_python_path = r'C:\Users\ly\Anaconda2\python.exe '
        gdal_script = r'C:\Users\ly\Anaconda2\Lib\site-packages\osgeo\scripts\gdal_merge.py'
        files_to_mosaic = glob.glob('{}\\*.tif'.format(in_dir))
        files_string = " ".join(files_to_mosaic)
        command = "{} {} -o {} -of gtiff ".format(anaconda_python_path,gdal_script,out_tif) + files_string
        output = subprocess.check_output(command)
        # output

def main():
    ModisDownload().run()
    pass


if __name__ == '__main__':
    main()