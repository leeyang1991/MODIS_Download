# coding=utf-8

from __init__ import *
class MRT:

    def __init__(self):
        # self.date = date
        self.__conf__()
        self.product = self.download_folder.split('\\')[-2]
        # self.product_output_folder = self.output+self.product

        self.mk_dir(self.temp_folder)
        # self.mk_dir(self.output)
        # self.mk_dir(self.product_output_folder)
        pass

    def __conf__(self):
        # self.download_folder = r'G:\project_ltj\modis_download\Namibia\HDF\MOD13A3.006\\'
        # self.download_folder = r'G:\project_ltj\modis_download\Namibia\HDF\MOD17A2H.006\\'
        # self.download_folder = r'G:\project_ltj\modis_download\CJZXY\HDF\MOD13A3.006\\'
        self.download_folder = r'G:\project_ltj\modis_download\CJZXY\HDF\MOD17A2H.006\\'

        # self.temp_folder = this_root+'\\temp\\Namibia\\MOD13A3.006\\'
        # self.temp_folder = this_root+'\\temp\\Namibia\\MOD17A2H.006\\'
        # self.temp_folder = this_root+'\\temp\\CJZXY\\MOD13A3.006\\'
        self.temp_folder = this_root + '\\temp\\CJZXY\\MOD17A2H.006\\'

        # self.output = data_root+'\\Namibia\\MOD13A3.006\\'
        # self.output = data_root+'\\Namibia\\MOD17A2H.006\\'
        # self.output = data_root+'\\CJZXY\\MOD13A3.006\\'
        # self.output = data_root + '\\CJZXY\\MOD17A2H.006\\'


        self.conf_file = data_root+'conf\\Namibia.prm'

        pass


    def run(self):
        dates = self.gen_dates()
        for d in dates:
            self.MRT_mosaic(d)
            # self.MRT_resample(d, conf_file)
            # self.delete_temp_hdf()
            # break
        pass


    def mk_dir(self,dir):
        T.mk_dir(dir,force=1)


    def gen_dates(self):
        dates = os.listdir(self.download_folder)
        return dates
        # pass

    def MRT_mosaic(self,date):
        # 生成要拼接的文件列表
        # dates = self.gen_dates()
        d = date
        f_dir = self.download_folder + d + '\\'
        f_list = os.listdir(f_dir)
        temp_file_list_folder = self.temp_folder +'\\'+self.product+'\\'
        self.mk_dir(temp_file_list_folder)
        file_list = temp_file_list_folder + d +'_files.txt'

        f = open(file_list, 'w')

        for fi in f_list:
            f.write(f_dir + fi + '\n')

        f.close()
        # exit()
        # MRT 拼接命令行
        mrt_mosaic = os.path.join(this_root,'MRT\\bin\\mrtmosaic.exe')
        mosaic_cmd_line = mrt_mosaic + ' -i ' + file_list + ' -s "1 0 0 0 0 0 0 0 0 0 0 0 0" -o ' + temp_file_list_folder + d + '_mosaic.hdf'
        # print cmd_line
        print mosaic_cmd_line
        os.system(mosaic_cmd_line)


    def MRT_resample(self,date,conf_file):

        # MRT 投影重采样
        temp_file_list_folder = self.temp_folder +'\\'+self.product+'\\'
        mrt_resample = os.path.join(this_root,'MRT\\bin\\resample.exe')
        # dates = self.gen_dates()
        d = date
        output_file = self.product_output_folder+'\\'+d+'.tif'
        resample_cmd_line = mrt_resample + ' -i ' + temp_file_list_folder + d +'_mosaic.hdf -p ' + conf_file + ' -o ' + output_file
        print resample_cmd_line
        os.system(resample_cmd_line)


    def delete_temp_hdf(self):
        temp_file_list_folder = self.temp_folder + '\\' + self.product + '\\'
        f = temp_file_list_folder + d + '_mosaic.hdf'
        try:
            os.remove(f)
        except:
            pass
        # print(f)

    def loop_mosaic(self):

        pass

    def loop_resample(self):

        pass



class HDF_process:

    def __init__(self):
        # self.region = 'Namibia'
        self.region = 'CJZXY'
        # self.product = 'MOD13A3.006'
        # self.product = 'MOD17A2H.006'
        self.product = 'MCD12Q1.006'
        pass


    def run(self):
        self.do_hdf_to_tif()
        self.do_mosaic_tif()
        pass


    def do_hdf_to_tif(self):
        fdir = this_root+r'\modis_download\{}\HDF\{}\\'.format(self.region,self.product)
        outdir = this_root+r'\modis_download\{}\tif\{}\\'.format(self.region,self.product)
        for folder in tqdm(os.listdir(fdir)):
            outdir_i = outdir+folder+'\\'
            T.mk_dir(outdir_i,force=True)
            for f in os.listdir(os.path.join(fdir,folder)):
                hdf = os.path.join(fdir,folder,f)
                self.hdf_subdataset_extraction(hdf,outdir_i,0)


    def do_mosaic_tif(self):
        fdir = this_root+r'modis_download\{}\tif\{}\\'.format(self.region,self.product)
        outdir = this_root+r'modis_download\{}\mosaic\{}\\'.format(self.region,self.product)
        T.mk_dir(outdir,force=True)
        for folder in tqdm(os.listdir(fdir)):
            out_tif = outdir+folder+'.tif'
            self.tif_mosaic(fdir+folder,out_tif)

        pass


    def hdf_subdataset_extraction(self,hdf_file, dst_dir, subdataset):
        """unpack a single subdataset from a HDF5 container and write to GeoTiff"""
        # # # forked from https://gis.stackexchange.com/questions/174017/extract-scientific-layers-from-modis-hdf-dataeset-using-python-gdal
        # open the dataset
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
        anaconda_python_path = r'C:\Users\ly\Anaconda2\python.exe '
        gdal_script = r'C:\Users\ly\Anaconda2\Lib\site-packages\osgeo\scripts\gdal_merge.py'
        files_to_mosaic = glob.glob('{}\\*.tif'.format(in_dir))
        files_string = " ".join(files_to_mosaic)
        command = "{} {} -o {} -of gtiff ".format(anaconda_python_path,gdal_script,out_tif) + files_string
        output = subprocess.check_output(command)
        # output


def main():
    # MRT().run()
    HDF_process().run()

    pass


if __name__ == '__main__':
    main()