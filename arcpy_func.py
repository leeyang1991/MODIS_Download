# coding=gbk

import os
import arcpy
from arcpy.sa import *
from tqdm import tqdm
import multiprocessing
from multiprocessing.pool import ThreadPool as TPool
import copy_reg
import types



class MULTIPROCESS:
    '''
    可对类内的函数进行多进程并行
    由于GIL，多线程无法跑满CPU，对于不占用CPU的计算函数可用多线程
    并行计算加入进度条
    '''

    def __init__(self, func, params):
        self.func = func
        self.params = params
        copy_reg.pickle(types.MethodType, self._pickle_method)
        pass

    def _pickle_method(self, m):
        if m.im_self is None:
            return getattr, (m.im_class, m.im_func.func_name)
        else:
            return getattr, (m.im_self, m.im_func.func_name)

    def run(self, process=-9999, process_or_thread='p', **kwargs):
        '''
        # 并行计算加进度条
        :param func: input a kenel_function
        :param params: para1,para2,para3... = params
        :param process: number of cpu
        :param thread_or_process: multi-thread or multi-process,'p' or 't'
        :param kwargs: tqdm kwargs
        :return:
        '''

        if process > 0:
            if process_or_thread == 'p':
                pool = multiprocessing.Pool(process)
            elif process_or_thread == 't':
                pool = TPool(process)
            else:
                raise IOError('process_or_thread key error, input keyword such as "p" or "t"')

            results = list(tqdm(pool.imap(self.func, self.params), total=len(self.params), **kwargs))
            pool.close()
            pool.join()
            return results
        else:
            if process_or_thread == 'p':
                pool = multiprocessing.Pool()
            elif process_or_thread == 't':
                pool = TPool()
            else:
                raise IOError('process_or_thread key error, input keyword such as "p" or "t"')

            results = list(tqdm(pool.imap(self.func, self.params), total=len(self.params), **kwargs))
            pool.close()
            pool.join()
            return results



def mk_dir(dir, force=False):
    if not os.path.isdir(dir):
        if force == True:
            os.makedirs(dir)
        else:
            os.mkdir(dir)

def project_transformation(inras,outras,cellsize):
    prj_f = r"C:\Program Files (x86)\ArcGIS\Desktop10.2\Reference Systems\georef1.prj"
    resampling_type = 'NEAREST'
    arcpy.ProjectRaster_management(
        in_raster = inras,
        out_raster = outras,
        out_coor_system = prj_f,
        resampling_type = resampling_type,
        cell_size = cellsize,
        geographic_transform = None,
        Registration_Point = None,
        in_coor_system = None
                                    )


def arcpy_clip(in_raster,out_raster,in_template_dataset,nodata_value):
    # in_raster = this_root+'MRT_resample\\2000_257.txt_mosaic.hdf.FVC.tif'
    # out_raster = this_root+'test.tif'
    # in_template_dataset = this_root+'shp\\neimeng.shp'
    # nodata_value = 255
    clipping_geometry = True
    # maintain_clipping_extent = 1
    # print 'clipping'

    arcpy.Clip_management(
    in_raster=in_raster, rectangle=None, out_raster=out_raster,
        in_template_dataset=in_template_dataset, nodata_value=nodata_value,
        clipping_geometry=clipping_geometry)


def main():
    pass



if __name__ == '__main__':
    main()