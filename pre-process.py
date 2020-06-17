# coding=gbk

from __init__ import *


def bi_weekly_to_monthly(fdir,outdir,template_tif):

    # fdir = data_root + 'MODIS_clip\Namibia\MOD17A2H.006\\'
    # outdir = data_root + 'MODIS_clip\Namibia\MOD17A2H.006_monthly\\'
    # template_tif = this_root + 'conf\\Namibia.tif'

    D = DIC_and_TIF(template_tif)
    Tools().mk_dir(outdir)

    for y in range(2002,2020):
        for m in range(1, 13):
            date = '{}{:02d}'.format(y, m)
            print date
            one_month_tif = []
            for f in os.listdir(fdir):
                if not f.endswith('.tif'):
                    continue
                date_i = f[:6]
                if date_i == date:
                    one_month_tif.append(f)
            arrs = []
            for tif in one_month_tif:
                arr = to_raster.raster2array(fdir + tif)[0]
                arr = np.array(arr,dtype=np.float)
                arr[arr < -2999] = np.nan
                arrs.append(arr)
            # print arrs
            if len(arrs) == 0:
                continue
            one_month_mean = []
            for i in range(len(arrs[0])):
                temp = []
                for j in range(len(arrs[0][0])):
                    sum_ = []
                    for k in range(len(arrs)):
                        val = arrs[k][i][j]
                        sum_.append(val)
                    mean = np.nanmean(sum_)
                    temp.append(mean)
                one_month_mean.append(temp)
            one_month_mean = np.array(one_month_mean)
            D.arr_to_tif(one_month_mean, outdir + '{}.tif'.format(date))

    pass


def unify_raster(raster1, raster2):
    array1 = to_raster.raster2array(raster1)[0]
    # array1 = np.array(array1)
    shape1 = np.shape(array1)

    array2 = to_raster.raster2array(raster2)[0]
    shape2 = np.shape(array2)

    # print shape1, shape2
    if not shape1 == shape2:
        mincol = min(shape1[0], shape2[0])
        minrow = min(shape1[1], shape2[1])

        temp1 = array1[:mincol]
        temp2 = array2[:mincol]

        array1_new = temp1.T[:minrow].T
        array2_new = temp2.T[:minrow].T

        # print np.shape(array1_new)
        # print np.shape(array2_new)
        return array1_new, array2_new
        # if not shape1[0] == shape2[0] and shape1[1] == shape2[1]:
        #     min_col = min(shape1[0],shape2[0])
        #     array1_new = array1[:min_col]
        #     array2_new = array2[:min_col]
        #     print 'mincol'
        #     return array1_new,array2_new
        #
        # elif not shape1[1] == shape2[1] and shape1[0] == shape2[0]:
        #     min_row = min(shape1[1],shape2[1])
        #     array1_new = array1.T[:min_row].T
        #     array2_new = array2.T[:min_row].T
        #     print 'minrow'
        #     return array1_new,array2_new
        #
        # elif not shape1[0] == shape2[0] and not shape1[1] == shape2[1]:

    else:
        return array1, array2


def check_data_transform(fdir):
    for f in os.listdir(fdir):
        dic = T.load_npy(fdir+f)
        for pix in dic:
            vals = dic[pix]
            if vals[0]<=-9999:
                continue
            vals = np.array(vals,dtype=np.float)
            vals[vals<=-9999]=np.nan
            plt.plot(vals)
            plt.show()


class CleanData:

    def __init__(self):

        pass

    def run(self):
        for region in ['CJZXY','Namibia']:
            for x in ['MOD13A3.006', 'MOD17A2H.006']:
                fdir = data_root + r'perpix\{}\{}\\'.format(region,x)
                outdir = data_root + r'perpix\{}\{}_clean\\'.format(region,x)
                self.clean_origin_vals(fdir,outdir)
        pass


    def clean_origin_vals(self,fdir,outdir):
        T.mk_dir(outdir)
        for f in tqdm(os.listdir(fdir)):
            dic = T.load_npy(fdir+f)
            clean_dic = {}
            for pix in dic:
                val = dic[pix]
                val = np.array(val,dtype=np.float)
                val[val<-9999]=np.nan
                val[val>30000]=np.nan
                new_val = T.interp_nan(val,kind='linear')
                if len(new_val) == 1:
                    continue
                # plt.plot(val)
                # plt.show()
                clean_dic[pix] = new_val
            np.save(outdir+f,clean_dic)

    def clean_origin_vals_SWE(self,x='SWE'):
        fdir = data_root+'{}\\per_pix\\'.format(x)
        outdir = data_root+'{}\\per_pix_clean\\'.format(x)
        T.mk_dir(outdir)
        for f in tqdm(os.listdir(fdir)):
            dic = T.load_npy(fdir+f)
            clean_dic = {}
            for pix in dic:
                val = dic[pix]
                val = np.array(val,dtype=np.float)
                val_filter = T.interp_nan(val)
                if len(val_filter) == 1:
                    continue
                new_val = []
                for i in val:
                    if np.isnan(i):
                        v = 0
                    else:
                        v = i
                    new_val.append(v)
                # plt.plot(new_val)
                # plt.show()
                clean_dic[pix] = new_val
            np.save(outdir+f,clean_dic)


    def check_clean(self):
        x = 'SWE'
        fdir = data_root + '{}\\per_pix_clean\\'.format(x)
        x_dic = {}
        for f in tqdm(os.listdir(fdir)):
            dic = T.load_npy(fdir+f)
            for key in dic:
                if len(dic[key]) == 0:
                    continue
                x_dic[key] = np.mean(dic[key])
        arr = D.pix_dic_to_spatial_arr(x_dic)
        # plt.imshow(arr,vmin=0,vmax=100) #pre
        # plt.imshow(arr,vmin=-30,vmax=30) # tmp
        # plt.imshow(arr,vmin=0,vmax=0.3) # sm
        plt.imshow(arr)
        plt.colorbar()
        plt.show()

    def clean_SPEI(self):
        for i in range(1,13):
            fdir = data_root + 'SPEI\\per_pix\\spei{:02d}\\'.format(i)
            outdir = data_root + 'SPEI\\per_pix_clean\\spei{:02d}\\'.format(i)
            T.mk_dir(outdir,force=1)
            for f in tqdm(os.listdir(fdir),desc=str(i)):
                dic = T.load_npy(fdir + f)
                clean_dic = {}
                for pix in dic:
                    val = dic[pix]
                    val = np.array(val, dtype=np.float)
                    val[val < -9999] = np.nan
                    # new_val = T.interp_nan(val, kind='linear')
                    new_val = T.interp_nan(val)
                    if len(new_val) == 1:
                        continue
                    # plt.plot(val)
                    # plt.show()
                    clean_dic[pix] = new_val
                np.save(outdir + f, clean_dic)


        pass


def gen_monthly_mean(fdir,outdir,template):
    D = DIC_and_TIF(template)
    T.mk_dir(outdir,force=True)
    for mon in tqdm(range(1, 13)):
        # print x,mon
        arr_sum = []
        flag = 0
        void_dic = D.void_spatial_dic()
        for year in range(2003,2020):
            f = fdir+'{}{:02d}.tif'.format(year,mon)
            if not os.path.isfile(f):
                continue
            flag += 1
            arr = to_raster.raster2array(f)[0]
            T.mask_999999_arr(arr)
            dic = D.spatial_arr_to_dic(arr)
            for pix in dic:
                void_dic[pix].append(dic[pix])
        mon_mean_dic = {}
        for pix in void_dic:
            vals = void_dic[pix]
            mean_vals = np.nanmean(vals)
            mon_mean_dic[pix] = mean_vals
        mon_mean = D.pix_dic_to_spatial_arr(mon_mean_dic)
        D.arr_to_tif(mon_mean,outdir+'{:02d}.tif'.format(mon))


def main():

    pass



if __name__ == '__main__':
    main()