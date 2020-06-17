# coding='gbk'
from matplotlib import pyplot as plt
import numpy as np
from scipy import interpolate
from scipy import signal
import time
import to_raster
import ogr, os, osr
from tqdm import tqdm
import datetime
from scipy import stats, linalg
import pandas as pd
import seaborn as sns
from matplotlib.font_manager import FontProperties
import imageio
from scipy.stats import gaussian_kde as kde
import matplotlib as mpl
import multiprocessing
from multiprocessing.pool import ThreadPool as TPool
import copy_reg
import types
from scipy.stats import gamma as gam
import math
import copy
import scipy
import sklearn
import ternary
import random
import h5py
from netCDF4 import Dataset
import shutil
import requests
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import urllib2
from cookielib import CookieJar
import re
import gdal
import subprocess, glob


def sleep(t=1):
    time.sleep(t)

this_root = 'F:\\project_ltj\\'
data_root = 'F:\\project_ltj\\data\\'
results_root = 'F:\\project_ltj\\results\\'
# from HANTS import *

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False
from LY_Tools import *
T = Tools()
D = DIC_and_TIF
S = SMOOTH()
M = MULTIPROCESS

