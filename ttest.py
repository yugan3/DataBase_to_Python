# -*- coding: utf-8 -*-
"""
Created on Wed May  6 22:42:12 2020

@author: Gan
"""

from scipy import stats
import numpy as np
import scipy.stats

sending1 = np.array([0.23,0.17,0.19,0.14,0.17,0.21,0.20,0.17])
control1 = np.array([0.26,0.18,0.19,0.13,0.16,0.17,0.19,0.16])

sending2 = np.array([0.10,0.10,0.10,0.13,0.15,0.13,0.15])
control2 = np.array([0.09,0.08,0.12,0.14,0.17,0.09,0.15])

def ttest(A,B):
    t, pvalue = scipy.stats.ttest_ind(B,A)
    print(t, pvalue/2)