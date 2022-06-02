#############################################################################
# ID: #883qox22
# Last Updated: 19 May 2022
# 
# Skills test 2:
# --------------
# Use the SpeedDating file attached to write a script that will perform 
# one of the options below. You can use any library in either Stata, R, or
# Python.  There are 195 columns and attached is also a column explanation
# document. You can perform your analysis on any column(s) of your choice. 
# o	Multivariate Analysis
# o	Principal Component Analysis
# o	Cox PH
# o	Clustering
# o	Segmentation
# o	MANOVA
# After you have completed your statistics, please supply a short-written
# (1 – 2 paragraph) analysis to explain your report. You may also supply a 
# graph(s) to support your findings. (The primary focus is both the skill of
# the scripting and a clear and concise communication of the analysis). 
# A meaningful insight is not mandatory, understanding how to use statistics
# and how to validate your findings is the key. 
#############################################################################


####################
# Clear Memory
####################
rm(list=ls())


##################
# Load libraries
##################
library(tidyverse)
library(caret)
library(factoextra)
library(ggfortify)


#####################
# Import raw dataset
#####################
speed_dating <- read.csv("SpeedDating.csv")
# n = 8,378 obs of 195 variables


############################
# Exploratory Data Analysis
############################
#display structure of the dataset
glimpse(speed_dating)


#How many columns have missing values?
length(which(colSums(is.na(speed_dating))>0))
# [1] 174

#Which variables have missing values?
names(which(colSums(is.na(speed_dating))>0))
# [1] "id"       "positin1" "pid"      "int_corr" "age_o"    "race_o"   "pf_o_att"
# [8] "pf_o_sin" "pf_o_int" "pf_o_fun" "pf_o_amb" "pf_o_sha" "attr_o"   "sinc_o"  
# [15] "intel_o"  "fun_o"    "amb_o"    "shar_o"   "like_o"   "prob_o"   "met_o"   
# [22] "age"      "field_cd" "race"     "imprace"  "imprelig" "goal"     "date"    
# [29] "go_out"   "career_c" "sports"   "tvsports" "exercise" "dining"   "museums" 
# [36] "art"      "hiking"   "gaming"   "clubbing" "reading"  "tv"       "theater" 
# [43] "movies"   "concerts" "music"    "shopping" "yoga"     "exphappy" "expnum"  
# [50] "attr1_1"  "sinc1_1"  "intel1_1" "fun1_1"   "amb1_1"   "shar1_1"  "attr4_1" 
# [57] "sinc4_1"  "intel4_1" "fun4_1"   "amb4_1"   "shar4_1"  "attr2_1"  "sinc2_1" 
# [64] "intel2_1" "fun2_1"   "amb2_1"   "shar2_1"  "attr3_1"  "sinc3_1"  "fun3_1"  
# [71] "intel3_1" "amb3_1"   "attr5_1"  "sinc5_1"  "intel5_1" "fun5_1"   "amb5_1"  
# [78] "attr"     "sinc"     "intel"    "fun"      "amb"      "shar"     "like"    
# [85] "prob"     "met"      "match_es" "attr1_s"  "sinc1_s"  "intel1_s" "fun1_s"  
# [92] "amb1_s"   "shar1_s"  "attr3_s"  "sinc3_s"  "intel3_s" "fun3_s"   "amb3_s"  
# [99] "satis_2"  "length"   "numdat_2" "attr7_2"  "sinc7_2"  "intel7_2" "fun7_2"  
# [106] "amb7_2"   "shar7_2"  "attr1_2"  "sinc1_2"  "intel1_2" "fun1_2"   "amb1_2"  
# [113] "shar1_2"  "attr4_2"  "sinc4_2"  "intel4_2" "fun4_2"   "amb4_2"   "shar4_2" 
# [120] "attr2_2"  "sinc2_2"  "intel2_2" "fun2_2"   "amb2_2"   "shar2_2"  "attr3_2" 
# [127] "sinc3_2"  "intel3_2" "fun3_2"   "amb3_2"   "attr5_2"  "sinc5_2"  "intel5_2"
# [134] "fun5_2"   "amb5_2"   "you_call" "them_cal" "date_3"   "numdat_3" "num_in_3"
# [141] "attr1_3"  "sinc1_3"  "intel1_3" "fun1_3"   "amb1_3"   "shar1_3"  "attr7_3" 
# [148] "sinc7_3"  "intel7_3" "fun7_3"   "amb7_3"   "shar7_3"  "attr4_3"  "sinc4_3" 
# [155] "intel4_3" "fun4_3"   "amb4_3"   "shar4_3"  "attr2_3"  "sinc2_3"  "intel2_3"
# [162] "fun2_3"   "amb2_3"   "shar2_3"  "attr3_3"  "sinc3_3"  "intel3_3" "fun3_3"  
# [169] "amb3_3"   "attr5_3"  "sinc5_3"  "intel5_3" "fun5_3"   "amb5_3" 


#Which variables are completely filled?
names(which(colSums(is.na(speed_dating))==0))
# [1] "iid"      "gender"   "idg"      "condtn"   "wave"     "round"    "position"
# [8] "order"    "partner"  "match"    "samerace" "dec_o"    "field"    "undergra"
# [15] "mn_sat"   "tuition"  "from"     "zipcode"  "income"   "career"   "dec" 

# Note: 21 columns with non-missing values

# Check how well columns are populated - display proportion populated
colSums(!is.na(speed_dating)) / nrow(speed_dating)
#        iid         id     gender        idg     condtn       wave      round 
# 1.00000000 0.99988064 1.00000000 1.00000000 1.00000000 1.00000000 1.00000000 
#   position   positin1      order    partner        pid      match   int_corr 
# 1.00000000 0.77966102 1.00000000 1.00000000 0.99880640 1.00000000 0.98114108 
#   samerace      age_o     race_o   pf_o_att   pf_o_sin   pf_o_int   pf_o_fun 
# 1.00000000 0.98758654 0.99128670 0.98937694 0.98937694 0.98937694 0.98830270 
#   pf_o_amb   pf_o_sha      dec_o     attr_o     sinc_o    intel_o      fun_o 
# 0.98722846 0.98460253 1.00000000 0.97469563 0.96574361 0.96347577 0.95703032 
#      amb_o     shar_o     like_o     prob_o      met_o        age      field 
# 0.91382191 0.87156839 0.97015994 0.96204345 0.95404631 0.98866078 1.00000000 
#   field_cd   undergra     mn_sat    tuition       race    imprace   imprelig 
# 0.99021246 1.00000000 1.00000000 1.00000000 0.99248031 0.99057054 0.99057054 
#       from    zipcode     income       goal       date     go_out     career 
# 1.00000000 1.00000000 1.00000000 0.99057054 0.98842206 0.99057054 1.00000000 
#   career_c     sports   tvsports   exercise     dining    museums        art 
# 0.98352829 0.99057054 0.99057054 0.99057054 0.99057054 0.99057054 0.99057054 
#     hiking     gaming   clubbing    reading         tv    theater     movies 
# 0.99057054 0.99057054 0.99057054 0.99057054 0.99057054 0.99057054 0.99057054 
#   concerts      music   shopping       yoga   exphappy     expnum    attr1_1 
# 0.99057054 0.99057054 0.99057054 0.99057054 0.98794462 0.21484841 0.99057054 
#    sinc1_1   intel1_1     fun1_1     amb1_1    shar1_1    attr4_1    sinc4_1 
# 0.99057054 0.99057054 0.98937694 0.98818334 0.98555741 0.77452853 0.77452853 
#   intel4_1     fun4_1     amb4_1    shar4_1    attr2_1    sinc2_1   intel2_1 
# 0.77452853 0.77452853 0.77452853 0.77190260 0.99057054 0.99057054 0.99057054 
#     fun2_1     amb2_1    shar2_1    attr3_1    sinc3_1     fun3_1   intel3_1 
# 0.99057054 0.98937694 0.98937694 0.98746718 0.98746718 0.98746718 0.98746718 
#     amb3_1    attr5_1    sinc5_1   intel5_1     fun5_1     amb5_1        dec 
# 0.98746718 0.58558128 0.58558128 0.58558128 0.58558128 0.58558128 1.00000000 
#       attr       sinc      intel        fun        amb       shar       like 
# 0.97588923 0.96693722 0.96466937 0.95822392 0.91501552 0.87264264 0.97135354 
#       prob        met   match_es    attr1_s    sinc1_s   intel1_s     fun1_s 
# 0.96311769 0.95523991 0.85999045 0.48889950 0.48889950 0.48889950 0.48889950 
#     amb1_s    shar1_s    attr3_s    sinc3_s   intel3_s     fun3_s     amb3_s 
# 0.48889950 0.48889950 0.47744092 0.47744092 0.47744092 0.47744092 0.47744092 
#    satis_2     length   numdat_2    attr7_2    sinc7_2   intel7_2     fun7_2 
# 0.89078539 0.89078539 0.88720458 0.23681069 0.23334925 0.23681069 0.23681069 
#     amb7_2    shar7_2    attr1_2    sinc1_2   intel1_2     fun1_2     amb1_2 
# 0.23334925 0.23561709 0.88863691 0.89078539 0.89078539 0.89078539 0.89078539 
#    shar1_2    attr4_2    sinc4_2   intel4_2     fun4_2     amb4_2    shar4_2 
# 0.89078539 0.68930532 0.68930532 0.68930532 0.68930532 0.68930532 0.68930532 
#    attr2_2    sinc2_2   intel2_2     fun2_2     amb2_2    shar2_2    attr3_2 
# 0.68930532 0.68930532 0.68930532 0.68930532 0.68930532 0.68930532 0.89078539 
#    sinc3_2   intel3_2     fun3_2     amb3_2    attr5_2    sinc5_2   intel5_2 
# 0.89078539 0.89078539 0.89078539 0.89078539 0.52243972 0.52243972 0.52243972 
#     fun5_2     amb5_2   you_call   them_cal     date_3   numdat_3   num_in_3 
# 0.52243972 0.52243972 0.47433755 0.47433755 0.47433755 0.17856290 0.07973263 
#    attr1_3    sinc1_3   intel1_3     fun1_3     amb1_3    shar1_3    attr7_3 
# 0.47433755 0.47433755 0.47433755 0.47433755 0.47433755 0.47433755 0.24063022 
#    sinc7_3   intel7_3     fun7_3     amb7_3    shar7_3    attr4_3    sinc4_3 
# 0.24063022 0.24063022 0.24063022 0.24063022 0.24063022 0.35318692 0.35318692 
#   intel4_3     fun4_3     amb4_3    shar4_3    attr2_3    sinc2_3   intel2_3 
# 0.35318692 0.35318692 0.35318692 0.35318692 0.35318692 0.35318692 0.35318692 
#     fun2_3     amb2_3    shar2_3    attr3_3    sinc3_3   intel3_3     fun3_3 
# 0.35318692 0.35318692 0.24063022 0.47433755 0.47433755 0.47433755 0.47433755 
#     amb3_3    attr5_3    sinc5_3   intel5_3     fun5_3     amb5_3 
# 0.47433755 0.24063022 0.24063022 0.24063022 0.24063022 0.24063022 


# summary statistics of all variables
summary(speed_dating)

# There were 21 waves (events) in total from 16 Oct 2002 to 7 Apr 2004
summary(speed_dating$wave)
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# 1.00    7.00   11.00   11.35   15.00   21.00

#unique subject id (based on wave id and gender)
summary(speed_dating$iid)
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# 1.0   154.0   281.0   283.7   407.0   552.0 

#subject number within wave (perhaps gender?)
summary(speed_dating$id)
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max.    NA's 
# 1.00    4.00    8.00    8.96   13.00   22.00       1
   
#subject number within gender (perhaps within wave?)
summary(speed_dating$idg)
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# 1.00    8.00   16.00   17.33   26.00   44.00
# Note: Wave 21 had 22 males and 22 females

# How many obs with no missing values?
na.omit(speed_dating)
# 0 obs 


#################################################
# Principal Components Analysis (PCA)
#################################################
# PCA Advantages
# --------------
# 1. Useful for dimension reduction on high-dimensional data sets
#    Helps reduce the number of predictors using principal components.
#    Finds the best components by fitting different 'axes' when projecting
#    values (scores) onto system
# 2. Converts highly correlated features to a set of uncorrelated 
#    principal components, which are linear weighted combinations of 
#    the original features 
# 3. Allows interpretation of many variables using a 2-dimensional 
#    biplot
# 4. Can be used for developing prediction models

# PCA Disadvantages
# -----------------
# 1. Only numeric variables can be used.
# 2. Prediction models using principal components are less interpretable.


############################################
# Create Analysis Dataset for PCA
############################################
# Of the 21 waves:
# - 17 waves had a preference scale of 100% allocation.
# - 4 waves (waves 6, 7, 8, and 9) had a preference scale of 1-10

# For this exercise, we will only look at data for the majority waves with 
# a 100% allocation

table(speed_dating$wave)
#   1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16  17  18  19  20 
# 200 608 200 648 190  50 512 200 800 162 882 392 180 720 684  96 280  72 450  84 
#  21 
# 968

# Observations in waves 6,7,8,9
800+200+512+50
# [1] 1562

8378-1562
# 6816

speed_dating_pref100 <- speed_dating %>%
  filter(!wave %in% c(6,7,8,9) )
# n = 6,816 obs of 195 variables


##########################################################################
# Exclude categorical variables and identification variables
# from PCA 
##########################################################################
colnames(speed_dating_pref100)

omit_vars <- c("iid","id","gender","idg","condtn","wave","round",
               "position","positin1","order","partner","pid","match",
               "samerace","race_o","dec_o", "met_o","field","field_cd",
               "undergra","mn_sat","tuition","race","from",
               "zipcode","income","goal","date","go_out","career",
               "career_c","dec","met", "length","numdat_2","date_3")

speed_pref100_subset <- speed_dating_pref100 %>%
  select(!omit_vars)
# n = 6,816 obs of 159 variables

colnames(speed_pref100_subset)


# Check how well columns are populated - display proportion populated
col_pop_prop <- colSums(!is.na(speed_pref100_subset)) / nrow(speed_pref100_subset)
col_pop_prop
#   int_corr      age_o   pf_o_att   pf_o_sin   pf_o_int   pf_o_fun   pf_o_amb 
# 0.97828638 0.98547535 0.98767606 0.98767606 0.98767606 0.98635563 0.98503521 
#   pf_o_sha     attr_o     sinc_o    intel_o      fun_o      amb_o     shar_o 
# 0.98180751 0.97007042 0.96141432 0.95950704 0.95525235 0.91285211 0.87470657 
#     like_o     prob_o        age    imprace   imprelig     sports   tvsports 
# 0.96742958 0.95906690 0.98679577 0.98914319 0.98914319 0.98914319 0.98914319 
#   exercise     dining    museums        art     hiking     gaming   clubbing 
# 0.98914319 0.98914319 0.98914319 0.98914319 0.98914319 0.98914319 0.98914319 
#     reading         tv    theater     movies   concerts      music   shopping 
# 0.98914319 0.98914319 0.98914319 0.98914319 0.98914319 0.98914319 0.98914319 
#       yoga   exphappy     expnum    attr1_1    sinc1_1   intel1_1     fun1_1 
# 0.98914319 0.98591549 0.26408451 0.98914319 0.98914319 0.98914319 0.98767606 
#     amb1_1    shar1_1    attr4_1    sinc4_1   intel4_1     fun4_1     amb4_1 
# 0.98620892 0.98298122 0.72359155 0.72359155 0.72359155 0.72359155 0.72359155 
#    shar4_1    attr2_1    sinc2_1   intel2_1     fun2_1     amb2_1    shar2_1 
# 0.72036385 0.98914319 0.98914319 0.98914319 0.98914319 0.98767606 0.98767606 
#    attr3_1    sinc3_1     fun3_1   intel3_1     amb3_1    attr5_1    sinc5_1 
# 0.98532864 0.98532864 0.98532864 0.98532864 0.98532864 0.71977700 0.71977700 
#    intel5_1     fun5_1     amb5_1       attr       sinc      intel        fun 
# 0.71977700 0.71977700 0.71977700 0.97153756 0.96288146 0.96097418 0.95671948 
#        amb       shar       like       prob   match_es    attr1_s    sinc1_s 
# 0.91431925 0.87602700 0.96889671 0.96038732 0.92106808 0.38879108 0.38879108 
#   intel1_s     fun1_s     amb1_s    shar1_s    attr3_s    sinc3_s   intel3_s 
# 0.38879108 0.38879108 0.38879108 0.38879108 0.38350939 0.38350939 0.38350939 
#     fun3_s     amb3_s    satis_2    attr7_2    sinc7_2   intel7_2     fun7_2 
# 0.38350939 0.38350939 0.88981808 0.29107981 0.28682512 0.29107981 0.29107981 
#     amb7_2    shar7_2    attr1_2    sinc1_2   intel1_2     fun1_2     amb1_2 
# 0.28682512 0.28961268 0.88717723 0.88981808 0.88981808 0.88981808 0.88981808 
#    shar1_2    attr4_2    sinc4_2   intel4_2     fun4_2     amb4_2    shar4_2 
# 0.88981808 0.64216549 0.64216549 0.64216549 0.64216549 0.64216549 0.64216549 
#    attr2_2    sinc2_2   intel2_2     fun2_2     amb2_2    shar2_2    attr3_2 
# 0.64216549 0.64216549 0.64216549 0.64216549 0.64216549 0.64216549 0.88981808 
#    sinc3_2   intel3_2     fun3_2     amb3_2    attr5_2    sinc5_2   intel5_2 
# 0.88981808 0.88981808 0.88981808 0.88981808 0.64216549 0.64216549 0.64216549 
#     fun5_2     amb5_2   you_call   them_cal   numdat_3   num_in_3    attr1_3 
# 0.64216549 0.64216549 0.44468897 0.44468897 0.15551643 0.08670775 0.44468897 
#    sinc1_3   intel1_3     fun1_3     amb1_3    shar1_3    attr7_3    sinc7_3 
# 0.44468897 0.44468897 0.44468897 0.44468897 0.44468897 0.29577465 0.29577465 
#   intel7_3     fun7_3     amb7_3    shar7_3    attr4_3    sinc4_3   intel4_3 
# 0.29577465 0.29577465 0.29577465 0.29577465 0.29577465 0.29577465 0.29577465 
#     fun4_3     amb4_3    shar4_3    attr2_3    sinc2_3   intel2_3     fun2_3 
# 0.29577465 0.29577465 0.29577465 0.29577465 0.29577465 0.29577465 0.29577465 
#     amb2_3    shar2_3    attr3_3    sinc3_3   intel3_3     fun3_3     amb3_3 
# 0.29577465 0.29577465 0.44468897 0.44468897 0.44468897 0.44468897 0.44468897 
#    attr5_3    sinc5_3   intel5_3     fun5_3     amb5_3 
# 0.29577465 0.29577465 0.29577465 0.29577465 0.29577465 

#Calculate correlations between variables
cor(speed_pref100_subset)
# Cannot compute because all columns contain missing values

# How many columns are over 70% populated?
sum(col_pop_prop > 0.70)
# [1] 86

# Retain only those columnns that are over 70% populated
newcols <- colnames(speed_pref100_subset) [col_pop_prop > 0.70]
speed_pref100_subset2 <- speed_pref100_subset[, newcols]
# n = 6,816 obs of 86 variables

# Check how well columns are populated - display proportion populated
col_pop_prop2 <- colSums(!is.na(speed_pref100_subset2)) / nrow(speed_pref100_subset2)
col_pop_prop2


####################################################################
# Fill in missing values using K-nearest neighbors (KNN) Imputation
####################################################################
# The square root of the number of variables is generally used for k
speed_dating_preProc <- preProcess(speed_pref100_subset2,
                                     method = c("knnImpute"),
                                     k = 9)

speed_dating_preProc
# Created from 2871 samples and 86 variables
# 
# Pre-processing:
# - centered (86)
# - ignored (0)
# - 9 nearest neighbor imputation (86)
# - scaled (86)

speed_dating_imputed1 <- predict(speed_dating_preProc, 
                                 speed_pref100_subset2,
                                 na.action = na.pass)

# The speed_dating_imputed1 data set will be normalized. 
# To de-normalize and get the original data back:

for(i in 1:ncol(speed_dating_imputed1)) {
  speed_dating_imputed1[i] <- 
    speed_dating_imputed1[i]*speed_dating_preProc$std[i]+
    speed_dating_preProc$mean[i] 
}


# summary stats of the 42 variables
summary(speed_dating_imputed1)


# examine a sample of original and imputed records
# sample of original records
speed_pref100_subset2[3441:3446, 1:8]
#      int_corr age_o pf_o_att pf_o_sin pf_o_int pf_o_fun pf_o_amb pf_o_sha
# 3441    -0.35    22       15       25       15       20       10       15
# 3442     0.43    34       15       15       10       25       10       25
# 3443       NA    34        2       60       15        8        5       10
# 3444       NA    25       10       20       20       20       10       20
# 3445       NA    23       40       10       15       20       10        5
# 3446       NA    30       10       20       20       10       10       30

# sample of imputed records
speed_dating_imputed1[3441:3446, 1:8]
# int_corr age_o pf_o_att pf_o_sin pf_o_int pf_o_fun pf_o_amb pf_o_sha
# 3441 -0.35000000    22       15       25       15       20       10       15
# 3442  0.43000000    34       15       15       10       25       10       25
# 3443  0.34777778    34        2       60       15        8        5       10
# 3444  0.10555556    25       10       20       20       20       10       20
# 3445  0.26777778    23       40       10       15       20       10        5
# 3446  0.03666667    30       10       20       20       10       10       30


# Correlation matrix of the matrix
cor_mat <- cor(speed_dating_imputed1)

#write to Excel spreadsheet to verify absolute value of correlations
#above 0.5
write_excel_csv(data.frame(cor_mat), "corr_mat.csv")


# compute pairwise correlations of the variables
# make a df of all the pairwise combinations of column names
corrs_df <- as.data.frame(t(combn(names(speed_dating_imputed1), m=2)))

# compute the correlation between each of the columns of `dat`
corrs_df$cor <- apply(corrs_df, 1, 
                      function(x) cor(speed_dating_imputed1[[x[1]]],
                                      speed_dating_imputed1[[x[2]]]))


#set correlation threshold to 0.5 (moderately strong threshold)
#We want to exclude all absolute correlations below 0.5
threshold <- .5
corrs_threshold <- corrs_df[abs(corrs_df$cor) > threshold, ] 

#identify the unique columns that have an absolute correlation with any of 
#the other variables above 0.5
cols_dup_corr <- c(corrs_threshold$V1, corrs_threshold$V2)
cols_unique_corr_thresh <- unique(cols_dup_corr)

#list all columns which have an absolute correlation with any of the 
#other variables above 0.5
cols_unique_corr_thresh
# [1] "attr_o"   "sinc_o"   "intel_o"  "fun_o"    "shar_o"   "museums"  "art"     
# [8] "theater"  "concerts" "attr1_1"  "sinc1_1"  "intel1_1" "amb1_1"   "shar1_1" 
# [15] "attr4_1"  "attr2_1"  "attr3_1"  "sinc3_1"  "fun3_1"   "intel3_1" "amb3_1"  
# [22] "attr5_1"  "intel5_1" "fun5_1"   "amb5_1"   "attr"     "sinc"     "intel"   
# [29] "fun"      "shar"     "attr1_2"  "like_o"   "amb_o"    "movies"   "music"   
# [36] "sinc1_2"  "intel1_2" "amb1_2"   "shar1_2"  "sinc4_1"  "intel4_1" "sinc2_1" 
# [43] "intel2_1" "amb2_1"   "attr3_2"  "sinc5_1"  "sinc3_2"  "fun3_2"   "intel3_2"
# [50] "amb3_2"   "like"     "amb"   


#Analyze only the 52 variables with moderate to high correlation
speed_dating_imputed2 <- speed_dating_imputed1[,cols_unique_corr_thresh]
# n = 6,816 obs of 52 variables


################################################
# Conduct PCA on Analysis dataset
################################################
speed_pca <- prcomp(speed_dating_imputed2,
             center = TRUE,
             scale. = TRUE)


#what attributes are stored in pc
attributes(speed_pca)
# $names
# [1] "sdev"     "rotation" "center"   "scale"    "x"       
# 
# $class
# [1] "prcomp"

speed_pca$center[1:5]
#   attr_o   sinc_o  intel_o    fun_o   shar_o 
# 6.166368 7.148058 7.333064 6.381822 5.445797

speed_pca$scale[1:5]
#   attr_o   sinc_o  intel_o    fun_o   shar_o 
# 1.938066 1.750053 1.537795 1.939027 2.069303 


#verify the center and scale attributes of pca object
mean(speed_dating_imputed2$attr_o)
# [1] 6.166368
sd(speed_dating_imputed2$attr_o)
# [1] 1.938066


# Print the pca object
print(speed_pca)
# We get standard deviations and rotations also called loadings

#display the pca loadings
speed_pca$rotation

#display the pca scores
speed_pca$x


#correlation matrix between our data and principal components
#This is very useful in order to determine if there is any relevancy 
#between our actual components and our actual data, and the higher it is 
#to 1, then the more closely related the principal components are to your
#given dataset.
round(cor(speed_dating_imputed2, speed_pca$x),3)


# summary of the pca object
summary(speed_pca)
# Importance of components:
#                           PC1     PC2     PC3     PC4     PC5     PC6     PC7
# Standard deviation     2.5070 2.25937 2.09471 1.84617 1.78601 1.51101 1.46499
# Proportion of Variance 0.1209 0.09817 0.08438 0.06554 0.06134 0.04391 0.04127
# Cumulative Proportion  0.1209 0.21903 0.30342 0.36896 0.43030 0.47421 0.51548
#                            PC8     PC9    PC10    PC11    PC12    PC13    PC14
# Standard deviation     1.39250 1.33452 1.23767 1.14079 1.08194 1.06160 0.98270
# Proportion of Variance 0.03729 0.03425 0.02946 0.02503 0.02251 0.02167 0.01857
# Cumulative Proportion  0.55277 0.58702 0.61648 0.64151 0.66402 0.68569 0.70426
#                           PC15    PC16    PC17    PC18    PC19    PC20    PC21
# Standard deviation     0.94834 0.93114 0.91243 0.87942 0.85453 0.82114 0.79878
# Proportion of Variance 0.01729 0.01667 0.01601 0.01487 0.01404 0.01297 0.01227
# Cumulative Proportion  0.72156 0.73823 0.75424 0.76911 0.78316 0.79612 0.80839
#                           PC22    PC23    PC24    PC25    PC26   PC27    PC28
# Standard deviation     0.76368 0.75701 0.74900 0.73916 0.71675 0.6880 0.67673
# Proportion of Variance 0.01122 0.01102 0.01079 0.01051 0.00988 0.0091 0.00881
# Cumulative Proportion  0.81961 0.83063 0.84142 0.85192 0.86180 0.8709 0.87971
#                           PC29    PC30    PC31   PC32    PC33    PC34    PC35
# Standard deviation     0.67398 0.66054 0.63589 0.6117 0.60298 0.57768 0.57438
# Proportion of Variance 0.00874 0.00839 0.00778 0.0072 0.00699 0.00642 0.00634
# Cumulative Proportion  0.88845 0.89684 0.90462 0.9118 0.91880 0.92522 0.93157
#                           PC36    PC37    PC38    PC39    PC40    PC41    PC42
# Standard deviation     0.56239 0.55574 0.53863 0.52889 0.52125 0.50722 0.50324
# Proportion of Variance 0.00608 0.00594 0.00558 0.00538 0.00523 0.00495 0.00487
# Cumulative Proportion  0.93765 0.94359 0.94917 0.95455 0.95977 0.96472 0.96959
#                          PC43   PC44    PC45    PC46    PC47    PC48    PC49
# Standard deviation     0.4943 0.4619 0.43950 0.41593 0.41189 0.39122 0.38215
# Proportion of Variance 0.0047 0.0041 0.00371 0.00333 0.00326 0.00294 0.00281
# Cumulative Proportion  0.9743 0.9784 0.98210 0.98543 0.98869 0.99164 0.99445
#                          PC50    PC51    PC52
# Standard deviation     0.3530 0.32095 0.24742
# Proportion of Variance 0.0024 0.00198 0.00118
# Cumulative Proportion  0.9968 0.99882 1.00000

###########################################################################
# Note:
# - The first 3  components captures 30% of the total variation 
# - The first 7  components captures 50% of the total variation 
# - The first 17 components captures 75% of the total variation 
###########################################################################



#################################################
# Choosing the number of Principal Components
#################################################
# One of the main challenges with PCA is selecting the number of PCs.

# Method 1. Number of PCs using scree plot
# -----------------------------------------
# The scree plot shows the change in the relative importance of the 
# variances over the principal components.
# Here, the "elbow" of the plot is used to give us an estimate of the 
# optimum number of principal components that explains most of the variance

# Scree plot
screeplot(speed_pca, type="l", main = "Scree Plot")
grid()

# About 7 components appears to capture a lot of the variation, which 
# is the "elbow" where the steep curve flattens out


####################################################################
# Method 2: Using the Cumulative proportion of variation explained
####################################################################
# The cumulative proportion of variation explained by the leading PCs
# are widely used to determine the ideal number of components to retain.

# Variance explained by each PC
speed_pca_var <- speed_pca$sdev^2  

# Proportion of variance explained by each PC
speed_pca_pvar <- speed_pca_var/sum(speed_pca_var)
speed_pca_pvar

# Cumulative variance explained plot
plot(cumsum(speed_pca_pvar), xlab = "Principal component", 
     ylab = "Cum Prop variance explained", 
     ylim = c(0,1), type = 'b',
     main = "Choosing the # of PCs")
grid()
# Add a horizontal red line for 50% threshold
abline(h = 0.50, col = "red", lty=2)
# Add a horizontal blue line for 75% threshold
abline(h = 0.75, col = "blue", lty=2)
text(3,0.55, "0.50 (7 PCs)", cex=0.7)
text(4,0.80, "0.75 (17 PCs)", cex=0.7)
# First 7 components are needed to explain 50% of the variation
# First 16 components are needed to explain 75% of the variation

#save plot
dev.copy(png,"pca_cum_var_plot.png")
dev.off()



########################################
# 2-D PCA plots
########################################
# A PCA biplot shows both PC scores of samples (dots) and loadings of 
# variables (vectors). The further away these vectors are from a PC origin,
# the more influence they have on that PC.

# show loadings and scores on the plot with the % of variation explained
# by each of the components
fviz_pca_biplot(speed_pca)

# plot only the scores
fviz_pca_ind(speed_pca)

# plot only the loadings
fviz_pca_var(speed_pca)


# Loading plots also show how variables correlate with one another: 
# a small angle implies positive correlation, a large one suggests 
# negative correlation, and a 90° angle indicates no correlation between 
# two characteristics

# Arrows represent each variable in the dataset
loadings_plot <- autoplot(speed_pca, 
         loadings = TRUE,
         loadings.label = TRUE, 
         loadings.label.size = 3,
         alpha = 0.1)

loadings_plot + 
  ggtitle("PCA Loadings Biplot: 52 variables")

ggsave("pca_loadings.png")


# PC1 loadings in ascending sequence 
round(sort(speed_pca$rotation[,1]),3)
# fun3_1   fun3_2  attr3_1   fun5_1  attr5_1   amb3_2  attr3_2   amb5_1   amb3_1 
# -0.259   -0.257   -0.245   -0.241   -0.226   -0.223   -0.222   -0.219   -0.211 
# intel3_2 intel5_1 intel3_1    intel     sinc      amb      fun  sinc5_1     shar 
# -0.203   -0.197   -0.186   -0.175   -0.169   -0.166   -0.161   -0.151   -0.150 
# like  sinc3_2    music    fun_o  sinc3_1   attr_o   shar_o   like_o     attr 
# -0.149   -0.139   -0.126   -0.124   -0.123   -0.117   -0.115   -0.111   -0.110 
# art concerts  museums    amb_o  attr1_1   sinc_o  theater  intel_o  attr1_2 
# -0.106   -0.098   -0.097   -0.082   -0.075   -0.071   -0.070   -0.069   -0.065 
# attr4_1   movies  attr2_1   amb1_1 intel2_1   amb2_1 intel4_1 intel1_2   amb1_2 
# -0.061   -0.055   -0.028   -0.026   -0.021    0.000    0.002    0.010    0.028 
# intel1_1  sinc2_1  sinc4_1  shar1_2  shar1_1  sinc1_2  sinc1_1 
# 0.036    0.039    0.053    0.056    0.067    0.071    0.075

# variable with lowest correlation for PC1
which.min(speed_pca$rotation[,1])
# fun3_1 
# 19 

# variable with highest correlation for PC1
which.max(speed_pca$rotation[,1])
# sinc1_1 
# 11

# You see fun3_1 furthest to the left on the negative axis of PC1
# and sinc1_1 furthest to the right on the positive axis of PC1



#################################################
# Periodically Save Temporary R Session 
#################################################
#Save the working image
save.image(file="speed_dating_RSession.RData")

#################################
# Load the saved datasets 
#################################
rm(list=ls())
#load image
load("speed_dating_RSession.RData")


