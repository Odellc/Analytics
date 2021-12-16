#EOY Analysis 2021

#install.packages("rstudioapi")
#install.packages("readr")
#install.packages("Hmisc")
#install.packages("psych")
#install.packages("ggplot2")
#install.packages("tidyverse")
#install.packages("NPS")
#install.packages("magrittr")
# install.packages("ggmap")
#library(ggplotgui)
# library(rstudioapi)

#install.packages(c("leaflet", "sp"))
library(sp)
library(leaflet)
library(psych)
library(NPS)
library(ggplot2)
library(readr)
library(dplyr)
library(magrittr)
library(tidyverse)
library(ggmap)
#library(ggplotify)
#library(pastecs)
# library(Hmisc)

# register_google(key = " ")

#getwd()

## Basic descriptive stats

EOY_survey_224responses_Dec8 <- read_csv("EOY_survey_224responses_Dec8.csv")

#Summary
describe(EOY_survey_224responses_Dec8)
describe(EOY_survey_224responses_Dec8[,13:18])
describeBy(EOY_survey_224responses_Dec8,"role_short")

#Gather columns
OKR <- EOY_survey_224responses_Dec8$okr_nps_0to10
Gtmhub <- EOY_survey_224responses_Dec8$gtmhub_nps_0to10
Success <- EOY_survey_224responses_Dec8$cs_nps_0to10
okr_string <- EOY_survey_224responses_Dec8$okr_nps_string
gtm_string <- EOY_survey_224responses_Dec8$gtmhub_nps_string
succ_string <- EOY_survey_224responses_Dec8$cs_nps_string

length(Gtmhub[Gtmhub==8])

# describe(okr_nps_0to10)
# describe(gtmhub_nps_0to10)
# describe(cs_nps_0to10)


## Histograms to show distrubtions of scores
## OKR Methodology NPS (0-10)

df <- data.frame(OKR, Gtmhub, Success, okr_string, gtm_string, succ_string)

idx <- 0
for(i in df){
  idx <- idx +1
  if (idx < 4){
    df1 <- df %>% drop_na(colnames(df[idx]))
    # print(colnames(df1[idx+3]))
    # print(head(df1))
    # 
    # print(df1[idx])
    # print(df1[idx+3])
    
    
  print(ggplot(df1, aes(x = df1[,c(colnames(df1[idx]))], fill = df1[,c(colnames(df1[idx+3]))])) +
    geom_histogram(position = 'identity', alpha = 0.9, binwidth = .5, color="black") +
    labs(x = colnames(df[idx]), y = 'count of respondents')+
    labs(fill = 'NPS legend')+
    # scale_fill_brewer(palette = 'RdYlGn') +
    scale_x_continuous(expand=c(0,0), breaks= seq(0,10,1))+
    scale_y_continuous(expand=c(0,0))+
    theme_classic() +
    theme(
      axis.title = element_text(size = 14),
      axis.text = element_text(size = 12),
      legend.position = 'bottom'
    )+
      scale_fill_manual(values= c("#FF9C33", "#999999", "#0072B2")))
  }
}

OKR_NPS_original <- nps(na.omit(OKR))
Gtm_NPS_original <- nps(na.omit(Gtmhub))
CS_NPS_original <- nps(na.omit(Success))


length(OKR)
length(na.omit(OKR))
length(na.omit(Gtmhub))
length(na.omit(Success))

Gtmhub_na <- na.omit(Gtmhub)
Success_na <- na.omit(Success)
# filter(Gtmhub_na, Gtmhub_na[Gtmhub_na==6])


find_num_to_convert <- function(col_vec, num_change){
  return(round(length(col_vec[col_vec==num_change])/2,0))
} 


calc_new_nps <- function(col_vector, len, lst, num_to_change ){
  new_vector = rep(NA, len)
  idx = 0
  c = 0
  c2 = 0
  for(x in col_vector){
    idx <- idx + 1
    if((length(lst)== 1)& ((x==lst[1])&(c<num_to_change[1])) ){
      c <-c + 1
      x <- x + 1
      
    }
    if((length(lst)==2)&((x==lst[1]))&(c<num_to_change[1])){
      c <-c + 1
      x <- x + 1
    }
    if((length(lst)==2)&((x==lst[2]))&(c2<num_to_change[2])){
      c2 <-c2 + 1
      x <- x + 1
    }

    new_vector[idx] <- x 
  }
  
  print(nps(new_vector))
  }

#GTM= 6
calc_new_nps(Gtmhub_na, length(Gtmhub_na), c(6), c(find_num_to_convert(Gtmhub_na, 6)))

#GTM= 8
calc_new_nps(Gtmhub_na, length(Gtmhub_na), c(8), c(find_num_to_convert(Gtmhub_na, 8)))

#GTM= 6 & 8
calc_new_nps(Gtmhub_na, length(Gtmhub_na), c(6,8), c(find_num_to_convert(Gtmhub_na, 6),find_num_to_convert(Gtmhub_na, 8)))


#Success= 6
calc_new_nps(Success_na, length(Success_na), c(6), c(find_num_to_convert(Success_na, 6)))

#Success= 8
calc_new_nps(Success_na, length(Success_na), c(8), c(find_num_to_convert(Success_na, 8)))

#Success= 6 & 8
calc_new_nps(Success_na, length(Success_na), c(6,8), c(find_num_to_convert(Success_na, 6),find_num_to_convert(Success_na, 8)))


prop.table(table(EOY_survey_224responses_Dec8$logins_last12weeks, df$Gtmhub))

# nps(EOY_survey_224responses_Dec8$gtmhub_nps_0to10) %>% group_by(EOY_survey_224responses_Dec8$logins_last12weeks)

xtabs(~logins_last12weeks +gtmhub_nps_0to10 ,EOY_survey_224responses_Dec8 )

EOY_survey_224responses_Dec8 %>% group_by(logins_last12weeks) %>% summarise_at(vars(gtmhub_nps_0to10), list(nps=nps))

# EOY_survey_224responses_Dec8$Address <-  mapply(FUN = function(lon, lat) revgeocode(c(lon, lat)), EOY_survey_224responses_Dec8$`Location Longitude`, EOY_survey_224responses_Dec8$`Location Latitude`)


# EOY_survey_224responses_Dec8 %>% group_by(logins_last12weeks) %>% summarise_at(vars(cs_nps_0to10), list(nps=nps))


long <- na.omit(EOY_survey_224responses_Dec8$`Location Longitude`)
lat <- na.omit(EOY_survey_224responses_Dec8$`Location Latitude`)

df1 <- data.frame(long, lat)

coordinates(df1) <- ~ long+ lat
leaflet(df1) %>% addMarkers() %>% addTiles()
