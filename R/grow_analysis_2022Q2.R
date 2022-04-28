library("dplyr")
library("MASS")
library("lmtest")
library("pscl")
library("ggplot2")


getwd()
setwd("C:/Users/ChristopherOdell/Desktop/Grow")

data <- read.csv("combined_grow_campaign_by_day_2022-04-22.csv")


data_filtered <- data %>% filter(date >= "2021-01-01" )

data_filtered <- data_filtered %>% filter(date < "2022-04-01" )

data_filtered[is.na(data_filtered)] <- 0

data_filtered <- data_filtered %>% filter(spend > 0)


data_filtered <- data_filtered %>% mutate(type = case_when(
  grepl("lead", data_filtered$campaign) ~ "enterprise",
  grepl("other", data_filtered$campaign) ~ "other",
  grepl("awareness", data_filtered$campaign) ~ "branding",
  grepl("trial", data_filtered$campaign) ~ "trial",
  grepl("partner", data_filtered$campaign) ~ "partner",
  grepl("nurtureueure", data_filtered$campaign) ~ "partner",
  ))


data_filtered <- data_filtered %>% mutate(region = case_when(
  grepl("noam", data_filtered$campaign) ~ "noam",
  grepl("emea-apac", data_filtered$campaign) ~ "emea-apac",

))

data_filtered$type[is.na(data_filtered$type)] <- "tBD"
data_filtered$region[is.na(data_filtered$region)] <- "unspecified"

data_filtered$log_spend <- log(data_filtered$spend + 1)

data_filtered <- data_filtered %>% mutate_at( c("log_spend"), ~(scale(.) %>% as.vector))

plot(data_filtered$log_spend)

as.data.frame(table(data_filtered$source))

head(data_filtered)

unique(data_filtered$type)

as.data.frame(table(data_filtered$type))


head(data_filtered)

model_1 <- glm(sum_of_mqls ~ campaign, data=data_filtered, family = "poisson")

summary(model_1)


model_2 <- glm(sum_of_mqls ~ source, data=data_filtered, family = "poisson")

summary(model_2)


model_3 <- glm(new_business_count ~ source, data=data_filtered, family = "poisson")

summary(model_3)

model_4 <- glm(new_business_count ~ source * campaign, data=data_filtered, family = "poisson")

summary(model_4)



summary(model_3)

unique(data_filtered$source)

model_5 <- glm(new_business_count ~ factor(source), data=data_filtered, family = "poisson")

summary(model_5)


E1 <- resid(model_5, type = "pearson")
N1  <- nrow(data_filtered)
p1  <- length(coef(model_5)) + 1  # '+1' is for variance parameter in NB
sum(E1^2) / (N1 - p1)
#There is overdispersion


model_6 <- glm.nb(new_business_count ~ source * region, data=data_filtered)

summary(model_6)

table_2 <- with(data_filtered, table(campaign, new_business_count))

round(table_2,2)

plot(data_filtered$new_business_count)

lrtest(model_5, model_6)

E2 <- resid(model_6, type = "pearson")
N  <- nrow(data_filtered)
p  <- length(coef(model_6)) + 1  # '+1' is for variance parameter in NB
sum(E2^2) / (N - p)


#model_7 <- zeroinfl(new_business_count ~ source * spend, dist = 'negbin',data = data_filtered, na.rm = TRUE)

#summary(model_7)

res <- resid(model_6)

plot(density(res))

qqnorm(res)

table_2 <- with(data_filtered, table(source, spend))

table <- xtabs(spend~source, data_filtered)

round(table,2)

table_1 <- with(data_filtered, table(new_business_count, source))

round(table_1,2)
