install.packages("rjson")
library("rjson")

setwd("C:/Users/ChristopherOdell/Desktop/Hubspot_data")
submissions_json_data <- fromJSON(file = "hubspot_form_submissions"))
