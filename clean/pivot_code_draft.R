### pivot script to be converted to stata
### environment ###
pacman::p_load(tidyverse, haven)

### import data ###
df <- read_dta("data/raw_cleaned/S&P_cleaned/property_level/property_level_crosssection_data.dta")

### pivoting ###

