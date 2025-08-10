**** construct property level cross-section data ****
**** notes ****
* some string variables should be converted to numeric
* this script appends each property details data
* historical data is year data, but should be separately treated from time-invariant info
**** environment ****
clear all
set more off

* directories
global dir_raw "../../data/raw"
global dir_temp "../../data/temp"
global dir_cleaned "../../data/raw_cleaned"

* inputs
global input_metals_mining "$dir_raw/data_S&P/metals_mining"

* outputs
global output_property_level "$dir_cleaned/property_level"
global output_company_level "$dir_cleaned/company_level"

* intermediates
global temp_property_level "$dir_temp/property_level"
global temp_company_level "$dir_temp/company_level"
global temp_prop_details "$dir_temp/temp_mine_econ_modeled"


************************************************************************
cd "$input_metals_mining/properties_mine_econ_modeled_data"

* roadmap
program main

end


program 