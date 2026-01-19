**** construct company level cross-section data ****
**** notes ****
**1.Steps**
* first generate company IDs at the property level and create map between property ID/name and company ID
* merge map file to each cross-section data (not at property level) that includes property ID/name
* collapse each cross-section data to company level using the map file

**2.Company identifiers in raw data**
* operator_snl_instn_key
* owner_snl_instn_key (Current ownership details)

**** environment ****
clear all
set more off

* directories
global dir_raw "../../../data/raw"
global dir_temp "../../../data/temp"
global dir_cleaned "../../../data/raw_cleaned/S&P_cleaned"

* inputs
global input_property_level "$dir_cleaned/property_level"

* outputs
global output_company_level "$dir_cleaned/company_level"

* intermediates
/* global temp_property_level "$dir_temp/property_level"
global temp_company_level "$dir_temp/company_level"
global temp_reserves "$dir_temp/temp_reserves_resources" */

************************************************************************

program main

end

program generate_company_id
    use "$input_property_level/property_level_crosssection_data.dta", clear
    preserve
    keep operator_snl_instn_key
    duplicates drop
    drop if operator_snl_instn_key == "NA" | operator_snl_instn_key == ""
    /* gen company_id = operator_snl_instn_key
    destring company_id, replace  // this shows company_id has no missing values */
    rename operator_snl_instn_key company_id
    save "$dir_temp/company_id_list.dta", replace
end

program 
    restore
    * convert data from property level to company level
    

end
