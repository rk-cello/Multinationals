**** combine time invariant data and construct cross-section data ****
**** notes ****
**** environment ****
clear all
set more off

* directories
global dir "/Users/reinakishida/Dropbox/Project_Multinationals"
global dir_raw "$dir/data/raw"
global dir_temp "$dir/data/temp"
global dir_cleaned "$dir/data/raw_cleaned"

* inputs
global input_metals_mining "$dir_raw/data_S&P/metals_mining"

* outputs
global output_property_level "$dir_cleaned/S&P_cleaned/property_level"
global output_company_level "$dir_cleaned/S&P_cleaned/company_level"
global output_properties "$output_property_level/properties"


************************************************************************

program main
    merge_time_variant_prop_details

end

program merge_time_variant_prop_details
    clear all
    set more off

    cd "$dir_temp/temp_prop_details"

    * List of files to merge
    local files property_details_6_1.dta property_details_6_2.dta 

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use `first', clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        display "Merging file: `f'"
        merge 1:1 prop_name prop_id year using `f'
        drop _merge
    }

    // reorder to prop_name prop_id year and everythin else
    order prop_name prop_id year, first

    * Save the merged dataset
    save "$output_properties/properties_property_details_panel.dta", replace

end

program merge_time_variant_production
    clear all
    set more off

    cd "$dir_temp/temp_production"

    * List of files to merge
    local files production_3_1.dta production_3_2.dta production_3_3.dta production_3_4.dta production_3_5.dta production_3_6.dta production_3_7.dta production_4_1.dta production_4_2.dta production_4_3.dta production_4_4.dta production_4_5.dta production_4_6.dta production_4_7.dta production_4_8.dta 
    * Use the first file as the master dataset
    local first : word 1 of `files'
    use `first', clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        display "Merging file: `f'"
        merge 1:1 prop_name prop_id year using `f'
        drop _merge
    }


end