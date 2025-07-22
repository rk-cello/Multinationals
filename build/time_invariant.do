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
global output_property_level "$dir_cleaned/property_level"
global output_company_level "$dir_cleaned/company_level"
global output_property_crosssection "$dir_cleaned/S&P_cleaned/property_level/property_level_crosssection"


************************************************************************

program main
    merge_time_invariant_prop_details
    merge_time_invariant_production
    merge_time_invariant_reserves
    merge_time_invariant_tech_geo
    merge_time_invariant_financings //done
    merge_time_invariant_most_recent_transactions //done
end

**** PROPERTY DETAILS DATA MERGE ****

program merge_time_invariant_prop_details

    clear all
    set more off

    cd "$dir_temp/temp_prop_details"

    * List of files to merge
    local files property_details_1.dta property_details_2.dta property_details_3_1.dta property_details_3_2.dta property_details_4.dta property_details_5_1.dta property_details_5_2.dta property_details_5_3.dta property_details_5_4.dta property_details_5_5.dta property_details_5_6.dta property_details_5_7.dta property_details_5_8.dta property_details_5_9.dta property_details_5_10.dta property_details_5_11.dta property_details_5_12.dta property_details_5_13.dta property_details_5_14.dta property_details_5_15.dta property_details_7_1.dta property_details_7_2.dta property_details_7_3.dta property_details_8.dta property_details_9_1.dta property_details_9_2.dta property_details_9_3.dta property_details_9_4.dta property_details_9_5.dta property_details_10_1.dta property_details_10_2.dta property_details_10_3.dta property_details_11_1.dta property_details_11_2.dta

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use `first', clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        merge 1:1 prop_name prop_id using `f'
        drop _merge
    }

    * Save the merged dataset
    save "$output_property_crosssection/merged_time_invariant_prop_details.dta", replace

end


program merge_time_invariant_production
    
    clear all
    set more off

    cd "$dir_temp/temp_production"

    * List of files to merge
    local files production_1.dta production_2_1.dta production_2_2.dta production_2_3.dta production_2_4.dta production_2_5.dta production_2_6.dta

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use `first', clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        merge 1:1 prop_name prop_id using `f'
        drop _merge
    }

    * Save the merged dataset
    save "$output_property_crosssection/merged_time_invariant_production.dta", replace

end

program merge_time_invariant_reserves

    use "$temp_reserves/RR6.dta"
    save "$output_property_crosssection/merged_time_invariant_reserves.dta", replace

end

program merge_time_invariant_tech_geo

    use "$temp_tech_geo/tech_geo.dta"
    save "$output_property_crosssection/merged_time_invariant_tech_geo.dta", replace

end

program merge_time_invariant_financings
    
    clear all
    set more off

    cd "$dir_temp/temp_financings"

    * List of files to merge
    local files financings_1_1.dta financings_1_2.dta financings_1_3.dta financings_2.dta

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use `first', clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        merge 1:1 prop_name prop_id using `f'
        drop _merge
    }

    * Save the merged dataset
    save "$output_property_crosssection/merged_time_invariant_financings.dta", replace

end

program merge_time_invariant_most_recent_transactions
    
    clear all
    set more off

    cd "$dir_temp/temp_most_recent_transactions"

    * List of files to merge
    local files transaction_details_1.dta transaction_details_2.dta transaction_details_3.dta transaction_details_4.dta transaction_details_5.dta

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use `first', clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        merge 1:1 prop_name prop_id using `f'
        drop _merge
    }

    * Save the merged dataset
    save "$output_property_crosssection/merged_time_invariant_most_recent_transactions.dta", replace

end