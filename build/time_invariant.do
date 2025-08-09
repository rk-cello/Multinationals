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
    merge_time_invariant_prop_details //done
    merge_time_invariant_production //done
    merge_time_invariant_reserves //done
    merge_time_invariant_tech_geo //done
    merge_time_invariant_financings //done
    merge_time_invariant_most_recent_transactions //done
    merge_time_invariant_top_drill_results //done
    merge_time_invariant_claims //done
    merge_time_invariant_drill_results //done
    merge_time_invariant_capital_costs //done
    merge_time_invariant_transactions //done
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
        display "Merging file: `f'"
        merge 1:1 prop_name prop_id using `f'
        drop _merge
    }

    * Save the merged dataset
    save "$output_properties/properties_property_details_crosssection.dta", replace

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

    rename mill_capacity_tonnes_per_day mill_capacity_t_per_dy
    rename mill_capacity_tonnes_per_year mill_capacity_t_per_yr
    rename mill_capacity_cubic_m_per_day mill_capacity_cubic_m_per_dy
    rename mill_capacity_cubic_m_per_year mill_capacity_cubic_m_per_yr
    rename mining_produ_general_comments mining_prod_gen_comments
    rename mining_processing_cost_mt mining_process_cost_mt
    rename mining_processing_cost_cubic_m mining_process_cost_cubic_m
    local suffix "ore"
    foreach var of varlist start_up_yr-mining_process_cost_cubic_m {
        local newname = "`var'_`suffix'"
        rename `var' `newname'
        label var `newname' "`: variable label `var'' (`suffix')"
    }

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        merge 1:1 prop_name prop_id using `f'
        drop _merge
    }

    * Save the merged dataset
    save "$output_properties/properties_production_crosssection.dta", replace

end

program merge_time_invariant_reserves

    use "$dir_temp/temp_reserves_resources/RR6.dta"
    save "$output_properties/properties_reserves_resources_crosssection.dta", replace

end

program merge_time_invariant_tech_geo

    use "$dir_temp/temp_tech_geo/tech_geo.dta"
    save "$output_properties/properties_technical_geology_crosssection.dta", replace

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
    save "$output_properties/properties_financings_crosssection.dta", replace

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
    save "$output_properties/properties_most_recent_transactions_crosssection.dta", replace

end

program merge_time_invariant_top_drill_results
    
    clear all
    set more off

    cd "$dir_temp/temp_top_drill"

    * List of files to merge
    local files top_drill_1_1.dta top_drill_1_2.dta top_drill_1_3.dta top_drill_2_1.dta top_drill_2_2.dta top_drill_2_3.dta top_drill_2_4.dta top_drill_3_1.dta top_drill_3_2.dta top_drill_3_3.dta top_drill_3_4.dta top_drill_3_bulk_1.dta top_drill_3_bulk_2.dta top_drill_3_bulk_3.dta top_drill_3_bulk_4.dta top_drill_3_U3O8.dta most_recent_drill.dta

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
    save "$output_properties/properties_top_drill_results_crosssection.dta", replace

end

program merge_time_invariant_claims

    use "$dir_temp/temp_claims/claims_linked_to_properties.dta"
    save "$output_property_level/claims_crosssection.dta", replace
    
end

program merge_time_invariant_drill_results

    use "$dir_temp/temp_drill_results/drill_results_1&2.dta", clear
    save "$output_property_level/drill_results_crosssection.dta", replace

end

program merge_time_invariant_capital_costs

    use "$dir_temp/temp_drill_results/capital_costs_global.dta", clear
    save "$dir_cleaned/S&P_cleaned/capital_costs_crosssection.dta", replace

end

program merge_time_invariant_transactions
    clear all
    set more off

    cd "$dir_temp/temp_transactions"

    * List of files to merge
    local files transactions_1.dta transactions_2.dta transactions_3.dta transactions_4.dta transactions_5.dta transactions_6.dta transactions_7.dta transactions_8.dta transactions_9.dta transactions_10.dta transactions_11.dta

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use `first', clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        merge 1:1 sptr_target_name sptr_mi_transaction_id using `f'
        drop _merge
    }

    rename AW transaction_geography
    * Save the merged dataset
    save "$dir_cleaned/S&P_cleaned/transactions_crosssection.dta", replace

end
