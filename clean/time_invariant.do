**** combine time invariant data and construct cross-section data ****
**** notes ****
**** environment ****
clear all
set more off

* directories
global dir_raw "../../../data/raw"
global dir_temp "../../../data/temp"
global dir_cleaned "../../../data/raw_cleaned/S&P_cleaned"

* inputs
global input_metals_mining "$dir_raw/data_S&P/metals_mining"

* outputs
global output_property_level "$dir_cleaned/property_level"
global output_company_level "$dir_cleaned/company_level"
global output_properties "$output_property_level/properties"

* intermediates
global temp_prop_details "$dir_temp/temp_prop_details"
global temp_production "$dir_temp/temp_production"
global temp_reserves_resources "$dir_temp/temp_reserves_resources"
global temp_tech_geo "$dir_temp/temp_tech_geo"
global temp_financings "$dir_temp/temp_financings"
global temp_most_recent_transactions "$dir_temp/temp_most_recent_transactions"
global temp_top_drill "$dir_temp/temp_top_drill"
global temp_claims "$dir_temp/temp_claims"
global temp_drill_results "$dir_temp/temp_drill_results"
global temp_transactions "$dir_temp/temp_transactions"

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
    merge_time_invariant 
end

**** PROPERTY DETAILS DATA MERGE ****

program merge_time_invariant_prop_details

    clear all
    set more off

    * List of files to merge
    local files "$temp_prop_details/property_details_1.dta" "$temp_prop_details/property_details_2.dta" "$temp_prop_details/property_details_3_1.dta" "$temp_prop_details/property_details_3_2.dta" "$temp_prop_details/property_details_4.dta" "$temp_prop_details/property_details_5_1.dta" "$temp_prop_details/property_details_5_2.dta" "$temp_prop_details/property_details_5_3.dta" "$temp_prop_details/property_details_5_4.dta" "$temp_prop_details/property_details_5_5.dta" "$temp_prop_details/property_details_5_6.dta" "$temp_prop_details/property_details_5_7.dta" "$temp_prop_details/property_details_5_8.dta" "$temp_prop_details/property_details_5_9.dta" "$temp_prop_details/property_details_5_10.dta" "$temp_prop_details/property_details_5_11.dta" "$temp_prop_details/property_details_5_12.dta" "$temp_prop_details/property_details_5_13.dta" "$temp_prop_details/property_details_5_14.dta" "$temp_prop_details/property_details_5_15.dta" "$temp_prop_details/property_details_7_1.dta" "$temp_prop_details/property_details_7_2.dta" "$temp_prop_details/property_details_7_3.dta" "$temp_prop_details/property_details_8.dta" "$temp_prop_details/property_details_9_1.dta" "$temp_prop_details/property_details_9_2.dta" "$temp_prop_details/property_details_9_3.dta" "$temp_prop_details/property_details_9_4.dta" "$temp_prop_details/property_details_9_5.dta" "$temp_prop_details/property_details_10_1.dta" "$temp_prop_details/property_details_10_2.dta" "$temp_prop_details/property_details_10_3.dta" "$temp_prop_details/property_details_11_1.dta" "$temp_prop_details/property_details_11_2.dta"

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

    * List of files to merge
    local files "$temp_production/production_1.dta" "$temp_production/production_2_1.dta" "$temp_production/production_2_2.dta" "$temp_production/production_2_3.dta" "$temp_production/production_2_4.dta" "$temp_production/production_2_5.dta" "$temp_production/production_2_6.dta"

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

    use "$temp_reserves_resources/RR6.dta"
    save "$output_properties/properties_reserves_resources_crosssection.dta", replace

end

program merge_time_invariant_tech_geo

    use "$temp_tech_geo/tech_geo.dta"
    save "$output_properties/properties_technical_geology_crosssection.dta", replace

end

program merge_time_invariant_financings
    
    clear all
    set more off

    * List of files to merge
    local files "$temp_financings/financings_1_1.dta" "$temp_financings/financings_1_2.dta" "$temp_financings/financings_1_3.dta" "$temp_financings/financings_2.dta"

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

    * List of files to merge
    local files "$temp_most_recent_transactions/transaction_details_1.dta" "$temp_most_recent_transactions/transaction_details_2.dta" "$temp_most_recent_transactions/transaction_details_3.dta" "$temp_most_recent_transactions/transaction_details_4.dta" "$temp_most_recent_transactions/transaction_details_5.dta"

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

    * List of files to merge
    local files "$temp_top_drill/top_drill_1_1.dta" "$temp_top_drill/top_drill_1_2.dta" "$temp_top_drill/top_drill_1_3.dta" "$temp_top_drill/top_drill_2_1.dta" "$temp_top_drill/top_drill_2_2.dta" "$temp_top_drill/top_drill_2_3.dta" "$temp_top_drill/top_drill_2_4.dta" "$temp_top_drill/top_drill_3_1.dta" "$temp_top_drill/top_drill_3_2.dta" "$temp_top_drill/top_drill_3_3.dta" "$temp_top_drill/top_drill_3_4.dta" "$temp_top_drill/top_drill_3_bulk_1.dta" "$temp_top_drill/top_drill_3_bulk_2.dta" "$temp_top_drill/top_drill_3_bulk_3.dta" "$temp_top_drill/top_drill_3_bulk_4.dta" "$temp_top_drill/top_drill_3_U3O8.dta" "$temp_top_drill/most_recent_drill.dta"

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

    use "$temp_claims/claims_linked_to_properties.dta"
    rename RelatedProperty prop_name
    rename PropertyKey prop_id
    tostring prop_id, replace
    save "$output_property_level/claims_crosssection.dta", replace
    
end

program merge_time_invariant_drill_results

    use "$temp_drill_results/drill_results_1&2.dta", clear
    tostring prop_id, replace
    save "$output_property_level/drill_results_crosssection.dta", replace

end

program merge_time_invariant_capital_costs

    use "$temp_drill_results/capital_costs_global.dta", clear
    save "$dir_cleaned/capital_costs_crosssection.dta", replace

end

program merge_time_invariant_transactions
    clear all
    set more off

    * List of files to merge
    
    local files "$temp_transactions/transactions_1.dta" "$temp_transactions/transactions_2.dta" "$temp_transactions/transactions_3.dta" "$temp_transactions/transactions_4.dta" "$temp_transactions/transactions_5.dta" "$temp_transactions/transactions_6.dta" "$temp_transactions/transactions_7.dta" "$temp_transactions/transactions_8.dta" "$temp_transactions/transactions_9.dta" "$temp_transactions/transactions_10.dta" "$temp_transactions/transactions_11.dta"
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
    save "$dir_cleaned/transactions_crosssection.dta", replace

end

program merge_time_invariant 
    clear all
    use "$output_properties/properties_property_details_crosssection.dta"
    merge 1:1 prop_name prop_id using "$output_properties/properties_production_crosssection.dta", nogenerate
    merge 1:1 prop_name prop_id using "$output_properties/properties_reserves_resources_crosssection.dta", nogenerate
    merge 1:1 prop_name prop_id using "$output_properties/properties_technical_geology_crosssection.dta", nogenerate
    merge 1:1 prop_name prop_id using "$output_properties/properties_financings_crosssection.dta", nogenerate
    merge 1:1 prop_name prop_id using "$output_properties/properties_most_recent_transactions_crosssection.dta", nogenerate
    merge 1:1 prop_name prop_id using "$output_properties/properties_top_drill_results_crosssection.dta", nogenerate
    
    //merge 1:1 prop_name prop_id using "$output_property_level/claims_crosssection.dta", nogenerate
    // variables prop_name prop_id do not uniquely identify observations in the using data

    //merge 1:1 prop_name prop_id using "$output_property_level/drill_results_crosssection.dta", nogenerate
    // variables prop_name prop_id do not uniquely identify observations in the using data

    /* merge 1:1 prop_name prop_id using , nogenerate
    merge 1:1 prop_name prop_id using , nogenerate */

    save "$output_property_level/crosssection_data.dta", replace
    // does not include capital costs and transactions 
end