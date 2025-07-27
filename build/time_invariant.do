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
    merge_time_invariant_prop_details //done
    merge_time_invariant_production //done
    merge_time_invariant_reserves //done
    merge_time_invariant_tech_geo //done
    merge_time_invariant_financings //done
    merge_time_invariant_most_recent_transactions //done
    merge_time_invariant_top_drill_results //done
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
    save "$output_property_crosssection/properties_property_details_crosssection.dta", replace

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
    save "$output_property_crosssection/properties_production_crosssection.dta", replace

end

program merge_time_invariant_reserves

    use "$dir_temp/temp_reserves_resources/RR6.dta"
    save "$output_property_crosssection/properties_reserves_resources_crosssection.dta", replace

end

program merge_time_invariant_tech_geo

    use "$dir_temp/temp_tech_geo/tech_geo.dta"
    save "$output_property_crosssection/properties_technical_geology_crosssection.dta", replace

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
    save "$output_property_crosssection/properties_financings_crosssection.dta", replace

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
    save "$output_property_crosssection/properties_most_recent_transactions_crosssection.dta", replace

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
    save "$output_property_crosssection/properties_top_drill_results_crosssection.dta", replace

end

program merge_time_invariant_claims

    clear all
    set more off

    cd "$dir_temp/temp_claims"

    * List of files to merge
    tempfile merged_claims
    local files claims_1.dta claims_2.dta

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use `first', clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        merge 1:1 mining_claim_key using `f'
        drop _merge
    }

    * Save as a temporary file
    save `merged_claims', replace

    * link to properties
    cd "$input_metals_mining/claims"

    local linkfiles ClaimsLinkedtoProperties_Africa.xlsx ClaimsLinkedtoProperties_Asia.xlsx ClaimsLinkedtoProperties_LatinAmerica.xlsx 

        tempfile all_links
        clear

        // Loop through each link file, import, rename, label, and append
        local first = 1
        foreach f of local linkfiles {
            import excel "`f'", firstrow clear

            // Rename variables A-P to standard names in the specified order
            rename (A B C D E F G H I J K L M N O P) ///
                  (related_property property_key claim_name claim_owners claim_status country_region agency_claim_id claim_type claim_type_rptd commodities claim_area_ha application_date date_granted expiry_date source_as_of_date source)

            // Label variables
            label variable related_property "Related Property"
            label variable property_key "Property Key"
            label variable claim_name "Claim Name"
            label variable claim_owners "Claim Owners"
            label variable claim_status "Claim Status"
            label variable country_region "Country/Region"
            label variable agency_claim_id "Agency Claim ID"
            label variable claim_type "Claim Type"
            label variable claim_type_rptd "Claim Type (Reported)"
            label variable commodities "Commodities"
            label variable claim_area_ha "Claim Area (ha)"
            label variable application_date "Application Date"
            label variable date_granted "Date Granted"
            label variable expiry_date "Expiry Date"
            label variable source_as_of_date "Source As Of Date"
            label variable source "Source"

            // Keep only the variables in the specified order
            //keep related_property property_key claim_name claim_owners claim_status country_region agency_claim_id claim_type claim_type_rptd commodities claim_area_ha application_date date_granted expiry_date source_as_of_date source

            // Append to master
            if `first' {
                save `all_links', replace
                local first = 0
            }
            else {
                append using `all_links'
                save `all_links', replace
            }
        }

        // Merge with claims data
        merge 1:m agency_claim_id using `all_links'
        drop _merge


    * Save the merged dataset
    save "$output_property_crosssection/properties_claims_crosssection.dta", replace    
end