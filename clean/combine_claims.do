**** construct property level cross-section data ****
**** notes ****
* some string variables should be converted to numeric
* this script combines claims data in program combine_claims, but may be unnecessary
* claims data linked to properties is also combined. this should be used to construct cross-section data
**** environment ****
clear all
set more off

* directories
global dir_raw "../../../data/raw"
global dir_temp "../../../data/temp"
global dir_cleaned "../../../data/raw_cleaned"

* inputs
global input_metals_mining "$dir_raw/data_S&P/metals_mining"
global input_property_claims "$input_metals_mining/claims"

* intermediates
global temp_property_level "$dir_temp/property_level"
global temp_company_level "$dir_temp/company_level"
global temp_claims "$dir_temp/temp_claims"

************************************************************************


program main
    combine_claims
    combine_claims_linked_to_properties
end

program combine_claims
    clear all
    set more off
    
    local statuses "active_granted pending_application"
    local types "mining_lease_licence other_claim_types all_claim_types explo_permit other_claim_types"
    local regions "Africa AsiaPacific Brazil1 Brazil2 Chile EuropeMiddleEast LatinAmericaOthers Peru"
    tempfile temp_all
    save `temp_all', emptyok
    tempfile imported_file1

    foreach status of local statuses {
        foreach type of local types {
            foreach region of local regions {
                local file1 "$input_metals_mining/claims_1_general_info_`status'_`region'_`type'.xls"
                local file2 "$input_metals_mining/claims_2_general_info_location_`status'_`region'_`type'.xls" 

                // Check if file exists; if not, continue to next iteration
                capture confirm file "`file1'"
                if _rc != 0 {
                    display "File `file1' does not exist, skipping."
                    continue
                }
                capture confirm file "`file2'"
                if _rc != 0 {
                    display "File `file2' does not exist, skipping."
                    continue
                }
                display "Processing: `file1' and `file2'"

                // Read file1 main data (from row 7 down)
                import excel "`file1'", cellrange(A7) clear
                tostring A C-J, replace
                // rename variables
                rename A claim_name
                rename B mining_claim_key
                rename C mining_claim_id
                rename D list_of_owners
                rename E claim_status
                rename F claim_status_rptd
                rename G agency_claim_id
                rename H claim_type
                rename I claim_type_rptd
                rename J commodities
                // Label vars
                label variable claim_name "The reporting jurisdiction's name for the claim"
                label variable mining_claim_key "Mining Claim OID"
                label variable mining_claim_id "Identifier of a mining claim"
                label variable list_of_owners "All owners of the claim including percent ownership"
                label variable claim_status "Status of the claim"
                label variable claim_status_rptd "Status of the claim as reported by jurisdiction"
                label variable agency_claim_id "The reporting jurisdiction's ID for the claim"
                label variable claim_type "Type of claim"
                label variable claim_type_rptd "Type of claim as reported by jurisdiction"
                label variable commodities "Commodities the owner has rights to as reported by jurisdiction"

                save `imported_file1', replace

                // Read main data (from row 7 down)
                import excel "`file2'", cellrange(A7) clear
                tostring A G I-J, replace
                destring C-E H, replace force
                format C-E %tdnn/dd/CCYY, replace
                // rename variables
                rename A claim_name
                rename B mining_claim_key
                rename C application_date
                rename D date_granted
                rename E expiry_date
                rename F source_as_of_date
                rename G source
                rename H claim_area
                rename I country_name
                rename J snl_global_region
                // Label vars
                label variable claim_name "The reporting jurisdiction's name for the claim"
                label variable mining_claim_key "Mining Claim OID"
                label variable application_date "Date as of which the claim activity occurred"
                label variable date_granted "Date as of which the claim activity occurred"
                label variable expiry_date "Date as of which the claim activity occurred"
                label variable source_as_of_date "The date of the most recent filing by the mining jurisdiction"
                label variable source "Complete name of the institution, as specified in its charter"
                label variable claim_area "The reporting jurisdiction's area for the claim"
                label variable country_name "Common English name of a country"
                label variable snl_global_region "Name of the global region"

                merge 1:1 claim_name mining_claim_key using `imported_file1'
                drop _merge
                
                // Append to the accumulating dataset
                append using `temp_all'
                save `temp_all', replace
            }
        }
    }
    save "$temp_claims/claims.dta", replace
end

program combine_claims_linked_to_properties
    clear all
    set more off
    
    local linkfiles "$input_metals_mining/ClaimsLinkedtoProperties_Africa.xlsx" "$input_metals_mining/ClaimsLinkedtoProperties_Asia.xlsx" "$input_metals_mining/ClaimsLinkedtoProperties_LatinAmerica.xlsx" 

    tempfile all_links
    save `all_links', emptyok

    // Loop through each link file, import, rename, label, and append
    local first = 1
    foreach f of local linkfiles {
        import excel "`f'", firstrow clear
        append using `all_links'
        save `all_links', replace
    }

    * Save the merged dataset
    save "$temp_claims/claims_linked_to_properties.dta", replace
end