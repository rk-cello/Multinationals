**** construct property level cross-section data ****
**** notes ****
* some string variables should be converted to numeric
* this script appends each property details data
* historical data is year data, but should be separately treated from time-invariant info
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

* intermediates
global temp_property_level "$dir_temp/property_level"
global temp_company_level "$dir_temp/company_level"
global temp_top_drill "$dir_temp/temp_claims"


************************************************************************
cd "$input_metals_mining/claims"

program main
end

program combine_claims_1
    clear all
    set more off
    
    local statuses "active_granted pending_application"
    local types "mining_lease_licence other_claim_types all_claim_types explo_permit other_claim_types"
    local regions "Africa AsiaPacific Brazil1 Brazil2 Chile EuropeMiddleEast LatinAmericaOthers Peru"
    tempfile temp_all
    save `temp_all', emptyok

    foreach type of local types {
        foreach status of local statuses {
            foreach region of local regions {
                local file claims_1_general_info_`status'_`region'_`type'.xls 
                // Check if file exists; if not, continue to next iteration
                capture confirm file "`file'"
                if _rc != 0 {
                    continue
                }

                // Define base variable names
                local basevars "interval_id hole_id date_rptd rptd_by"

                // Construct new variable names (first two are prop_name and prop_id, rest are 4 x 20 = 80 vars)
                local newnames "prop_name prop_id"
                foreach base of local basevars {
                    forvalues i = 1/20 {
                        local newnames `newnames' `base'_dr`i'
                    }
                }

                // Read main data (from row 7 down)
                import excel "`file'", cellrange(A7) clear
                tostring W-AP BK-CD, replace

                // Rename variables: match old and new names by order
                // (Assumes the number of columns matches old/new names)
                local i = 1
                foreach v of varlist _all {
                    local newname : word `i' of `newnames'
                    rename `v' `newname'
                    local ++i
                }

                // Label fixed vars
                label variable prop_name "Name of the mine or facility"
                label variable prop_id "Unique key for the project"

                // Label financing block vars
                foreach base of local basevars {
                    forvalues i = 1/20 {
                        local varname `base'_dr`i'
                        label variable `varname' "`base' (Drill Result `i')"
                    }
                }
            }
        }
    }

        // Append to the accumulating dataset
        append using `temp_all'
        save `temp_all', replace

    save "$temp_top_drill/top_drill_1_1.dta", replace
end

