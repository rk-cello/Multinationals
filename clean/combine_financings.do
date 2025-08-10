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
global temp_financings "$dir_temp/temp_financings"


************************************************************************
cd "$input_metals_mining/properties_financings"

* roadmap
program main
    combine_financings_1
    combine_financings_2
    combine_financings_3
    combine_financings_4
end


program combine_financings_1
    clear all
    set more off
    
    local regions "Africa China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
        local file financings_1_most_recent_offerings_1_`region'.xls

        // Define base variable names
        local basevars "snl_offering_key date_announced funding_type description"

        // Construct new variable names (first two are prop_name and prop_id, rest are 4 x 20 = 80 vars)
        local newnames "prop_name prop_id"
        foreach base of local basevars {
            forvalues i = 1/20 {
                local newnames `newnames' `base'_f`i'
            }
        }

        // Read main data (from row 7 down)
        import excel "`file'", cellrange(A7) clear
        tostring AQ-CD, replace

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
                local varname `base'_f`i'
                label variable `varname' "`base' (Financing `i')"
            }
        }

        // Append to the accumulating dataset
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_financings/financings_1_1.dta", replace
end

program combine_financings_2
    clear all
    set more off

    local regions "Africa China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
        local file financings_1_most_recent_offerings_2_`region'.xls

        // Define base variable names
        local basevars "completion_date termination_date amount use_of_proceeds"

        // Construct new variable names (first two are prop_name and prop_id, rest are 4 x 20 = 80 vars)
        local newnames "prop_name prop_id"
        foreach base of local basevars {
            forvalues i = 1/20 {
                local newnames `newnames' `base'_f`i'
            }
        }

        // Read main data (from row 7 down)
        import excel "`file'", cellrange(A7) clear
        tostring BK-CD, replace

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
                local varname `base'_f`i'
                label variable `varname' "`base' (Financing `i')"
            }
        }

        // Append to the accumulating dataset
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_financings/financings_1_2.dta", replace
end

program combine_financings_3
    clear all
    set more off

    local regions "Africa China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
        local file financings_1_most_recent_offerings_3_`region'.xls

        // Define base variable names
        local basevars "flow_through IPO underwritten private_placement"

        // Construct new variable names (first two are prop_name and prop_id, rest are 4 x 20 = 80 vars)
        local newnames "prop_name prop_id"
        foreach base of local basevars {
            forvalues i = 1/20 {
                local newnames `newnames' `base'_f`i'
            }
        }

        // 3. Read main data (from row 7 down)
        import excel "`file'", cellrange(A7) clear

        // 4. Rename variables: match old and new names by order
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
                local varname `base'_f`i'
                label variable `varname' "`base' (Financing `i')"
            }
        }

        // 5. Append to the accumulating dataset
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_financings/financings_1_3.dta", replace
end

program combine_financings_4
    clear all
    set more off

    local regions "Africa China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
        local file financings_2_most_recent_credit_facilities_`region'.xls

        // Define base variable names
        local basevars "snl_funding_key revolving original_issue_date interest_rate original_amount amount_available current_historical term"

        // Construct new variable names (first two are prop_name and prop_id, rest are 4 x 20 = 80 vars)
        local newnames "prop_name prop_id"
        foreach base of local basevars {
            forvalues i = 1/10 {
                local newnames `newnames' `base'_f`i'
            }
        }

        // 3. Read main data (from row 7 down)
        import excel "`file'", cellrange(A7) clear
        tostring BK-BT, replace

        // 4. Rename variables: match old and new names by order
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
            forvalues i = 1/10 {
                local varname `base'_f`i'
                label variable `varname' "`base' (Funding `i')"
            }
        }

        // 5. Append to the accumulating dataset
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_financings/financings_2.dta", replace
end













