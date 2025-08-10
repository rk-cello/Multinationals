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
global temp_most_recent_transactions "$dir_temp/temp_most_recent_transactions"


************************************************************************
cd "$input_metals_mining/properties_most_recent_transactions"

* roadmap
program main
    combine_most_recent_transactions_1
    combine_most_recent_transactions_2
    combine_most_recent_transactions_3
    combine_most_recent_transactions_4
    combine_most_recent_transactions_5
end


program combine_most_recent_transactions_1
    clear all
    set more off
    
    local regions "Africa China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
        local file transaction_details_1_`region'.xls

        // Define base variable names
        local basevars "buyer_name_target_name snl_deal_key deal_type target"

        // Construct new variable names (first two are prop_name and prop_id, rest are 4 x 20 = 80 vars)
        local newnames "prop_name prop_id"
        foreach base of local basevars {
            forvalues i = 1/20 {
                local newnames `newnames' `base'_d`i'
            }
        }

        // Read main data (from row 7 down)
        import excel "`file'", cellrange(A7) clear
        tostring C-V AQ-CD, replace

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
                local varname `base'_d`i'
                label variable `varname' "`base' (Deal `i')"
            }
        }

        // Append to the accumulating dataset
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_most_recent_transactions/transaction_details_1.dta", replace
end

program combine_most_recent_transactions_2
    clear all
    set more off
    
    local regions "Africa China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
        local file transaction_details_2_`region'.xls

        // Define base variable names
        local basevars "target_country announce_date deal_status completion_termination_date"

        // Construct new variable names (first two are prop_name and prop_id, rest are 4 x 20 = 80 vars)
        local newnames "prop_name prop_id"
        foreach base of local basevars {
            forvalues i = 1/20 {
                local newnames `newnames' `base'_d`i'
            }
        }

        // Read main data (from row 7 down)
        import excel "`file'", cellrange(A7) clear
        tostring C-V AQ-BJ, replace

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
                local varname `base'_d`i'
                label variable `varname' "`base' (Deal `i')"
            }
        }

        // Append to the accumulating dataset
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_most_recent_transactions/transaction_details_2.dta", replace
end

program combine_most_recent_transactions_3
    clear all
    set more off
    
    local regions "Africa China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
        local file transaction_details_3_`region'.xls

        // Define base variable names
        local basevars "deal_consideration earn_in joint_venture deal_pct_acquired_announce"

        // Construct new variable names (first two are prop_name and prop_id, rest are 4 x 20 = 80 vars)
        local newnames "prop_name prop_id"
        foreach base of local basevars {
            forvalues i = 1/20 {
                local newnames `newnames' `base'_d`i'
            }
        }

        // Read main data (from row 7 down)
        import excel "`file'", cellrange(A7) clear
        tostring C-V AQ-BJ, replace

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
                local varname `base'_d`i'
                label variable `varname' "`base' (Deal `i')"
            }
        }

        // Append to the accumulating dataset
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_most_recent_transactions/transaction_details_3.dta", replace
end

program combine_most_recent_transactions_4
    clear all
    set more off
    
    local regions "Africa China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
        local file transaction_details_4_`region'.xls

        // Define base variable names
        local basevars "eal_pct_acquired_complete total_deal_value_announce total_deal_value_complete rptd_currency_code"

        // Construct new variable names (first two are prop_name and prop_id, rest are 4 x 20 = 80 vars)
        local newnames "prop_name prop_id"
        foreach base of local basevars {
            forvalues i = 1/20 {
                local newnames `newnames' `base'_d`i'
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
                local varname `base'_d`i'
                label variable `varname' "`base' (Deal `i')"
            }
        }

        // Append to the accumulating dataset
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_most_recent_transactions/transaction_details_4.dta", replace
end

program combine_most_recent_transactions_5
    clear all
    set more off
    
    local regions "Africa China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
        local file transaction_details_5_`region'.xls

        // Define base variable names
        local basevars "buyer buyer_country seller seller_country"

        // Construct new variable names (first two are prop_name and prop_id, rest are 4 x 20 = 80 vars)
        local newnames "prop_name prop_id"
        foreach base of local basevars {
            forvalues i = 1/20 {
                local newnames `newnames' `base'_d`i'
            }
        }

        // Read main data (from row 7 down)
        import excel "`file'", cellrange(A7) clear
        tostring C-CD, replace

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
                local varname `base'_d`i'
                label variable `varname' "`base' (Deal `i')"
            }
        }

        // Append to the accumulating dataset
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_most_recent_transactions/transaction_details_5.dta", replace
end

