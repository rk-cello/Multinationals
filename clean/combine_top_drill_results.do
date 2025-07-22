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
global temp_top_drill "$dir_temp/temp_top_drill"


************************************************************************
cd "$input_metals_mining/properties_top_drill_results"

* roadmap
program main
end

program combine_top_drill_1_1
    clear all
    set more off
    
    local regions "Africa China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
        local file top_drill_results_1_drill_hole_interval_info_1_`region'.xls

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

        // Append to the accumulating dataset
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_top_drill/top_drill_1_1.dta", replace
end

program combine_top_drill_1_2
    clear all
    set more off
    
    local regions "Africa China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
        local file top_drill_results_1_drill_hole_interval_info_2_`region'.xls

        // Define base variable names
        local basevars "interval depth explor_purpose significant_interval"

        // Construct new variable names (first two are prop_name and prop_id, rest are 4 x 20 = 80 vars)
        local newnames "prop_name prop_id"
        foreach base of local basevars {
            forvalues i = 1/20 {
                local newnames `newnames' `base'_dr`i'
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
                local varname `base'_dr`i'
                label variable `varname' "`base' (Drill Result `i')"
            }
        }

        // Append to the accumulating dataset
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_top_drill/top_drill_1_2.dta", replace
end

