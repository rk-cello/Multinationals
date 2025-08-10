**** construct property level cross-section data ****
**** notes ****
* some string variables should be converted to numeric
* this script appends each property details data
* historical data is year data, but should be separately treated from time-invariant info
**** environment ****
clear all
set more off

* directories
global dir_raw "../../../data/raw"
global dir_temp "../../../data/temp"
global dir_cleaned "../../../data/raw_cleaned"

* inputs
global input_metals_mining "$dir_raw/data_S&P/metals_mining"
global input_top_drill "$input_metals_mining/properties_top_drill_results"

* outputs
global output_property_level "$dir_cleaned/property_level"
global output_company_level "$dir_cleaned/company_level"

* intermediates
global temp_property_level "$dir_temp/property_level"
global temp_company_level "$dir_temp/company_level"
global temp_top_drill "$dir_temp/temp_top_drill"


************************************************************************

* roadmap
program main
    combine_top_drill_1_1
    combine_top_drill_1_2
    combine_top_drill_1_3
    combine_top_drill_2_1
    combine_top_drill_2_2
    combine_top_drill_2_3
    combine_top_drill_2_4
    combine_top_drill_3_1
    combine_top_drill_3_2
    combine_top_drill_3_3
    combine_top_drill_3_4
    combine_top_drill_3_bulk_1
    combine_top_drill_3_bulk_2
    combine_top_drill_3_bulk_3
    combine_top_drill_3_bulk_4
    top_drill_3_U3O8
    most_recent_drill_results
end

program combine_top_drill_1_1
    clear all
    set more off
    
    local regions "Africa China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
        local file "$input_top_drill/top_drill_results_1_drill_hole_interval_info_1_`region'.xls"

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
        local file "$input_top_drill/top_drill_results_1_drill_hole_interval_info_2_`region'.xls"

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

program combine_top_drill_1_3
    clear all
    set more off
    
    local regions "Africa China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
        local file "$input_top_drill/top_drill_results_1_drill_hole_interval_info_3_`region'.xls"

        // Define base variable names
        local basevars "primary_interval_commodity interval_commodities interval_value"

        // Construct new variable names (first two are prop_name and prop_id, rest are 4 x 20 = 80 vars)
        local newnames "prop_name prop_id"
        foreach base of local basevars {
            forvalues i = 1/20 {
                local newnames `newnames' `base'_dr`i'
            }
        }

        // Read main data (from row 7 down)
        import excel "`file'", cellrange(A7) clear
        tostring C-AP, replace

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

    save "$temp_top_drill/top_drill_1_3.dta", replace
end

program combine_top_drill_2_1
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific EuropeMiddleEast LatinAmericaOthers Peru"
    tempfile temp_all
    save `temp_all', emptyok

    local metals "gold palladium platinum platinum_g rhodium silver"
    local nmetals : word count `metals'

    foreach region of local regions {
        local file "$input_top_drill/top_drill_results_2_interval_grade_tonne_precious_metals_1_grade_`region'.xls"

        // Read data (from row 7 down)
        import excel "`file'", cellrange(A7) clear

        // Build list of new variable names
        local newnames "prop_name prop_id"
        forvalues i = 1/20 {
            foreach metal of local metals {
                local newnames `newnames' interval_grade_`i'_`metal'
            }
        }

        // Rename variables by order
        local i = 1
        foreach v of varlist _all {
            local newname : word `i' of `newnames'
            rename `v' `newname'
            local ++i
        }

        // Label main variables
        label variable prop_name "Name of the mine or facility"
        label variable prop_id "Unique key for the project"

        // Label all metal-grade variables
        forvalues i = 1/20 {
            local j = 1
            foreach metal of local metals {
                local varname interval_grade_`i'_`metal'
                local metal_label : word `j' of Gold Palladium Platinum "Platinum Group Metals" Rhodium Silver
                label variable `varname' "Grade (g/t) Drill Result `i' - `metal_label'"
                local ++j
            }
        }

        // Append and save
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_top_drill/top_drill_2_1.dta", replace
end

program combine_top_drill_2_2
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific EuropeMiddleEast LatinAmericaOthers Peru"
    tempfile temp_all
    save `temp_all', emptyok

    local metals "gold palladium platinum platinum_g rhodium silver"
    local nmetals : word count `metals'

    foreach region of local regions {
        local file "$input_top_drill/top_drill_results_2_interval_grade_tonne_precious_metals_2_grade_by_interval_`region'.xls"

        // Read data (from row 7 down)
        import excel "`file'", cellrange(A7) clear

        // Build list of new variable names
        local newnames "prop_name prop_id"
        forvalues i = 1/20 {
            foreach metal of local metals {
                local newnames `newnames' grade_x_interval_`i'_`metal'
            }
        }

        // Rename variables by order
        local i = 1
        foreach v of varlist _all {
            local newname : word `i' of `newnames'
            rename `v' `newname'
            local ++i
        }

        // Label main variables
        label variable prop_name "Name of the mine or facility"
        label variable prop_id "Unique key for the project"

        // Label all metal-grade variables
        forvalues i = 1/20 {
            local j = 1
            foreach metal of local metals {
                local varname grade_x_interval_`i'_`metal'
                local metal_label : word `j' of Gold Palladium Platinum "Platinum Group Metals" Rhodium Silver
                label variable `varname' "Grade x Interval (g/t) Drill Result `i' - `metal_label'"
                local ++j
            }
        }

        // Append and save
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_top_drill/top_drill_2_2.dta", replace
end

program combine_top_drill_2_3
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific EuropeMiddleEast LatinAmericaOthers Peru"
    tempfile temp_all
    save `temp_all', emptyok

    local metals "gold palladium platinum platinum_g rhodium silver"
    local nmetals : word count `metals'

    foreach region of local regions {
        local file "$input_top_drill/top_drill_results_2_interval_grade_tonne_precious_metals_3_grade_equivalent_`region'.xls"

        // Read data (from row 7 down)
        import excel "`file'", cellrange(A7) clear

        // Build list of new variable names
        local newnames "prop_name prop_id"
        forvalues i = 1/20 {
            foreach metal of local metals {
                local newnames `newnames' grade_equiv_`i'_`metal'
            }
        }

        // Rename variables by order
        local i = 1
        foreach v of varlist _all {
            local newname : word `i' of `newnames'
            rename `v' `newname'
            local ++i
        }

        // Label main variables
        label variable prop_name "Name of the mine or facility"
        label variable prop_id "Unique key for the project"

        // Label all metal-grade variables
        forvalues i = 1/20 {
            local j = 1
            foreach metal of local metals {
                local varname grade_equiv_`i'_`metal'
                local metal_label : word `j' of Gold Palladium Platinum "Platinum Group Metals" Rhodium Silver
                label variable `varname' "Grade Equivalent (g/t) Drill Result `i' - `metal_label'"
                local ++j
            }
        }

        // Append and save
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_top_drill/top_drill_2_3.dta", replace
end

program combine_top_drill_2_4
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific EuropeMiddleEast LatinAmericaOthers Peru"
    tempfile temp_all
    save `temp_all', emptyok

    local metals "gold palladium platinum platinum_g rhodium silver"
    local nmetals : word count `metals'

    foreach region of local regions {
        local file "$input_top_drill/top_drill_results_2_interval_grade_tonne_precious_metals_4_grade_equivalent_by_interval_`region'.xls"

        // Read data (from row 7 down)
        import excel "`file'", cellrange(A7) clear

        // Build list of new variable names
        local newnames "prop_name prop_id"
        forvalues i = 1/20 {
            foreach metal of local metals {
                local newnames `newnames' grade_equiv_x_int_`i'_`metal'
            }
        }

        // Rename variables by order
        local i = 1
        foreach v of varlist _all {
            local newname : word `i' of `newnames'
            rename `v' `newname'
            local ++i
        }

        // Label main variables
        label variable prop_name "Name of the mine or facility"
        label variable prop_id "Unique key for the project"

        // Label all metal-grade variables
        forvalues i = 1/20 {
            local j = 1
            foreach metal of local metals {
                local varname grade_equiv_x_int_`i'_`metal'
                local metal_label : word `j' of Gold Palladium Platinum "Platinum Group Metals" Rhodium Silver
                label variable `varname' "Grade Equivalent x Interval (g/t) Drill Result `i' - `metal_label'"
                local ++j
            }
        }

        // Append and save
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_top_drill/top_drill_2_4.dta", replace
end

program combine_top_drill_3_1
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    local metals "cobalt copper lead molybdenum nickel tin zinc zinclead"
    local nmetals : word count `metals'

    foreach region of local regions {
        local file "$input_top_drill/top_drill_results_3_interval_grade_percent_base_metals_1_grade_`region'.xls"

        // Read data (from row 7 down)
        import excel "`file'", cellrange(A7) clear

        // Build list of new variable names
        local newnames "prop_name prop_id"
        forvalues i = 1/20 {
            foreach metal of local metals {
                local newnames `newnames' interval_grade_pct_`i'_`metal'
            }
        }

        // Rename variables by order
        local i = 1
        foreach v of varlist _all {
            local newname : word `i' of `newnames'
            rename `v' `newname'
            local ++i
        }

        // Label main variables
        label variable prop_name "Name of the mine or facility"
        label variable prop_id "Unique key for the project"

        // Label all metal-grade variables
        forvalues i = 1/20 {
            local j = 1
            foreach metal of local metals {
                local varname interval_grade_pct_`i'_`metal'
                local metal_label : word `j' of Cobalt Copper Lead Molybdenum Nickel Tin Zinc "Zinc-Lead"
                label variable `varname' "Grade (%) Drill Result `i' - `metal_label'"
                local ++j
            }
        }

        // Append and save
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_top_drill/top_drill_3_1.dta", replace
end

program combine_top_drill_3_2
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    local metals "cobalt copper lead molybdenum nickel tin zinc zinclead"
    local nmetals : word count `metals'

    foreach region of local regions {
        local file "$input_top_drill/top_drill_results_3_interval_grade_percent_base_metals_2_grade_by_interval_`region'.xls"

        // Read data (from row 7 down)
        import excel "`file'", cellrange(A7) clear

        // Build list of new variable names
        local newnames "prop_name prop_id"
        forvalues i = 1/20 {
            foreach metal of local metals {
                local newnames `newnames' grade_x_int_pct_`i'_`metal'
            }
        }

        // Rename variables by order
        local i = 1
        foreach v of varlist _all {
            local newname : word `i' of `newnames'
            rename `v' `newname'
            local ++i
        }

        // Label main variables
        label variable prop_name "Name of the mine or facility"
        label variable prop_id "Unique key for the project"

        // Label all metal-grade variables
        forvalues i = 1/20 {
            local j = 1
            foreach metal of local metals {
                local varname grade_x_int_pct_`i'_`metal'
                local metal_label : word `j' of Cobalt Copper Lead Molybdenum Nickel Tin Zinc "Zinc-Lead"
                label variable `varname' "Grade x Interval (%) Drill Result `i' - `metal_label'"
                local ++j
            }
        }

        // Append and save
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_top_drill/top_drill_3_2.dta", replace
end

program combine_top_drill_3_3
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    local metals "cobalt copper lead molybdenum nickel tin zinc zinclead"
    local nmetals : word count `metals'

    foreach region of local regions {
        local file "$input_top_drill/top_drill_results_3_interval_grade_percent_base_metals_3_grade_equivalent_`region'.xls"

        // Read data (from row 7 down)
        import excel "`file'", cellrange(A7) clear

        // Build list of new variable names
        local newnames "prop_name prop_id"
        forvalues i = 1/20 {
            foreach metal of local metals {
                local newnames `newnames' grade_equiv_pct_`i'_`metal'
            }
        }

        // Rename variables by order
        local i = 1
        foreach v of varlist _all {
            local newname : word `i' of `newnames'
            rename `v' `newname'
            local ++i
        }

        // Label main variables
        label variable prop_name "Name of the mine or facility"
        label variable prop_id "Unique key for the project"

        // Label all metal-grade variables
        forvalues i = 1/20 {
            local j = 1
            foreach metal of local metals {
                local varname grade_equiv_pct_`i'_`metal'
                local metal_label : word `j' of Cobalt Copper Lead Molybdenum Nickel Tin Zinc "Zinc-Lead"
                label variable `varname' "Grade Equivalent (%) Drill Result `i' - `metal_label'"
                local ++j
            }
        }

        // Append and save
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_top_drill/top_drill_3_3.dta", replace
end

program combine_top_drill_3_4
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    local metals "cobalt copper lead molybdenum nickel tin zinc zinclead"
    local nmetals : word count `metals'

    foreach region of local regions {
        local file "$input_top_drill/top_drill_results_3_interval_grade_percent_base_metals_4_grade_equivalent_by_interval_`region'.xls"

        // Read data (from row 7 down)
        import excel "`file'", cellrange(A7) clear

        // Build list of new variable names
        local newnames "prop_name prop_id"
        forvalues i = 1/20 {
            foreach metal of local metals {
                local newnames `newnames' grade_eq_x_int_pct_`i'_`metal'
            }
        }

        // Rename variables by order
        local i = 1
        foreach v of varlist _all {
            local newname : word `i' of `newnames'
            rename `v' `newname'
            local ++i
        }

        // Label main variables
        label variable prop_name "Name of the mine or facility"
        label variable prop_id "Unique key for the project"

        // Label all metal-grade variables
        forvalues i = 1/20 {
            local j = 1
            foreach metal of local metals {
                local varname grade_eq_x_int_pct_`i'_`metal'
                local metal_label : word `j' of Cobalt Copper Lead Molybdenum Nickel Tin Zinc "Zinc-Lead"
                label variable `varname' "Grade Equivalent x Interval (%) Drill Result `i' - `metal_label'"
                local ++j
            }
        }

        // Append and save
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_top_drill/top_drill_3_4.dta", replace
end

program combine_top_drill_3_bulk_1
    clear all
    set more off

    local regions "Africa China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    local metals "ironore lithium manganese niobium phosphate potash titanium tungsten vanadium"
    local nmetals : word count `metals'

    foreach region of local regions {
        local file "$input_top_drill/top_drill_results_3_interval_grade_percent_bulk_specialty_commodities_1_grade_`region'.xls"

        // Read data (from row 7 down)
        import excel "`file'", cellrange(A7) clear

        // Build list of new variable names
        local newnames "prop_name prop_id"
        forvalues i = 1/20 {
            foreach metal of local metals {
                local newnames `newnames' interval_grade_pct_`i'_`metal'
            }
        }

        // Rename variables by order
        local i = 1
        foreach v of varlist _all {
            local newname : word `i' of `newnames'
            rename `v' `newname'
            local ++i
        }

        // Label main variables
        label variable prop_name "Name of the mine or facility"
        label variable prop_id "Unique key for the project"

        // Label all metal-grade variables
        forvalues i = 1/20 {
            local j = 1
            foreach metal of local metals {
                local varname interval_grade_pct_`i'_`metal'
                local metal_label : word `j' of "Iron Ore" Lithium Manganese Niobium Phosphate Potash Titanium Tungsten Vanadium
                label variable `varname' "Grade (%) Drill Result `i' - `metal_label'"
                local ++j
            }
        }

        // Append and save
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_top_drill/top_drill_3_bulk_1.dta", replace
end

program combine_top_drill_3_bulk_2
    clear all
    set more off

    local regions "Africa China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    local metals "ironore lithium manganese niobium phosphate potash titanium tungsten vanadium"
    local nmetals : word count `metals'

    foreach region of local regions {
        local file "$input_top_drill/top_drill_results_3_interval_grade_percent_bulk_specialty_commodities_2_grade_by_interval_`region'.xls"

        // Read data (from row 7 down)
        import excel "`file'", cellrange(A7) clear

        // Build list of new variable names
        local newnames "prop_name prop_id"
        forvalues i = 1/20 {
            foreach metal of local metals {
                local newnames `newnames' grade_x_int_pct_`i'_`metal'
            }
        }

        // Rename variables by order
        local i = 1
        foreach v of varlist _all {
            local newname : word `i' of `newnames'
            rename `v' `newname'
            local ++i
        }

        // Label main variables
        label variable prop_name "Name of the mine or facility"
        label variable prop_id "Unique key for the project"

        // Label all metal-grade variables
        forvalues i = 1/20 {
            local j = 1
            foreach metal of local metals {
                local varname grade_x_int_pct_`i'_`metal'
                local metal_label : word `j' of "Iron Ore" Lithium Manganese Niobium Phosphate Potash Titanium Tungsten Vanadium
                label variable `varname' "Grade x Interval (%) Drill Result `i' - `metal_label'"
                local ++j
            }
        }

        // Append and save
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_top_drill/top_drill_3_bulk_2.dta", replace
end

program combine_top_drill_3_bulk_3
    clear all
    set more off

    local regions "Africa China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    local metals "ironore lithium manganese niobium phosphate potash titanium tungsten vanadium"
    local nmetals : word count `metals'

    foreach region of local regions {
        local file "$input_top_drill/top_drill_results_3_interval_grade_percent_bulk_specialty_commodities_3_grade_equivalent_`region'.xls"

        // Read data (from row 7 down)
        import excel "`file'", cellrange(A7) clear

        // Build list of new variable names
        local newnames "prop_name prop_id"
        forvalues i = 1/20 {
            foreach metal of local metals {
                local newnames `newnames' grade_equiv_pct_`i'_`metal'
            }
        }

        // Rename variables by order
        local i = 1
        foreach v of varlist _all {
            local newname : word `i' of `newnames'
            rename `v' `newname'
            local ++i
        }

        // Label main variables
        label variable prop_name "Name of the mine or facility"
        label variable prop_id "Unique key for the project"

        // Label all metal-grade variables
        forvalues i = 1/20 {
            local j = 1
            foreach metal of local metals {
                local varname grade_equiv_pct_`i'_`metal'
                local metal_label : word `j' of "Iron Ore" Lithium Manganese Niobium Phosphate Potash Titanium Tungsten Vanadium
                label variable `varname' "Grade Equivalent (%) Drill Result `i' - `metal_label'"
                local ++j
            }
        }

        // Append and save
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_top_drill/top_drill_3_bulk_3.dta", replace
end

program combine_top_drill_3_bulk_4
    clear all
    set more off

    local regions "Africa China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    local metals "ironore lithium manganese niobium phosphate potash titanium tungsten vanadium"
    local nmetals : word count `metals'

    foreach region of local regions {
        local file "$input_top_drill/top_drill_results_3_interval_grade_percent_bulk_specialty_commodities_4_grade_equivalent_by_interval_`region'.xls"

        // Read data (from row 7 down)
        import excel "`file'", cellrange(A7) clear

        // Build list of new variable names
        local newnames "prop_name prop_id"
        forvalues i = 1/20 {
            foreach metal of local metals {
                local newnames `newnames' grade_eq_x_int_pct_`i'_`metal'
            }
        }

        // Rename variables by order
        local i = 1
        foreach v of varlist _all {
            local newname : word `i' of `newnames'
            rename `v' `newname'
            local ++i
        }

        // Label main variables
        label variable prop_name "Name of the mine or facility"
        label variable prop_id "Unique key for the project"

        // Label all metal-grade variables
        forvalues i = 1/20 {
            local j = 1
            foreach metal of local metals {
                local varname grade_eq_x_int_pct_`i'_`metal'
                local metal_label : word `j' of "Iron Ore" Lithium Manganese Niobium Phosphate Potash Titanium Tungsten Vanadium
                label variable `varname' "Grade Equivalent x Interval (%) Drill Result `i' - `metal_label'"
                local ++j
            }
        }

        // Append and save
        append using `temp_all'
        save `temp_all', replace
    }

    save "$temp_top_drill/top_drill_3_bulk_4.dta", replace
end

program top_drill_3_U3O8
    clear all
    set more off

    tempfile temp_all
    save `temp_all', emptyok

    local vars "interval_grade_pct grade_x_int_pct grade_equiv_pct grade_eq_x_int_pct"
    local nvars : word count `vars'

    local file "$input_top_drill/top_drill_results_3_interval_grade_percent_U3O8_all_Global.xls"

    // Read data (from row 7 down)
    import excel "`file'", cellrange(A7) clear

    // Build list of new variable names in the requested order
    local newnames "prop_name prop_id"
    foreach var of local vars {
        forvalues i = 1/20 {
            local newnames `newnames' `var'_`i'_U3O8
        }
    }

    // Rename variables by order
    local i = 1
    foreach v of varlist _all {
        local newname : word `i' of `newnames'
        rename `v' `newname'
        local ++i
    }

    // Label main variables
    label variable prop_name "Name of the mine or facility"
    label variable prop_id "Unique key for the project"

    // Label all metal-grade variables
    local labels `"Grade "Grade x Interval" "Grade Equivalent" "Grade Equivalent x Interval"'
    local l = 1
    foreach var of local vars {
        local var_label : word `l' of `labels'
        forvalues i = 1/20 {
            local varname `var'_`i'_U3O8
            label variable `varname' "`var_label' (%) Drill Result `i' - U3O8"
        }
        local ++l
    }

    // Append and save
    append using `temp_all'
    save `temp_all', replace

    save "$temp_top_drill/top_drill_3_U3O8.dta", replace
end

program most_recent_drill_results
    clear all
    set more off

    tempfile temp_all
    save `temp_all', emptyok

    local file "$input_top_drill/most_recent_drill_results_Global.xls"

    // Read data (from row 7 down)
    import excel "`file'", cellrange(A7) clear

    rename A prop_name
    rename B prop_id
    rename C date_most_recent_drill_results
    label variable prop_name "Name of the mine or facility"
    label variable prop_id "Unique key for the project"
    label variable date_most_recent_drill_results "Date of Most Recent Drill Results"

    // Save the cleaned data
    save "$temp_top_drill/most_recent_drill.dta", replace
end


