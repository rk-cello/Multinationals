**** construct property level cross-section data ****
**** notes ****
* some string variables should be converted to numeric
* this script appends each production data
* historical data is year data, but should be separately treated from time-invariant info
* same variable names are used for Ore, Commodity, etc. should be renamed to add prefix later
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
global temp_production "$dir_temp/temp_production"

************************************************************************
cd "$input_metals_mining/properties_production"

* roadmap
program main
    combine_production_1
    combine_production_2_1
    combine_production_2_2
    combine_production_2_3
    combine_production_2_4
    combine_production_2_5
    combine_production_2_6
    combine_production_3_1
    combine_production_3_2
    combine_production_3_3
    combine_production_3_4
    combine_production_3_5
    combine_production_3_6
    combine_production_3_7
    combine_production_4_1
    combine_production_4_2
    combine_production_4_3
    combine_production_4_4
    combine_production_4_5
    combine_production_4_6
    combine_production_4_7
    combine_production_4_8
    combine_production_4_9
    combine_production_4_10
    combine_production_4_11
    combine_production_4_12
    combine_production_4_13
    combine_production_4_14
    combine_production_4_15
    combine_production_4_16
    combine_production_4_17
    combine_production_4_18
    combine_production_4_19
    combine_production_4_20
    combine_production_4_21
    combine_production_4_22
    combine_production_4_23
    combine_production_4_24
    combine_production_4_25
    combine_production_5_1
    combine_production_5_2
end


program combine_production_1
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_1_ore_capacity_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * Rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  start_up_yr
    rename D  start_up_calendar_qtr
    rename E  proj_start_up_yr
    rename F  proj_start_up_qtr
    rename G  actual_closure_yr
    rename H  actual_closure_qtr
    rename I  proj_closure_yr
    rename J  proj_closure_qtr
    rename K  mill_capacity_tonnes_per_day
    rename L  mill_capacity_tonnes_per_year
    rename M  mill_capacity_cubic_m_per_day
    rename N  mill_capacity_cubic_m_per_year
    rename O  stripping_ratio
    rename P  waste_to_ore_ratio
    rename Q  production_cost_comments
    rename R  mining_produ_general_comments // shortened to fit
    rename S  mining_methods
    rename T  processing_methods
    rename U  production_forms
    rename V  mining_method1
    rename W  mining_method2
    rename X  mining_method3
    rename Y  mining_method4
    rename Z  mining_method5
    rename AA processing_method1
    rename AB processing_method2
    rename AC processing_method3
    rename AD processing_method4
    rename AE processing_method5
    rename AF mining_processing_cost_mt // shortened to fit
    rename AG mining_processing_cost_cubic_m // shortened to fit

    * Label variables
    label var prop_name                          "Name of the mine or facility"
    label var prop_id                            "Unique key for the project"
    label var start_up_yr                        "Actual start-up year"
    label var start_up_calendar_qtr             "Actual start-up calendar quarter"
    label var proj_start_up_yr                  "Projected start-up year"
    label var proj_start_up_qtr                 "Projected start-up calendar quarter"
    label var actual_closure_yr                 "Actual closure year"
    label var actual_closure_qtr                "Actual closure calendar quarter"
    label var proj_closure_yr                   "Projected closure year"
    label var proj_closure_qtr                  "Projected closure calendar quarter"
    label var mill_capacity_tonnes_per_day      "Mill capacity (tonnes/day)"
    label var mill_capacity_tonnes_per_year     "Mill capacity (tonnes/year)"
    label var mill_capacity_cubic_m_per_day     "Mill capacity (cubic meters/day)"
    label var mill_capacity_cubic_m_per_year    "Mill capacity (cubic meters/year)"
    label var stripping_ratio                   "Stripping ratio"
    label var waste_to_ore_ratio                "Waste-to-ore ratio"
    label var production_cost_comments          "Production cost comments"
    label var mining_produ_general_comments "Mining production general comments"
    label var mining_methods                    "Mining methods"
    label var processing_methods                "Processing methods"
    label var production_forms                  "Production forms"
    label var mining_method1                    "Mining method 1"
    label var mining_method2                    "Mining method 2"
    label var mining_method3                    "Mining method 3"
    label var mining_method4                    "Mining method 4"
    label var mining_method5                    "Mining method 5"
    label var processing_method1                "Processing method 1"
    label var processing_method2                "Processing method 2"
    label var processing_method3                "Processing method 3"
    label var processing_method4                "Processing method 4"
    label var processing_method5                "Processing method 5"
    label var mining_processing_cost_mt "Mining & processing costs per metric tonne"
    label var mining_processing_cost_cubic_m "Mining & processing costs per cubic meter"

    save "$temp_production/production_1.dta", replace
end


**** Commodity Capacity *********************************

program combine_production_2_1
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local metals "gold palladium platinum silver"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_2_commodity_capacity_precious_metals_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear

            * Rename variables for each metal
            rename A  prop_name
            rename B  prop_id
            rename C  millhead_grade_g_per_t_gold // shortened to fit
            rename D  millhead_grade_g_per_t_palladium // shortened to fit
            rename E  millhead_grade_g_per_t_platinum // shortened to fit
            rename F  millhead_grade_g_per_t_silver // shortened to fit

            rename G  recov_rate_gold
            rename H  recov_rate_palladium
            rename I  recov_rate_platinum
            rename J  recov_rate_silver

            rename K  production_capacity_oz_gold
            rename L  production_capacity_oz_palladium
            rename M  production_capacity_oz_platinum
            rename N  production_capacity_oz_silver

            rename O  cash_cost_per_oz_gold
            rename P  cash_cost_per_oz_palladium
            rename Q  cash_cost_per_oz_platinum
            rename R  cash_cost_per_oz_silver

            rename S  total_prod_cost_per_oz_gold // shortened to fit
            rename T  total_prod_cost_per_oz_palladium // shortened to fit
            rename U  total_prod_cost_per_oz_platinum // shortened to fit
            rename V  total_prod_cost_per_oz_silver // shortened to fit

            rename W  all_sustain_cost_oz_gold // shortened to fit
            rename X  all_sustain_cost_oz_palladium // shortened to fit
            rename Y  all_sustain_cost_oz_platinum // shortened to fit
            rename Z  all_sustain_cost_oz_silver // shortened to fit

            append using `temp_file'
            save `temp_file', replace
        }
    }

    * Label variables
    label var prop_name                          "Name of the mine or facility"
    label var prop_id                            "Unique key for the project"

    foreach metal of local metals {
        label var millhead_grade_g_per_t_`metal'    "Commodity concentration contained in material processed at facility, g/tonne (`metal')"
        label var recov_rate_`metal'                   "Quantity of commodity produced as perc of commodity in material processed (`metal')"
        label var production_capacity_oz_`metal'       "Quantity of commodity produced (`metal')"
        label var cash_cost_per_oz_`metal'             "Variable per unit cost associated with mining, processing and refining of commodity (`metal')"
        label var total_prod_cost_per_oz_`metal' "Variable per unit cost associated with production of the commodity (`metal')"
        label var all_sustain_cost_oz_`metal' "Total per unit cost associated with sustaining production levels of a commodity (`metal')"
    }

    save "$temp_production/production_2_1.dta", replace
end



program combine_production_2_2
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local metals "cobalt copper ferromolybdenum ferronickel lead molybdenum nickel tin zinc"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_2_commodity_capacity_base_metals_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear

            append using `temp_file'
            save `temp_file', replace
        }
    }

            rename A  prop_name
            rename B  prop_id
            label var prop_name                          "Name of the mine or facility"
            label var prop_id                            "Unique key for the project"

            unab vars : C-K
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "millhead_grade_pct_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Commodity concentration contained in material processed at facility, percent (`metal')"

                local ++i
            }

            unab vars : L-T
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "recov_rate_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Quantity of commodity produced as percent of commodity in material processed (`metal')"

                local ++i
            }

            unab vars : U-AC
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "production_capacity_t_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Quantity of commodity produced (`metal')"

                local ++i
            }

            unab vars : AD-AL
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "cash_cost_per_lb_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Variable per unit cost associated with mining, processing and refining of commodity (`metal')"

                local ++i
            }

            unab vars : AM-AU
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "total_prod_cost_per_lb_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname'  "Variable per unit cost associated with production of the commodity (`metal')"

                local ++i
            }

            unab vars : AV-BD
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_sustain_cost_per_lb_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with sustaining production levels of a commodity (`metal')"

                local ++i
            }
        
            save "$temp_production/production_2_2.dta", replace
end

program combine_production_2_3
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local commodities "alumina aluminum bauxite chromite chromium coal ferrochrome ferromanganese iron_ore manganese phosphate potash"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_2_commodity_capacity_bulk_commodities_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear

            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id
    label var prop_name                          "Name of the mine or facility"
    label var prop_id                            "Unique key for the project"

    * Millhead Grade Percent
    unab vars : C-N
    local i = 1
    foreach oldname of local vars {
        local commodity : word `i' of `commodities'
        local newname = "millhead_grade_pct_`commodity'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Concentration of commodity contained in material processed through the facility, percent (`commodity')"

        local ++i
    }

    * Recovery Rate
    unab vars : O-Z
    local i = 1
    foreach oldname of local vars {
        local commodity : word `i' of `commodities'
        local newname = "recov_rate_`commodity'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Quantity of commodity produced as a percent of the quantity of commodity contained in the material processed (`commodity')"

        local ++i
    }

    * Production Capacity (tonne)
    unab vars : AA-AL
    local i = 1
    foreach oldname of local vars {
        local commodity : word `i' of `commodities'
        local newname = "production_capacity_t_`commodity'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Quantity of commodity produced (`commodity')"

        local ++i
    }

    * Cash Cost per Tonne
    unab vars : AM-AX
    local i = 1
    foreach oldname of local vars {
        local commodity : word `i' of `commodities'
        local newname = "cash_cost_per_t_`commodity'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Variable per unit cost associated with mining, processing and refining of the commodity, less applicable credits for by-products (`commodity')"

        local ++i
    }

    * Total Production Cost per Tonne
    unab vars : AY-BJ
    local i = 1
    foreach oldname of local vars {
        local commodity : word `i' of `commodities'
        local newname = "total_prod_cost_per_t_`commodity'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Variable per unit cost associated with production of the commodity. Includes cash costs plus depreciation and amortization (`commodity')"

        local ++i
    }

    * All-in Sustaining Cost per Tonne
    unab vars : BK-BV
    local i = 1
    foreach oldname of local vars {
        local commodity : word `i' of `commodities'
        local newname = "all_sustain_cost_per_t_`commodity'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Total per unit cost associated with sustaining production levels of a commodity; may include general and administrative costs, reclamation and remediation at operating sites, exploration and research, development, and sustaining capital expenditure (`commodity')"

        local ++i
    }

    save "$temp_production/production_2_3.dta", replace
end


program combine_production_2_4
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local commodities "antimony ferrotungsten ferrovanadium graphite heavy_mineral_sands ilmenite lanthanides lithium niobium rutile scandium tantalum titanium tungsten vanadium yttrium zircon"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_2_commodity_capacity_specialty_commodities_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear

            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id
    label var prop_name                          "Name of the mine or facility"
    label var prop_id                            "Unique key for the project"

    * Millhead Grade Percent
    unab vars : C-S
    local i = 1
    foreach oldname of local vars {
        local commodity : word `i' of `commodities'
        local newname = "millhead_grade_pct_`commodity'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Concentration of commodity contained in material processed through the facility, percent (`commodity')"

        local ++i
    }

    * Recovery Rate
    unab vars : T-AJ
    local i = 1
    foreach oldname of local vars {
        local commodity : word `i' of `commodities'
        local newname = "recov_rate_`commodity'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Quantity of commodity produced as a percent of the quantity of commodity contained in the material processed (`commodity')"

        local ++i
    }

    * Production Capacity (tonne)
    unab vars : AK-BA
    local i = 1
    foreach oldname of local vars {
        local commodity : word `i' of `commodities'
        local newname = "production_capacity_t_`commodity'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Quantity of commodity produced (`commodity')"

        local ++i
    }

    * Cash Cost per Tonne
    unab vars : BB-BR
    local i = 1
    foreach oldname of local vars {
        local commodity : word `i' of `commodities'
        local newname = "cash_cost_per_t_`commodity'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Variable per unit cost associated with mining, processing and refining of the commodity, less applicable credits for by-products (`commodity')"

        local ++i
    }

    * Cash Cost per Pound
    unab vars : BS-CI
    local i = 1
    foreach oldname of local vars {
        local commodity : word `i' of `commodities'
        local newname = "cash_cost_per_lb_`commodity'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Variable per unit cost associated with mining, processing and refining of the commodity, less applicable credits for by-products (`commodity')"

        local ++i
    }

    * Total Production Cost per Tonne
    unab vars : CJ-CZ
    local i = 1
    foreach oldname of local vars {
        local commodity : word `i' of `commodities'
        local newname = "total_prod_cost_per_t_`commodity'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Variable per unit cost associated with production of the commodity. Includes cash costs plus depreciation and amortization (`commodity')"

        local ++i
    }

    * Total Production Cost per Pound
    unab vars : DA-DQ
    local i = 1
    foreach oldname of local vars {
        local commodity : word `i' of `commodities'
        local newname = "total_prod_cost_per_lb_`commodity'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Variable per unit cost associated with production of the commodity. Includes cash costs plus depreciation and amortization (`commodity')"

        local ++i
    }

    * All-in Sustaining Cost per Tonne
    unab vars : DR-EH
    local i = 1
    foreach oldname of local vars {
        local commodity : word `i' of `commodities'
        local newname = "all_sustain_cost_per_t_`commodity'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Total per unit cost associated with sustaining production levels of a commodity; may include general and administrative costs, reclamation and remediation at operating sites, exploration and research, development, and sustaining capital expenditure (`commodity')"

        local ++i
    }

    * All-in Sustaining Cost per Pound
    unab vars : EI-EY
    local i = 1
    foreach oldname of local vars {
        local commodity : word `i' of `commodities'
        local newname = "all_sustain_cost_per_lb_`commodity'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Total per unit cost associated with sustaining production levels of a commodity; may include general and administrative costs, reclamation and remediation at operating sites, exploration and research, development, and sustaining capital expenditure (`commodity')"

        local ++i
    }

    save "$temp_production/production_2_4.dta", replace
end


program combine_production_2_5
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_2_commodity_capacity_diamonds_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring B, replace

            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id
    label var prop_name                          "Name of the mine or facility"
    label var prop_id                            "Unique key for the project"

    rename C  millhead_grade_pct_diamonds 
    label var millhead_grade_pct_diamonds "Concentration of commodity contained in material processed through facility, percent (diamonds)"
    rename D  recov_rate_diamonds 
    label var recov_rate_diamonds "Quantity of commodity produced as a percent of the quantity of commodity contained in the material processed (diamonds)"
    rename E  production_capacity_ct_diamonds 
    label var production_capacity_ct_diamonds "Quantity of commodity produced (diamonds)"
    rename F  cash_cost_per_ct_diamonds 
    label var cash_cost_per_ct_diamonds "Variable per unit cost associated with mining, processing and refining of the commodity (diamonds)"
    rename G  total_prod_cost_per_ct_diamonds // shortened to fit
    label var total_prod_cost_per_ct_diamonds "Variable per unit cost associated with production of the commodity (diamonds)"
    rename H  all_sustain_cost_per_ct_diamonds // shortened to fit
    label var all_sustain_cost_per_ct_diamonds "Total per unit cost associated with sustaining production levels of a commodity (diamonds)"

    save "$temp_production/production_2_5.dta", replace
end


program combine_production_2_6
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_2_commodity_capacity_U3O8_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring B, replace

            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id
    label var prop_name                          "Name of the mine or facility"
    label var prop_id                            "Unique key for the project"

    rename C  millhead_grade_pct_U3O8 
    label var millhead_grade_pct_U3O8 "Concentration of commodity contained in material processed through facility, percent (U3O8)"
    rename D  recov_rate_U3O8 
    label var recov_rate_U3O8 "Quantity of commodity produced as a percent of the quantity of commodity contained in the material processed (U3O8)"
    rename E  production_capacity_lb_U3O8 
    label var production_capacity_lb_U3O8 "Quantity of commodity produced (U3O8)"
    rename F  cash_cost_per_lb_U3O8 // shortened to fit
    label var cash_cost_per_lb_U3O8 "Variable per unit cost associated with mining, processing and refining of the commodity (U3O8)"
    rename G  total_prod_cost_per_lb_U3O8 // shortened to fit
    label var total_prod_cost_per_lb_U3O8 "Variable per unit cost associated with production of the commodity (U3O8)"
    rename H  all_sustain_cost_per_lb_U3O8 // shortened to fit
    label var all_sustain_cost_per_lb_U3O8 "Total per unit cost associated with sustaining production levels of a commodity (U3O8)"

    save "$temp_production/production_2_6.dta", replace
end


**** Ore Production and Costs *********************************
program combine_production_3_1
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_3_ore_production_costs_1_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            //tostring C-AI, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    * Rename variables for years 2023 to 1991
    local year = 2023
    foreach var of varlist C-AI {
        local newname = "mining_process_cost_per_t_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    * Add labels for renamed variables
    local year = 2023
    foreach var of varlist mining_process_cost_per_t_2023-mining_process_cost_per_t_1991 {
        label var `var' "Total per unit cost associated with mining and processing ore (`year')"
        local year = `year' - 1
    }

    save "$temp_production/production_3_1.dta", replace

    // wide to long
    clear
    use "$temp_production/production_3_1.dta"
    
    reshape long mining_process_cost_per_t_, i(prop_name prop_id) j(year)
    rename mining_process_cost_per_t_ mining_process_cost_per_t
    label var mining_process_cost_per_t "Total per unit cost associated with mining and processing ore"

    save "$temp_production/production_3_1.dta", replace

end


program combine_production_3_2
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_3_ore_production_costs_2_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            //tostring C-AI, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    * Rename variables for years 2023 to 1991
    local year = 2023
    foreach var of varlist C-AI {
        local newname = "mining_process_cost_cubic_m_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    * Add labels for renamed variables
    local year = 2023
    foreach var of varlist mining_process_cost_cubic_m_2023-mining_process_cost_cubic_m_1991 {
        label var `var' "Total per unit cost associated with mining and processing ore (`year')"
        local year = `year' - 1
    }

    save "$temp_production/production_3_2.dta", replace

    // wide to long
    clear
    use "$temp_production/production_3_2.dta"
    
    reshape long mining_process_cost_cubic_m_, i(prop_name prop_id) j(year)
    rename mining_process_cost_cubic_m_ mining_process_cost_cubic_m
    label var mining_process_cost_cubic_m "Total per unit cost associated with mining and processing ore"

    save "$temp_production/production_3_2.dta", replace
end
    

program combine_production_3_3
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_3_ore_production_costs_3_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            //tostring C-AI, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    * Rename variables for years 2023 to 1991
    local year = 2023
    foreach var of varlist C-AI {
        local newname = "production_cost_comments_`year'" 
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    * Add labels for renamed variables
    local year = 2023
    foreach var of varlist production_cost_comments_2023-production_cost_comments_1991 {
        label var `var' "Comments expanding on or clarifying production cost data (`year')"
        local year = `year' - 1
    }

    save "$temp_production/production_3_3.dta", replace

    // wide to long
    clear
    use "$temp_production/production_3_3.dta"
    
    reshape long production_cost_comments_, i(prop_name prop_id) j(year)
    rename production_cost_comments_ production_cost_comments
    label var production_cost_comments "Comments expanding on or clarifying production cost data"

    save "$temp_production/production_3_3.dta", replace
end


program combine_production_3_4
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_3_ore_production_costs_4_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    * Rename variables for years 2023 to 1991
    local year = 2023
    foreach var of varlist C-AI {
        local newname = "ore_processed_mass_`year'" 
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    * Add labels for renamed variables
    local year = 2023
    foreach var of varlist ore_processed_mass_2023-ore_processed_mass_1991 {
        label var `var' "Quantity of material processed through the facility (`year')"
        local year = `year' - 1
    }

    save "$temp_production/production_3_4.dta", replace

    // wide to long
    clear
    use "$temp_production/production_3_4.dta"
    
    reshape long ore_processed_mass_, i(prop_name prop_id) j(year)
    rename ore_processed_mass_ ore_processed_mass
    label var ore_processed_mass "Quantity of material processed through the facility"

    save "$temp_production/production_3_4.dta", replace
end


program combine_production_3_5
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_3_ore_production_costs_5_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    * Rename variables for years 2023 to 1991
    local year = 2023
    foreach var of varlist C-AI {
        local newname = "ore_processed_volume_`year'" 
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    * Add labels for renamed variables
    local year = 2023
    foreach var of varlist ore_processed_volume_2023-ore_processed_volume_1991 {
        label var `var' "Quantity of material processed through the facility (`year')"
        local year = `year' - 1
    }

    save "$temp_production/production_3_5.dta", replace

    // wide to long
    clear
    use "$temp_production/production_3_5.dta"
    
    reshape long ore_processed_volume_, i(prop_name prop_id) j(year)
    rename ore_processed_volume_ ore_processed_volume
    label var ore_processed_volume "Quantity of material processed through the facility"

    save "$temp_production/production_3_5.dta", replace
end


program combine_production_3_6
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_3_ore_production_costs_6_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    * Rename variables for years 2023 to 1991
    local year = 2023
    foreach var of varlist C-AI {
        local newname = "mining_production_comments_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    * Add labels for renamed variables
    local year = 2023
    foreach var of varlist mining_production_comments_2023-mining_production_comments_1991 {
        label var `var' "General comments that apply to a mining processing facility (`year')"
        local year = `year' - 1
    }

    save "$temp_production/production_3_6.dta", replace

    // wide to long
    clear
    use "$temp_production/production_3_6.dta"
    
    reshape long mining_production_comments_, i(prop_name prop_id) j(year)
    rename mining_production_comments_ mining_production_comments
    label var mining_production_comments "General comments that apply to a mining processing facility"

    save "$temp_production/production_3_6.dta", replace
end


program combine_production_3_7
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_3_ore_production_costs_7_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    * Rename variables for years 2023 to 1991
    local year = 2023
    foreach var of varlist C-AI {
        local newname = "production_certainty_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    * Add labels for renamed variables
    local year = 2023
    foreach var of varlist production_certainty_2023-production_certainty_1991 {
        label var `var' "Production certainty (`year')"
        local year = `year' - 1
    }

    save "$temp_production/production_3_7.dta", replace

    // wide to long
    clear
    use "$temp_production/production_3_7.dta"
    
    reshape long production_certainty_, i(prop_name prop_id) j(year)
    rename production_certainty_ production_certainty
    label var production_certainty "Production certainty"

    save "$temp_production/production_3_7.dta", replace
end


**** Commodity Production and Costs *********************************
program combine_production_4_1
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_actual_estimate_forecast_1991_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    * Rename variables for years 2023 to 1991
    local year = 2023
    foreach var of varlist C-AI {
        local newname = "production_certainty_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    * Add labels for renamed variables
    local year = 2023
    foreach var of varlist production_certainty_2023-production_certainty_1991 {
        label var `var' "Production certainty (`year')"
        local year = `year' - 1
    }

    save "$temp_production/production_4_1.dta", replace

    // wide to long
    clear
    use "$temp_production/production_4_1.dta"
    
    reshape long production_certainty_, i(prop_name prop_id) j(year)
    rename production_certainty_ production_certainty
    label var production_certainty "Production certainty"

    save "$temp_production/production_4_1.dta", replace
end


program combine_production_4_2
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_all_costs_diamonds_1991_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring B, replace
            keep A-ED
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id
    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

 * cash cost
    local year = 2023
    foreach var of varlist C-AI {
        local newname = "cash_cost_per_ct_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }
    
    local year = 2023
    foreach var of varlist cash_cost_per_ct_2023-cash_cost_per_ct_1991 {
        label var `var' "Variable per unit cost associated with mining, processing, refining of commodity (`year')"
        local year = `year' - 1
    }

 * total production cost
    local year = 2023
    foreach var of varlist AJ-BP {
        local newname = "total_prod_cost_per_ct_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }
    
    local year = 2023
    foreach var of varlist total_prod_cost_per_ct_2023-total_prod_cost_per_ct_1991 {
        label var `var' "Variable per unit cost associated with production of commodity (`year')"
        local year = `year' - 1
    }

 * all-in sustaining cost
    local year = 2023
    foreach var of varlist BQ-CW {
        local newname = "all_sustain_cost_ct_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }
    
    local year = 2023
    foreach var of varlist all_sustain_cost_ct_2023-all_sustain_cost_ct_1991 {
        label var `var' "Total per unit cost associated with sustaining production levels of commodity (`year')"
        local year = `year' - 1
    }

 * all-in cost
    local year = 2023
    foreach var of varlist CX-ED {
        local newname = "all_cost_ct_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }
    
    local year = 2023
    foreach var of varlist all_cost_ct_2023-all_cost_ct_1991 {
        label var `var' "Total per unit cost associated with production of commodity (`year')"
        local year = `year' - 1
    }

 // wide to long
    reshape long cash_cost_per_ct_ total_prod_cost_per_ct_ all_sustain_cost_ct_ all_cost_ct_, i(prop_name prop_id) j(year)
    rename cash_cost_per_ct_ cash_cost_per_ct
    rename total_prod_cost_per_ct_ total_prod_cost_per_ct
    rename all_sustain_cost_ct_ all_sustain_cost_ct
    rename all_cost_ct_ all_cost_ct

    label var cash_cost_per_ct "Variable per unit cost associated with mining, processing, refining of commodity"
    label var total_prod_cost_per_ct "Variable per unit cost associated with production of commodity"
    label var all_sustain_cost_ct "Total per unit cost associated with sustaining production levels of commodity"
    label var all_cost_ct "Total per unit cost associated with production of commodity"

 // add suffix to variable names
    //clear
    //use "$temp_production/production_4_2.dta"

    local suffix "diamonds"
    foreach var of varlist cash_cost_per_ct total_prod_cost_per_ct all_sustain_cost_ct all_cost_ct {
        local newname = "`var'_`suffix'"
        rename `var' `newname'
        label var `newname' "`: variable label `var'' (`suffix')"
    }
    
    save "$temp_production/production_4_2.dta", replace
end

program combine_production_4_3
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_all_costs_U3O8_1991_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            keep A-ED
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id
    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    * cash cost
    local year = 2023
    foreach var of varlist C-AI {
        local newname = "cash_cost_per_lb_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }
    
    local year = 2023
    foreach var of varlist cash_cost_per_lb_2023-cash_cost_per_lb_1991 {
        label var `var' "Variable per unit cost associated with mining, processing, refining of commodity (`year')"
        local year = `year' - 1
    }

    * total production cost
    local year = 2023
    foreach var of varlist AJ-BP {
        local newname = "total_prod_cost_per_lb_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }
    
    local year = 2023
    foreach var of varlist total_prod_cost_per_lb_2023-total_prod_cost_per_lb_1991 {
        label var `var' "Variable per unit cost associated with production of commodity (`year')"
        local year = `year' - 1
    }

    * all-in sustaining cost
    local year = 2023
    foreach var of varlist BQ-CW {
        local newname = "all_sustain_cost_lb_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }
    
    local year = 2023
    foreach var of varlist all_sustain_cost_lb_2023-all_sustain_cost_lb_1991 {
        label var `var' "Total per unit cost associated with sustaining production levels of commodity (`year')"
        local year = `year' - 1
    }

    * all-in cost
    local year = 2023
    foreach var of varlist CX-ED {
        local newname = "all_cost_lb_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }
    
    local year = 2023
    foreach var of varlist all_cost_lb_2023-all_cost_lb_1991 {
        label var `var' "Total per unit cost associated with production of commodity (`year')"
        local year = `year' - 1
    }

    // wide to long
    reshape long cash_cost_per_lb_ total_prod_cost_per_lb_ all_sustain_cost_lb_ all_cost_lb_, i(prop_name prop_id) j(year)
    rename cash_cost_per_lb_ cash_cost_per_lb
    rename total_prod_cost_per_lb_ total_prod_cost_per_lb
    rename all_sustain_cost_lb_ all_sustain_cost_lb
    rename all_cost_lb_ all_cost_lb

    label var cash_cost_per_lb "Variable per unit cost associated with mining, processing, refining of commodity"
    label var total_prod_cost_per_lb "Variable per unit cost associated with production of commodity"
    label var all_sustain_cost_lb "Total per unit cost associated with sustaining production levels of commodity"
    label var all_cost_lb "Total per unit cost associated with production of commodity"

    // add suffix to variable names
    local suffix "U3O8"
    foreach var of varlist cash_cost_per_lb total_prod_cost_per_lb all_sustain_cost_lb all_cost_lb {
        local newname = "`var'_`suffix'"
        rename `var' `newname'
        label var `newname' "`: variable label `var'' (`suffix')"
    }
    save "$temp_production/production_4_3.dta", replace

end

program combine_production_4_4
    local regions "Africa EmergingAsiaPacific LatinAmerica"
    local metals "cobalt copper ferromolybdenum ferronickel lead molybdenum nickel tin zinc"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_all_in_costs_base_metals_1991_2005_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id
    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

 * all in cost
    local year = 2005
    unab vars : C-K
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }
    
    local year = `year' - 1
    unab vars : L-T
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars : U-AC
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  AD-AL
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  AM-AU
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  AV-BD
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  BE-BM
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  BN-BV
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars : BW-CE
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  CF-CN
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  CO-CW
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  CX-DF
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :   DG-DO
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :   DP-DX
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  DY-EG
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }



 // wide to long
    * 1. Reshape to long by metal-year combined
    reshape long all_in_cost_t_, i(prop_name prop_id) j(metal_year) string
    * 2. Split metal_year into year and metal
    split metal_year, parse("_")
    rename metal_year1 year
    rename metal_year2 base_metal
    label var all_in_cost_t_ "Total per unit cost associated with production of commodity"

    replace base_metal = "ferromolybdenum" if base_metal == "ferromolybden"
    drop metal_year

    //save "$temp_production/production_4_4.dta", replace

 // long to wide
    reshape wide all_in_cost_t_, i(prop_name prop_id year) j(base_metal) string
    label var all_in_cost_t_cobalt "Total per unit cost associated with production of commodity (cobalt)"
    label var all_in_cost_t_copper "Total per unit cost associated with production of commodity (copper)"
    label var all_in_cost_t_ferromolybdenum "Total per unit cost associated with production of commodity (ferromolybdenum)"
    label var all_in_cost_t_ferronickel "Total per unit cost associated with production of commodity (ferronickel)"
    label var all_in_cost_t_lead "Total per unit cost associated with production of commodity (lead)"
    label var all_in_cost_t_molybdenum "Total per unit cost associated with production of commodity (molybdenum)"
    label var all_in_cost_t_nickel "Total per unit cost associated with production of commodity (nickel)"
    label var all_in_cost_t_tin "Total per unit cost associated with production of commodity (tin)"
    label var all_in_cost_t_zinc "Total per unit cost associated with production of commodity (zinc)"
    
    save "$temp_production/production_4_4.dta", replace
    
end


program combine_production_4_5
    local regions "Africa EmergingAsiaPacific LatinAmerica"
    local metals "cobalt copper ferromolybdenum ferronickel lead molybdenum nickel tin zinc"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_all_in_costs_base_metals_2006_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id
    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

 * all in cost
    local year = 2023
    unab vars : C-K
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }
    
    local year = `year' - 1
    unab vars : L-T
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars : U-AC
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  AD-AL
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  AM-AU
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  AV-BD
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  BE-BM
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  BN-BV
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars : BW-CE
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  CF-CN
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  CO-CW
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  CX-DF
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :   DG-DO
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :   DP-DX
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  DY-EG
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars : EH-EP
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  EQ-EY
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }

    local year = `year' - 1
    unab vars :  EZ-FH
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "all_in_cost_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`metal')"

                local ++i
            }


    save "$temp_production/production_4_5.dta", replace

 // wide to long
    clear
    use "$temp_production/production_4_5.dta"

    * 1. Reshape to long by metal-year combined
    reshape long all_in_cost_t_, i(prop_name prop_id) j(metal_year) string

    * 2. Split metal_year into year and metal
    split metal_year, parse("_")
    rename metal_year1 year
    rename metal_year2 base_metal

    //rename all_in_cost_t_ all_in_cost_t
    label var all_in_cost_t_ "Total per unit cost associated with production of commodity"

    replace base_metal = "ferromolybdenum" if base_metal == "ferromolybden"
    drop metal_year

 // long to wide
    reshape wide all_in_cost_t_, i(prop_name prop_id year) j(base_metal) string
    label var all_in_cost_t_cobalt "Total per unit cost associated with production of commodity (cobalt)"
    label var all_in_cost_t_copper "Total per unit cost associated with production of commodity (copper)"
    label var all_in_cost_t_ferromolybdenum "Total per unit cost associated with production of commodity (ferromolybdenum)"
    label var all_in_cost_t_ferronickel "Total per unit cost associated with production of commodity (ferronickel)"
    label var all_in_cost_t_lead "Total per unit cost associated with production of commodity (lead)"
    label var all_in_cost_t_molybdenum "Total per unit cost associated with production of commodity (molybdenum)"
    label var all_in_cost_t_nickel "Total per unit cost associated with production of commodity (nickel)"
    label var all_in_cost_t_tin "Total per unit cost associated with production of commodity (tin)"
    label var all_in_cost_t_zinc "Total per unit cost associated with production of commodity (zinc)"

    save "$temp_production/production_4_5.dta", replace

end

program combine_production_4_6
    local regions "Africa China EmergingAsiaPacificOthers LatinAmerica"
    local commodities "alumina aluminum bauxite chromite chromium coal ferrochrome ferromanganese iron_ore manganese phosphate potash"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_all_in_costs_bulk_commodities_1992_2007_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id
    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

  * all in cost
    local year = 2007
    unab vars : C-N
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : O-Z
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }
    
    local year = `year' - 1
    unab vars : AA-AL
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : AM-AX
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : AY-BJ
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : BK-BV
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : BW-CH
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : CI-CT
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : CU-DF
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : DG-DR
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : DS-ED
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : EE-EP
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }
    
    local year = `year' - 1
    unab vars : EQ-FB
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }
    
    local year = `year' - 1
    unab vars : FC-FN
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : FO-FZ
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : GA-GL
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    save "$temp_production/production_4_6.dta", replace

 // wide to long
    clear
    use "$temp_production/production_4_6.dta"

    * 1. Reshape to long by metal-year combined
    reshape long all_in_cost_t_, i(prop_name prop_id) j(commodity_year) string

    * 2. Split commodity_year into year and commodity
    split commodity_year, parse("_")
    rename commodity_year1 year
    rename commodity_year2 commodity

    label var all_in_cost_t_ "Total per unit cost associated with production of commodity"

    replace commodity = "iron_ore" if commodity == "iron"
    replace commodity = "ferromanganese" if commodity == "ferromanganes"
    drop commodity_year commodity_year3

    reshape wide all_in_cost_t_, i(prop_name prop_id year) j(commodity) string
    label var all_in_cost_t_alumina "Total per unit cost associated with production of commodity (alumina)"
    label var all_in_cost_t_aluminum "Total per unit cost associated with production of commodity (aluminum)"
    label var all_in_cost_t_bauxite "Total per unit cost associated with production of commodity (bauxite)"
    label var all_in_cost_t_chromite "Total per unit cost associated with production of commodity (chromite)"
    label var all_in_cost_t_chromium "Total per unit cost associated with production of commodity (chromium)"
    label var all_in_cost_t_coal "Total per unit cost associated with production of commodity (coal)"
    label var all_in_cost_t_ferrochrome "Total per unit cost associated with production of commodity (ferrochrome)"
    label var all_in_cost_t_ferromanganese "Total per unit cost associated with production of commodity (ferromanganese)"
    label var all_in_cost_t_iron_ore "Total per unit cost associated with production of commodity (iron ore)"
    label var all_in_cost_t_manganese "Total per unit cost associated with production of commodity (manganese)"
    label var all_in_cost_t_phosphate "Total per unit cost associated with production of commodity (phosphate)"
    label var all_in_cost_t_potash "Total per unit cost associated with production of commodity (potash)"

    save "$temp_production/production_4_6.dta", replace
end
    
program combine_production_4_7
    local regions "Africa China EmergingAsiaPacificOthers LatinAmerica"
    local commodities "alumina aluminum bauxite chromite chromium coal ferrochrome ferromanganese iron_ore manganese phosphate potash"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_all_in_costs_bulk_commodities_2008_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id
    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

 * all in cost
    local year = 2023
    unab vars : C-N
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : O-Z
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }
    
    local year = `year' - 1
    unab vars : AA-AL
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : AM-AX
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : AY-BJ
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : BK-BV
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : BW-CH
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : CI-CT
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : CU-DF
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : DG-DR
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : DS-ED
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : EE-EP
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }
    
    local year = `year' - 1
    unab vars : EQ-FB
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }
    
    local year = `year' - 1
    unab vars : FC-FN
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : FO-FZ
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    local year = `year' - 1
    unab vars : GA-GL
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "all_in_cost_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')" 

                local ++i
            }

    save "$temp_production/production_4_7.dta", replace

 // wide to long
    clear
    use "$temp_production/production_4_7.dta"

    * 1. Reshape to long by metal-year combined
    reshape long all_in_cost_t_, i(prop_name prop_id) j(commodity_year) string

    * 2. Split commodity_year into year and commodity
    split commodity_year, parse("_")
    rename commodity_year1 year
    rename commodity_year2 commodity

    //rename all_in_cost_t_ all_in_cost_t
    label var all_in_cost_t_ "Total per unit cost associated with production of commodity"

    replace commodity = "iron_ore" if commodity == "iron"
    replace commodity = "ferromanganese" if commodity == "ferromanganes"
    drop commodity_year commodity_year3

    //save "$temp_production/production_4_7.dta", replace

    //clear
    //use "$temp_production/production_4_7.dta"
    //replace commodity = "iron_ore" if commodity == "iron ore"
    //rename all_in_cost_t all_in_cost_t_
    reshape wide all_in_cost_t_, i(prop_name prop_id year) j(commodity) string
    label var all_in_cost_t_alumina "Total per unit cost associated with production of commodity (alumina)"
    label var all_in_cost_t_aluminum "Total per unit cost associated with production of commodity (aluminum)"
    label var all_in_cost_t_bauxite "Total per unit cost associated with production of commodity (bauxite)"
    label var all_in_cost_t_chromite "Total per unit cost associated with production of commodity (chromite)"
    label var all_in_cost_t_chromium "Total per unit cost associated with production of commodity (chromium)"
    label var all_in_cost_t_coal "Total per unit cost associated with production of commodity (coal)"
    label var all_in_cost_t_ferrochrome "Total per unit cost associated with production of commodity (ferrochrome)"
    label var all_in_cost_t_ferromanganese "Total per unit cost associated with production of commodity (ferromanganese)"
    label var all_in_cost_t_iron_ore "Total per unit cost associated with production of commodity (iron ore)"
    label var all_in_cost_t_manganese "Total per unit cost associated with production of commodity (manganese)"
    label var all_in_cost_t_phosphate "Total per unit cost associated with production of commodity (phosphate)"
    label var all_in_cost_t_potash "Total per unit cost associated with production of commodity (potash)"
    save "$temp_production/production_4_7.dta", replace


end

program combine_production_4_8
    local regions "Africa Brazil LatinAmericaOthers Peru" // EmergingAsiaPacific has different structure
    local metals "gold palladium platinum silver"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    import excel "production_4_commodity_production_costs_all_in_costs_precious_metals_1991_2023_EmergingAsiaPacific.xls", cellrange(A7) clear
    drop C
    local oldcols "D E F G H I J K L M N O P Q R S T U V W X Y Z AA AB AC AD AE AF AG AH AI AJ AK AL AM AN AO AP AQ AR AS AT AU AV AW AX AY AZ BA BB BC BD BE BF BG BH BI BJ BK BL BM BN BO BP BQ BR BS BT BU BV BW BX BY BZ CA CB CC CD CE CF CG CH CI CJ CK CL CM CN CO CP CQ CR CS CT CU CV CW CX CY CZ DA DB DC DD DE DF DG DH DI DJ DK DL DM DN DO DP DQ DR DS DT DU DV DW DX DY DZ EA EB EC ED EE"
    local newcols "C D E F G H I J K L M N O P Q R S T U V W X Y Z AA AB AC AD AE AF AG AH AI AJ AK AL AM AN AO AP AQ AR AS AT AU AV AW AX AY AZ BA BB BC BD BE BF BG BH BI BJ BK BL BM BN BO BP BQ BR BS BT BU BV BW BX BY BZ CA CB CC CD CE CF CG CH CI CJ CK CL CM CN CO CP CQ CR CS CT CU CV CW CX CY CZ DA DB DC DD DE DF DG DH DI DJ DK DL DM DN DO DP DQ DR DS DT DU DV DW DX DY DZ EA EB EC ED"

    local i = 1
    foreach oldcol of local oldcols {
        local newcol : word `i' of `newcols'
        rename `oldcol' `newcol'
        local i = `i' + 1
    }
    save `temp_file', replace

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_all_in_costs_precious_metals_1991_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id
    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    * all in cost
    local year = 2023
    unab vars : C-ED
    local i = 1
    foreach oldname of local vars {
        local metal : word `i' of `metals'
        local newname = "all_in_cost_oz_`year'_`metal'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Total per unit cost associated with production of metal in `year' (`metal')"

        local i = `i' + 1
        if (`i' > 4) {
            local i = 1
            local year = `year' - 1
        }
    }

    // wide to long

    * 1. Reshape to long by metal-year combined
    reshape long all_in_cost_oz_, i(prop_name prop_id) j(metal_year) string

    * 2. Split metal_year into year and metal
    split metal_year, parse("_")
    rename metal_year1 year
    rename metal_year2 precious_metal
    drop metal_year

    reshape wide all_in_cost_oz_, i(prop_name prop_id year) j(precious_metal) string
    label var all_in_cost_oz_gold "Total per unit cost associated with production of gold"
    label var all_in_cost_oz_palladium "Total per unit cost associated with production of palladium"
    label var all_in_cost_oz_platinum "Total per unit cost associated with production of platinum"
    label var all_in_cost_oz_silver "Total per unit cost associated with production of silver"

    save "$temp_production/production_4_8.dta", replace
end

program combine_production_4_9
    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

 // 1991-2001
    local commodities "alumina aluminum bauxite chromite chromium coal ferrochrome ferromanganese iron_ore manganese phosphate potash"
    import excel "production_4_commodity_production_costs_all_in_costs_specialty_commodities_1991_2001_AfricaEmergingAsiaPacificLatinAmerica.xls", cellrange(A7) clear

    rename A  prop_name
    rename B  prop_id
    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    local year = 2001
    unab vars : C-ED
    local i = 1
    foreach oldname of local vars {
        local commodity : word `i' of `commodities'
        local newname = "all_in_cost_t_`year'_`commodity'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')"

        local i = `i' + 1
        if (`i' > 12) {
            local i = 1
            local year = `year' - 1
        }
    }

    * 1. Reshape to long by commodity-year combined
    reshape long all_in_cost_t_, i(prop_name prop_id) j(commodity_year) string

    * 2. Split commodity_year into year and commodity
    split commodity_year, parse("_")
    rename commodity_year1 year
    rename commodity_year2 commodity

    replace commodity = "iron_ore" if commodity == "iron"
    replace commodity = "ferromanganese" if commodity == "ferromanganes"
    drop commodity_year commodity_year3

    save temp_file, replace

 // 2002_2012
    clear
    local commodities "alumina aluminum bauxite chromite chromium coal ferrochrome ferromanganese iron_ore manganese phosphate potash"
    import excel "production_4_commodity_production_costs_all_in_costs_specialty_commodities_2002_2012_AfricaEmergingAsiaPacificLatinAmerica.xls", cellrange(A7) clear

    rename A  prop_name
    rename B  prop_id
    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    local year = 2012
    unab vars : C-ED
    local i = 1
    foreach oldname of local vars {
        local commodity : word `i' of `commodities'
        local newname = "all_in_cost_t_`year'_`commodity'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')"

        local i = `i' + 1
        if (`i' > 12) {
            local i = 1
            local year = `year' - 1
        }
    }

    * 1. Reshape to long by commodity-year combined
    reshape long all_in_cost_t_, i(prop_name prop_id) j(commodity_year) string

    * 2. Split commodity_year into year and commodity
    split commodity_year, parse("_")
    rename commodity_year1 year
    rename commodity_year2 commodity

    replace commodity = "iron_ore" if commodity == "iron"
    replace commodity = "ferromanganese" if commodity == "ferromanganes"
    drop commodity_year commodity_year3
    
    append using temp_file
    save temp_file, replace

 // 2013_2023
    clear
    local commodities "antimony ferrotungsten ferrovanadium graphite heavy_mineral_sands ilmenite lanthanides lithium niobium rutile scandium tantalum titanium tungsten vanadium yttrium zircon"
    import excel "production_4_commodity_production_costs_all_in_costs_specialty_commodities_2013_2023_AfricaEmergingAsiaPacificLatinAmerica.xls", cellrange(A7) clear

    rename A  prop_name
    rename B  prop_id
    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    local year = 2023
    unab vars : C-GG
    local i = 1
    foreach oldname of local vars {
        local commodity : word `i' of `commodities'
        local newname = "all_in_cost_t_`year'_`commodity'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Total per unit cost associated with production of commodity in `year' (`commodity')"

        local i = `i' + 1
        if (`i' > 17) {
            local i = 1
            local year = `year' - 1
        }
    }

    * 1. Reshape to long by commodity-year combined
    reshape long all_in_cost_t_, i(prop_name prop_id) j(commodity_year) string

    * 2. Split commodity_year into year and commodity
    split commodity_year, parse("_")
    rename commodity_year1 year
    rename commodity_year2 commodity
    replace commodity = "heavy_mineral" if commodity == "heavy"
    drop commodity_year commodity_year3
    
    append using temp_file
    save temp_file, replace

    //save "$temp_production/production_4_9.dta", replace

 // long to wide
    //clear
    //use "$temp_production/production_4_9.dta"
    //rename all_in_cost_t all_in_cost_t_
    //replace commodity = "heavy_mineral" if commodity == "heavy_mineral_sands"

    reshape wide all_in_cost_t_, i(prop_name prop_id year) j(commodity) string

    label var all_in_cost_t_antimony "Total per unit cost associated with production of antimony"
    label var all_in_cost_t_ferrotungsten "Total per unit cost associated with production of ferrotungsten"
    label var all_in_cost_t_ferrovanadium "Total per unit cost associated with production of ferrovanadium"
    label var all_in_cost_t_graphite "Total per unit cost associated with production of graphite"
    label var all_in_cost_t_heavy_mineral "Total per unit cost associated with production of heavy mineral sands"
    label var all_in_cost_t_ilmenite "Total per unit cost associated with production of ilmenite"
    label var all_in_cost_t_lanthanides "Total per unit cost associated with production of lanthanides"
    label var all_in_cost_t_lithium "Total per unit cost associated with production of lithium"
    label var all_in_cost_t_niobium "Total per unit cost associated with production of niobium"
    label var all_in_cost_t_rutile "Total per unit cost associated with production of rutile"
    label var all_in_cost_t_scandium "Total per unit cost associated with production of scandium"
    label var all_in_cost_t_tantalum "Total per unit cost associated with production of tantalum"
    label var all_in_cost_t_titanium "Total per unit cost associated with production of titanium"
    label var all_in_cost_t_tungsten "Total per unit cost associated with production of tungsten"
    label var all_in_cost_t_vanadium "Total per unit cost associated with production of vanadium"
    label var all_in_cost_t_yttrium "Total per unit cost associated with production of yttrium"
    label var all_in_cost_t_zircon "Total per unit cost associated with production of zircon"
    label var all_in_cost_t_alumina "Total per unit cost associated with production of alumina"
    label var all_in_cost_t_aluminum "Total per unit cost associated with production of aluminum
    label var all_in_cost_t_bauxite "Total per unit cost associated with production of bauxite"
    label var all_in_cost_t_chromite "Total per unit cost associated with production of chromite"
    label var all_in_cost_t_chromium "Total per unit cost associated with production of chromium"
    label var all_in_cost_t_coal "Total per unit cost associated with production of coal"
    label var all_in_cost_t_ferrochrome "Total per unit cost associated with production of ferrochrome"
    label var all_in_cost_t_ferromanganese "Total per unit cost associated with production of ferromanganese"
    label var all_in_cost_t_iron_ore "Total per unit cost associated with production of iron ore"
    label var all_in_cost_t_manganese "Total per unit cost associated with production of manganese"
    label var all_in_cost_t_phosphate "Total per unit cost associated with production of phosphate"
    label var all_in_cost_t_potash "Total per unit cost associated with production of potash"

    save "$temp_production/production_4_9.dta", replace
end

program combine_production_4_10
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_all_production_diamonds_1991_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring B, replace
            keep A-ED
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id
    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    * millhead_grade_ct_per_t
    local year = 2023
    foreach var of varlist C-AI {
        local newname = "millhead_grade_ct_per_t_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }
    
    local year = 2023
    foreach var of varlist millhead_grade_ct_per_t_2023-millhead_grade_ct_per_t_1991 {
        label var `var' "Concentration of commodity contained in material processed through the facility (`year')"
        local year = `year' - 1
    }

    * recov_rate 
    local year = 2023
    foreach var of varlist AJ-BP {
        local newname = "recov_rate_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }
    
    local year = 2023
    foreach var of varlist recov_rate_2023-recov_rate_1991 {
        label var `var' "Quantity of commodity produced as a percent of the commodity in the material processed (`year')"
        local year = `year' - 1
    }

    * commodity_production_ct
    local year = 2023
    foreach var of varlist BQ-CW {
        local newname = "commodity_production_ct_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }
    
    local year = 2023
    foreach var of varlist commodity_production_ct_2023-commodity_production_ct_1991 {
        label var `var' "Quantity of commodity produced (`year')"
        local year = `year' - 1
    }

    * rptd_equiv
    local year = 2023
    foreach var of varlist CX-ED {
        local newname = "rptd_equiv_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }
    
    local year = 2023
    foreach var of varlist rptd_equiv_2023-rptd_equiv_1991 {
        label var `var' "Indicates that production is expressed on an equivalency basis (`year')"
        local year = `year' - 1
    }

    //save "$temp_production/production_4_10.dta", replace

    // wide to long
    //clear
    //use "$temp_production/production_4_10.dta"
    
    reshape long millhead_grade_ct_per_t_ recov_rate_ commodity_production_ct_ rptd_equiv_, i(prop_name prop_id) j(year)
    rename millhead_grade_ct_per_t_ millhead_grade_ct_t_diamonds
    rename recov_rate_ recov_rate_diamonds
    rename commodity_production_ct_ commodity_prod_ct_diamonds
    rename rptd_equiv_ rptd_equiv_diamonds

    label var millhead_grade_ct_t_diamonds "Concentration of commodity contained in material processed through facility (diamonds)"
    label var recov_rate_diamonds "Quantity of commodity produced as percent of commodity in material processed (diamonds)"
    label var commodity_prod_ct_diamonds "Quantity of commodity produced (diamonds)"
    label var rptd_equiv_diamonds "Indicates that production is expressed on an equivalency basis (diamonds)"

    save "$temp_production/production_4_10.dta", replace
end
    
program combine_production_4_11
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_all_production_U3O8_1991_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring B, replace
            keep A-ED
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id
    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    * millhead_grade_pct
    local year = 2023
    foreach var of varlist C-AI {
        local newname = "millhead_grade_pct_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }
    
    local year = 2023
    foreach var of varlist millhead_grade_pct_2023-millhead_grade_pct_1991 {
        label var `var' "Concentration of commodity contained in material processed through the facility (`year')"
        local year = `year' - 1
    }

    * recov_rate 
    local year = 2023
    foreach var of varlist AJ-BP {
        local newname = "recov_rate_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }
    
    local year = 2023
    foreach var of varlist recov_rate_2023-recov_rate_1991 {
        label var `var' "Quantity of commodity produced as a percent of the commodity in the material processed (`year')"
        local year = `year' - 1
    }

    * commodity_production_lb
    local year = 2023
    foreach var of varlist BQ-CW {
        local newname = "commodity_production_lb_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }
    
    local year = 2023
    foreach var of varlist commodity_production_lb_2023-commodity_production_lb_1991 {
        label var `var' "Quantity of commodity produced (`year')"
        local year = `year' - 1
    }

    * rptd_equiv
    local year = 2023
    foreach var of varlist CX-ED {
        local newname = "rptd_equiv_`year'" // shortened to fit
        rename `var' `newname'
        local year = `year' - 1
    }
    
    local year = 2023
    foreach var of varlist rptd_equiv_2023-rptd_equiv_1991 {
        label var `var' "Indicates that production is expressed on an equivalency basis (`year')"
        local year = `year' - 1
    }

    //save "$temp_production/production_4_11.dta", replace

    // wide to long
    //clear
    //use "$temp_production/production_4_11.dta"
    
    reshape long millhead_grade_pct_ recov_rate_ commodity_production_lb_ rptd_equiv_, i(prop_name prop_id) j(year)
    rename millhead_grade_pct_ millhead_grade_pct_U3O8
    rename recov_rate_ recov_rate_U3O8
    rename commodity_production_lb_ commodity_production_lb_U3O8
    rename rptd_equiv_ rptd_equiv_U3O8

    label var millhead_grade_pct_U3O8 "Concentration of commodity contained in material processed through facility (U3O8)"
    label var recov_rate_U3O8 "Quantity of commodity produced as percent of commodity in material processed (U3O8)"
    label var commodity_production_lb_U3O8 "Quantity of commodity produced (U3O8)"
    label var rptd_equiv_U3O8 "Indicates that production is expressed on an equivalency basis (U3O8)"

    save "$temp_production/production_4_11.dta", replace
end

program combine_production_4_12
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local metals "cobalt copper ferromolybdenum ferronickel lead molybdenum nickel tin zinc"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_commodity_production_base_metals_1991_1999_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear

            append using `temp_file'
            save `temp_file', replace
        }
    }

            rename A  prop_name
            rename B  prop_id
            label var prop_name                          "Name of the mine or facility"
            label var prop_id                            "Unique key for the project"

            local year = 1999
            unab vars : C-CE
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "commodity_prod_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Quantity of commodity produced in `year' (`metal')"

                local i = `i' + 1
                if (`i' > 9) {
                    local i = 1
                    local year = `year' - 1
                }
            }

            * 1. Reshape to long
            reshape long commodity_prod_t_, i(prop_name prop_id) j(metal_year) string

            * 2. Split
            split metal_year, parse("_")
            rename metal_year1 year
            rename metal_year2 base_metal

            replace base_metal = "ferromolybdenum" if base_metal == "ferromolyb"
            replace base_metal = "ferronickel" if base_metal == "ferronicke"
            label var commodity_prod_t_ "Quantity of commodity produced"
            drop metal_year

            //clear
            //use "$temp_production/production_4_12.dta"
            //rename commodity_prod_t commodity_prod_t_
            /*
            reshape wide commodity_prod_t_, i(prop_name prop_id year) j(base_metal) string
            //rename commodity_prod_t_ferronicke commodity_prod_t_ferronickel
            label var commodity_prod_t_cobalt "Quantity of commodity produced (cobalt)"
            label var commodity_prod_t_copper "Quantity of commodity produced (copper)"
            label var commodity_prod_t_ferromolybdenum "Quantity of commodity produced (ferromolybdenum)"
            label var commodity_prod_t_ferronickel "Quantity of commodity produced (ferronickel)"
            label var commodity_prod_t_lead "Quantity of commodity produced (lead)"
            label var commodity_prod_t_molybdenum "Quantity of commodity produced (molybdenum)"
            label var commodity_prod_t_nickel "Quantity of commodity produced (nickel)"
            label var commodity_prod_t_tin "Quantity of commodity produced (tin)"
            label var commodity_prod_t_zinc "Quantity of commodity produced (zinc)"
            */

            save "$temp_production/production_4_12.dta", replace
end

program combine_production_4_13
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local metals "cobalt copper ferromolybdenum ferronickel lead molybdenum nickel tin zinc"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_commodity_production_base_metals_2000_2008_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear

            append using `temp_file'
            save `temp_file', replace
        }
    }

            rename A  prop_name
            rename B  prop_id
            label var prop_name                          "Name of the mine or facility"
            label var prop_id                            "Unique key for the project"

            local year = 2008
            unab vars : C-CE
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "commodity_prod_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Quantity of commodity produced in `year' (`metal')"

                local i = `i' + 1
                if (`i' > 9) {
                    local i = 1
                    local year = `year' - 1
                }
            }

            * 1. Reshape to long
            reshape long commodity_prod_t_, i(prop_name prop_id) j(metal_year) string

            * 2. Split
            split metal_year, parse("_")
            rename metal_year1 year
            rename metal_year2 base_metal

            replace base_metal = "ferromolybdenum" if base_metal == "ferromolyb"
            replace base_metal = "ferronickel" if base_metal == "ferronicke"
            label var commodity_prod_t_ "Quantity of commodity produced"
            drop metal_year

            /*
            reshape wide commodity_prod_t_, i(prop_name prop_id year) j(base_metal) string
            rename commodity_prod_t_ferronicke commodity_prod_t_ferronickel
            label var commodity_prod_t_cobalt "Quantity of commodity produced (cobalt)"
            label var commodity_prod_t_copper "Quantity of commodity produced (copper)"
            label var commodity_prod_t_ferromolybdenum "Quantity of commodity produced (ferromolybdenum)"
            label var commodity_prod_t_ferronickel "Quantity of commodity produced (ferronickel)"
            label var commodity_prod_t_lead "Quantity of commodity produced (lead)"
            label var commodity_prod_t_molybdenum "Quantity of commodity produced (molybdenum)"
            label var commodity_prod_t_nickel "Quantity of commodity produced (nickel)"
            label var commodity_prod_t_tin "Quantity of commodity produced (tin)"
            label var commodity_prod_t_zinc "Quantity of commodity produced (zinc)"
            */

            save "$temp_production/production_4_13.dta", replace
end

program combine_production_4_14
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local metals "cobalt copper ferromolybdenum ferronickel lead molybdenum nickel tin zinc"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_commodity_production_base_metals_2009_2017_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear

            append using `temp_file'
            save `temp_file', replace
        }
    }

            rename A  prop_name
            rename B  prop_id
            label var prop_name                          "Name of the mine or facility"
            label var prop_id                            "Unique key for the project"

            local year = 2017
            unab vars : C-CE
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "commodity_prod_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Quantity of commodity produced in `year' (`metal')"

                local i = `i' + 1
                if (`i' > 9) {
                    local i = 1
                    local year = `year' - 1
                }
            }

            * 1. Reshape to long
            reshape long commodity_prod_t_, i(prop_name prop_id) j(metal_year) string

            * 2. Split
            split metal_year, parse("_")
            rename metal_year1 year
            rename metal_year2 base_metal

            replace base_metal = "ferromolybdenum" if base_metal == "ferromolyb"
            replace base_metal = "ferronickel" if base_metal == "ferronicke"
            label var commodity_prod_t_ "Quantity of commodity produced"
            drop metal_year

            /*
            clear
            use "$temp_production/production_4_14.dta"
            //rename commodity_prod_t commodity_prod_t_
            reshape wide commodity_prod_t_, i(prop_name prop_id year) j(base_metal) string
            rename commodity_prod_t_ferronicke commodity_prod_t_ferronickel
            label var commodity_prod_t_cobalt "Quantity of commodity produced (cobalt)"
            label var commodity_prod_t_copper "Quantity of commodity produced (copper)"
            label var commodity_prod_t_ferromolybdenum "Quantity of commodity produced (ferromolybdenum)"
            label var commodity_prod_t_ferronickel "Quantity of commodity produced (ferronickel)"
            label var commodity_prod_t_lead "Quantity of commodity produced (lead)"
            label var commodity_prod_t_molybdenum "Quantity of commodity produced (molybdenum)"
            label var commodity_prod_t_nickel "Quantity of commodity produced (nickel)"
            label var commodity_prod_t_tin "Quantity of commodity produced (tin)"
            label var commodity_prod_t_zinc "Quantity of commodity produced (zinc)"
            */

            save "$temp_production/production_4_14.dta", replace
end

program combine_production_4_15
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local metals "cobalt copper ferromolybdenum ferronickel lead molybdenum nickel tin zinc"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_commodity_production_base_metals_2018_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear

            append using `temp_file'
            save `temp_file', replace
        }
    }

            rename A  prop_name
            rename B  prop_id
            label var prop_name                          "Name of the mine or facility"
            label var prop_id                            "Unique key for the project"

            local year = 2023
            unab vars : C-BD
            local i = 1
            foreach oldname of local vars {
                local metal : word `i' of `metals'
                local newname = "commodity_prod_t_`year'_`metal'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Quantity of commodity produced in `year' (`metal')"

                local i = `i' + 1
                if (`i' > 9) {
                    local i = 1
                    local year = `year' - 1
                }
            }

            * 1. Reshape to long
            reshape long commodity_prod_t_, i(prop_name prop_id) j(metal_year) string

            * 2. Split
            split metal_year, parse("_")
            rename metal_year1 year
            rename metal_year2 base_metal

            replace base_metal = "ferromolybdenum" if base_metal == "ferromolyb"
            replace base_metal = "ferronickel" if base_metal == "ferronicke"
            label var commodity_prod_t_ "Quantity of commodity produced"
            drop metal_year

            /*
            //clear
            //use "$temp_production/production_4_15.dta"
            //rename commodity_prod_t commodity_prod_t_
            reshape wide commodity_prod_t_, i(prop_name prop_id year) j(base_metal) string
            rename commodity_prod_t_ferronicke commodity_prod_t_ferronickel
            label var commodity_prod_t_cobalt "Quantity of commodity produced (cobalt)"
            label var commodity_prod_t_copper "Quantity of commodity produced (copper)"
            label var commodity_prod_t_ferromolybdenum "Quantity of commodity produced (ferromolybdenum)"
            label var commodity_prod_t_ferronickel "Quantity of commodity produced (ferronickel)"
            label var commodity_prod_t_lead "Quantity of commodity produced (lead)"
            label var commodity_prod_t_molybdenum "Quantity of commodity produced (molybdenum)"
            label var commodity_prod_t_nickel "Quantity of commodity produced (nickel)"
            label var commodity_prod_t_tin "Quantity of commodity produced (tin)"
            label var commodity_prod_t_zinc "Quantity of commodity produced (zinc)"
            */

            save "$temp_production/production_4_15.dta", replace
        
end

program combine_production_4_16
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local commodities "alumina aluminum bauxite chromite chromium coal ferrochrome ferromanganese iron_ore manganese phosphate potash"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_commodity_production_bulk_commodities_1992_1999_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear

            append using `temp_file'
            save `temp_file', replace
        }
    }

            rename A  prop_name
            rename B  prop_id
            label var prop_name                          "Name of the mine or facility"
            label var prop_id                            "Unique key for the project"

            local year = 1999
            unab vars : C-CT
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "commodity_prod_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Quantity of commodity produced in `year' (`commodity')"

                local i = `i' + 1
                if (`i' > 12) {
                    local i = 1
                    local year = `year' - 1
                }
            }

            * 1. Reshape to long
            reshape long commodity_prod_t_, i(prop_name prop_id) j(commodity_year) string

            * 2. Split
            split commodity_year, parse("_")
            rename commodity_year1 year
            rename commodity_year2 commodity

            replace commodity = "ferromanganese" if commodity == "ferromanga"
            replace commodity = "ferrochrome" if commodity == "ferrochrom"
            replace commodity = "iron_ore" if commodity == "iron"
            //rename commodity_prod_t_ commodity_prod_t
            label var commodity_prod_t_ "Quantity of commodity produced"
            drop commodity_year commodity_year3


        /*
        clear
        use "$temp_production/production_4_16.dta"
        //replace commodity = "iron_ore" if commodity == "iron ore"
        //rename all_in_cost_t all_in_cost_t_
        reshape wide commodity_prod_t_, i(prop_name prop_id year) j(commodity) string
        label var commodity_prod_t_alumina "Total per unit cost associated with production of commodity (alumina)"
        label var commodity_prod_t_aluminum "Total per unit cost associated with production of commodity (aluminum)"
        label var commodity_prod_t_bauxite "Total per unit cost associated with production of commodity (bauxite)"
        label var commodity_prod_t_chromite "Total per unit cost associated with production of commodity (chromite)"
        label var commodity_prod_t_chromium "Total per unit cost associated with production of commodity (chromium)"
        label var commodity_prod_t_coal "Total per unit cost associated with production of commodity (coal)"
        label var commodity_prod_t_ferrochrome "Total per unit cost associated with production of commodity (ferrochrome)"
        label var commodity_prod_t_ferromanganese "Total per unit cost associated with production of commodity (ferromanganese)"
        label var commodity_prod_t_iron_ore "Total per unit cost associated with production of commodity (iron ore)"
        label var commodity_prod_t_manganese "Total per unit cost associated with production of commodity (manganese)"
        label var commodity_prod_t_phosphate "Total per unit cost associated with production of commodity (phosphate)"
        label var commodity_prod_t_potash "Total per unit cost associated with production of commodity (potash)"
        */
        
        save "$temp_production/production_4_16.dta", replace
        
end

program combine_production_4_17
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local commodities "alumina aluminum bauxite chromite chromium coal ferrochrome ferromanganese iron_ore manganese phosphate potash"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_commodity_production_bulk_commodities_2000_2007_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear

            append using `temp_file'
            save `temp_file', replace
        }
    }

            rename A  prop_name
            rename B  prop_id
            label var prop_name                          "Name of the mine or facility"
            label var prop_id                            "Unique key for the project"

            local year = 2007
            unab vars : C-CT
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "commodity_prod_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Quantity of commodity produced in `year' (`commodity')"

                local i = `i' + 1
                if (`i' > 12) {
                    local i = 1
                    local year = `year' - 1
                }
            }

            * 1. Reshape to long
            reshape long commodity_prod_t_, i(prop_name prop_id) j(commodity_year) string

            * 2. Split
            split commodity_year, parse("_")
            rename commodity_year1 year
            rename commodity_year2 commodity

            replace commodity = "ferromanganese" if commodity == "ferromanga"
            replace commodity = "ferrochrome" if commodity == "ferrochrom"
            replace commodity = "iron_ore" if commodity == "iron"
            //rename commodity_prod_t_ commodity_prod_t
            label var commodity_prod_t_ "Quantity of commodity produced"
            drop commodity_year commodity_year3

            /*
            clear
            use "$temp_production/production_4_17.dta"
            //replace commodity = "iron_ore" if commodity == "iron ore"
            //rename all_in_cost_t all_in_cost_t_
            reshape wide commodity_prod_t_, i(prop_name prop_id year) j(commodity) string
            label var commodity_prod_t_alumina "Total per unit cost associated with production of commodity (alumina)"
            label var commodity_prod_t_aluminum "Total per unit cost associated with production of commodity (aluminum)"
            label var commodity_prod_t_bauxite "Total per unit cost associated with production of commodity (bauxite)"
            label var commodity_prod_t_chromite "Total per unit cost associated with production of commodity (chromite)"
            label var commodity_prod_t_chromium "Total per unit cost associated with production of commodity (chromium)"
            label var commodity_prod_t_coal "Total per unit cost associated with production of commodity (coal)"
            label var commodity_prod_t_ferrochrome "Total per unit cost associated with production of commodity (ferrochrome)"
            label var commodity_prod_t_ferromanganese "Total per unit cost associated with production of commodity (ferromanganese)"
            label var commodity_prod_t_iron_ore "Total per unit cost associated with production of commodity (iron ore)"
            label var commodity_prod_t_manganese "Total per unit cost associated with production of commodity (manganese)"
            label var commodity_prod_t_phosphate "Total per unit cost associated with production of commodity (phosphate)"
            label var commodity_prod_t_potash "Total per unit cost associated with production of commodity (potash)"
            */

            save "$temp_production/production_4_17.dta", replace
        
end

program combine_production_4_18
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local commodities "alumina aluminum bauxite chromite chromium coal ferrochrome ferromanganese iron_ore manganese phosphate potash"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_commodity_production_bulk_commodities_2008_2015_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear

            append using `temp_file'
            save `temp_file', replace
        }
    }

            rename A  prop_name
            rename B  prop_id
            label var prop_name                          "Name of the mine or facility"
            label var prop_id                            "Unique key for the project"

            local year = 2015
            unab vars : C-CT
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "commodity_prod_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Quantity of commodity produced in `year' (`commodity')"

                local i = `i' + 1
                if (`i' > 12) {
                    local i = 1
                    local year = `year' - 1
                }
            }

            * 1. Reshape to long
            reshape long commodity_prod_t_, i(prop_name prop_id) j(commodity_year) string

            * 2. Split
            split commodity_year, parse("_")
            rename commodity_year1 year
            rename commodity_year2 commodity

            replace commodity = "ferromanganese" if commodity == "ferromanga"
            replace commodity = "ferrochrome" if commodity == "ferrochrom"
            replace commodity = "iron_ore" if commodity == "iron"
            //rename commodity_prod_t_ commodity_prod_t
            label var commodity_prod_t_ "Quantity of commodity produced"
            drop commodity_year commodity_year3

            /*
            clear
            use "$temp_production/production_4_18.dta"
            //replace commodity = "iron_ore" if commodity == "iron ore"
            //rename all_in_cost_t all_in_cost_t_
            reshape wide commodity_prod_t_, i(prop_name prop_id year) j(commodity) string
            label var commodity_prod_t_alumina "Total per unit cost associated with production of commodity (alumina)"
            label var commodity_prod_t_aluminum "Total per unit cost associated with production of commodity (aluminum)"
            label var commodity_prod_t_bauxite "Total per unit cost associated with production of commodity (bauxite)"
            label var commodity_prod_t_chromite "Total per unit cost associated with production of commodity (chromite)"
            label var commodity_prod_t_chromium "Total per unit cost associated with production of commodity (chromium)"
            label var commodity_prod_t_coal "Total per unit cost associated with production of commodity (coal)"
            label var commodity_prod_t_ferrochrome "Total per unit cost associated with production of commodity (ferrochrome)"
            label var commodity_prod_t_ferromanganese "Total per unit cost associated with production of commodity (ferromanganese)"
            label var commodity_prod_t_iron_ore "Total per unit cost associated with production of commodity (iron ore)"
            label var commodity_prod_t_manganese "Total per unit cost associated with production of commodity (manganese)"
            label var commodity_prod_t_phosphate "Total per unit cost associated with production of commodity (phosphate)"
            label var commodity_prod_t_potash "Total per unit cost associated with production of commodity (potash)"
            */

            save "$temp_production/production_4_18.dta", replace
        
end

program combine_production_4_19
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local commodities "alumina aluminum bauxite chromite chromium coal ferrochrome ferromanganese iron_ore manganese phosphate potash"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_commodity_production_bulk_commodities_2016_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear

            append using `temp_file'
            save `temp_file', replace
        }
    }

            rename A  prop_name
            rename B  prop_id
            label var prop_name                          "Name of the mine or facility"
            label var prop_id                            "Unique key for the project"

            local year = 2023
            unab vars : C-CT
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "commodity_prod_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Quantity of commodity produced in `year' (`commodity')"

                local i = `i' + 1
                if (`i' > 12) {
                    local i = 1
                    local year = `year' - 1
                }
            }

            * 1. Reshape to long
            reshape long commodity_prod_t_, i(prop_name prop_id) j(commodity_year) string

            * 2. Split
            split commodity_year, parse("_")
            rename commodity_year1 year
            rename commodity_year2 commodity

            replace commodity = "ferromanganese" if commodity == "ferromanga"
            replace commodity = "ferrochrome" if commodity == "ferrochrom"
            replace commodity = "iron_ore" if commodity == "iron"
            //rename commodity_prod_t_ commodity_prod_t
            label var commodity_prod_t_ "Quantity of commodity produced"
            drop commodity_year commodity_year3

            /*
            clear
            use "$temp_production/production_4_19.dta"
            //replace commodity = "iron_ore" if commodity == "iron ore"
            //rename all_in_cost_t all_in_cost_t_
            reshape wide commodity_prod_t_, i(prop_name prop_id year) j(commodity) string
            label var commodity_prod_t_alumina "Total per unit cost associated with production of commodity (alumina)"
            label var commodity_prod_t_aluminum "Total per unit cost associated with production of commodity (aluminum)"
            label var commodity_prod_t_bauxite "Total per unit cost associated with production of commodity (bauxite)"
            label var commodity_prod_t_chromite "Total per unit cost associated with production of commodity (chromite)"
            label var commodity_prod_t_chromium "Total per unit cost associated with production of commodity (chromium)"
            label var commodity_prod_t_coal "Total per unit cost associated with production of commodity (coal)"
            label var commodity_prod_t_ferrochrome "Total per unit cost associated with production of commodity (ferrochrome)"
            label var commodity_prod_t_ferromanganese "Total per unit cost associated with production of commodity (ferromanganese)"
            label var commodity_prod_t_iron_ore "Total per unit cost associated with production of commodity (iron ore)"
            label var commodity_prod_t_manganese "Total per unit cost associated with production of commodity (manganese)"
            label var commodity_prod_t_phosphate "Total per unit cost associated with production of commodity (phosphate)"
            label var commodity_prod_t_potash "Total per unit cost associated with production of commodity (potash)"
            */

            save "$temp_production/production_4_19.dta", replace
        
end

program combine_production_4_20
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local commodities "gold palladium platinum silver"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_commodity_production_precious_metals_1991_2000_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear

            append using `temp_file'
            save `temp_file', replace
        }
    }

            rename A  prop_name
            rename B  prop_id
            label var prop_name                          "Name of the mine or facility"
            label var prop_id                            "Unique key for the project"

            local year = 2000
            unab vars : C-AP
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "commodity_prod_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Quantity of commodity produced in `year' (`commodity')"

                local i = `i' + 1
                if (`i' > 4) {
                    local i = 1
                    local year = `year' - 1
                }
            }

            * 1. Reshape to long
            reshape long commodity_prod_t_, i(prop_name prop_id) j(commodity_year) string

            * 2. Split
            split commodity_year, parse("_")
            rename commodity_year1 year
            rename commodity_year2 precious_metal
            //replace commodity = "ferromanganese" if commodity == "ferromanga"
            
            label var commodity_prod_t_ "Quantity of commodity produced"
            drop commodity_year

            save "$temp_production/production_4_20.dta", replace
        
end

program combine_production_4_21
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local commodities "gold palladium platinum silver"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_commodity_production_precious_metals_2001_2010_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear

            append using `temp_file'
            save `temp_file', replace
        }
    }

            rename A  prop_name
            rename B  prop_id
            label var prop_name                          "Name of the mine or facility"
            label var prop_id                            "Unique key for the project"

            local year = 2010
            unab vars : C-AP
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "commodity_prod_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Quantity of commodity produced in `year' (`commodity')"

                local i = `i' + 1
                if (`i' > 4) {
                    local i = 1
                    local year = `year' - 1
                }
            }

            * 1. Reshape to long
            reshape long commodity_prod_t_, i(prop_name prop_id) j(commodity_year) string

            * 2. Split
            split commodity_year, parse("_")
            rename commodity_year1 year
            rename commodity_year2 precious_metal
            //replace commodity = "ferromanganese" if commodity == "ferromanga"
            
            label var commodity_prod_t_ "Quantity of commodity produced"
            drop commodity_year

            save "$temp_production/production_4_21.dta", replace
        
end

program combine_production_4_22
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local commodities "gold palladium platinum silver"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_commodity_production_precious_metals_2011_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear

            append using `temp_file'
            save `temp_file', replace
        }
    }

            rename A  prop_name
            rename B  prop_id
            label var prop_name                          "Name of the mine or facility"
            label var prop_id                            "Unique key for the project"

            local year = 2023
            unab vars : C-BB
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "commodity_prod_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Quantity of commodity produced in `year' (`commodity')"

                local i = `i' + 1
                if (`i' > 4) {
                    local i = 1
                    local year = `year' - 1
                }
            }

            * 1. Reshape to long
            reshape long commodity_prod_t_, i(prop_name prop_id) j(commodity_year) string

            * 2. Split
            split commodity_year, parse("_")
            rename commodity_year1 year
            rename commodity_year2 precious_metal
            //replace commodity = "ferromanganese" if commodity == "ferromanga"
            
            label var commodity_prod_t_ "Quantity of commodity produced"
            drop commodity_year

            save "$temp_production/production_4_22.dta", replace
        
end

program combine_production_4_23
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local commodities "antimony ferrotungsten ferrovanadium graphite heavy_mineral_sands ilmenite lanthanides lithium niobium rutile scandium tantalum titanium tungsten vanadium yttrium zircon"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_commodity_production_specialty_commodities_1991_2001_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear

            append using `temp_file'
            save `temp_file', replace
        }
    }

            rename A  prop_name
            rename B  prop_id
            label var prop_name                          "Name of the mine or facility"
            label var prop_id                            "Unique key for the project"

            local year = 2001
            unab vars : C-GG
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "commodity_prod_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Quantity of commodity produced in `year' (`commodity')"

                local i = `i' + 1
                if (`i' > 17) {
                    local i = 1
                    local year = `year' - 1
                }
            }

            * 1. Reshape to long
            reshape long commodity_prod_t_, i(prop_name prop_id) j(commodity_year) string

            * 2. Split
            split commodity_year, parse("_")
            rename commodity_year1 year
            rename commodity_year2 specialty_commodity
            replace specialty_commodity = "ferrotungsten" if specialty_commodity == "ferrotungs"
            replace specialty_commodity = "ferrovanadium" if specialty_commodity == "ferrovanad"
            replace specialty_commodity = "heavy_mineral" if specialty_commodity == "heavy"
            
            label var commodity_prod_t_ "Quantity of commodity produced"
            drop commodity_year commodity_year3

            save "$temp_production/production_4_23.dta", replace
        
end

program combine_production_4_24
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local commodities "antimony ferrotungsten ferrovanadium graphite heavy_mineral_sands ilmenite lanthanides lithium niobium rutile scandium tantalum titanium tungsten vanadium yttrium zircon"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_commodity_production_specialty_commodities_2002_2012_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear

            append using `temp_file'
            save `temp_file', replace
        }
    }

            rename A  prop_name
            rename B  prop_id
            label var prop_name                          "Name of the mine or facility"
            label var prop_id                            "Unique key for the project"

            local year = 2012
            unab vars : C-GG
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "commodity_prod_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Quantity of commodity produced in `year' (`commodity')"

                local i = `i' + 1
                if (`i' > 17) {
                    local i = 1
                    local year = `year' - 1
                }
            }

            * 1. Reshape to long
            reshape long commodity_prod_t_, i(prop_name prop_id) j(commodity_year) string

            * 2. Split
            split commodity_year, parse("_")
            rename commodity_year1 year
            rename commodity_year2 specialty_commodity
            replace specialty_commodity = "ferrotungsten" if specialty_commodity == "ferrotungs"
            replace specialty_commodity = "ferrovanadium" if specialty_commodity == "ferrovanad"
            replace specialty_commodity = "heavy_mineral" if specialty_commodity == "heavy"

            label var commodity_prod_t_ "Quantity of commodity produced"
            drop commodity_year commodity_year3

            save "$temp_production/production_4_24.dta", replace
        
end

program combine_production_4_25
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local commodities "antimony ferrotungsten ferrovanadium graphite heavy_mineral_sands ilmenite lanthanides lithium niobium rutile scandium tantalum titanium tungsten vanadium yttrium zircon"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_commodity_production_specialty_commodities_2013_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear

            append using `temp_file'
            save `temp_file', replace
        }
    }

            rename A  prop_name
            rename B  prop_id
            label var prop_name                          "Name of the mine or facility"
            label var prop_id                            "Unique key for the project"

            local year = 2023
            unab vars : C-GG
            local i = 1
            foreach oldname of local vars {
                local commodity : word `i' of `commodities'
                local newname = "commodity_prod_t_`year'_`commodity'"
                local shortname = substr("`newname'", 1, 32)

                rename `oldname' `shortname'
                label var `shortname' "Quantity of commodity produced in `year' (`commodity')"

                local i = `i' + 1
                if (`i' > 17) {
                    local i = 1
                    local year = `year' - 1
                }
            }

            * 1. Reshape to long
            reshape long commodity_prod_t_, i(prop_name prop_id) j(commodity_year) string

            * 2. Split
            split commodity_year, parse("_")
            rename commodity_year1 year
            rename commodity_year2 specialty_commodity
            replace specialty_commodity = "ferrotungsten" if specialty_commodity == "ferrotungs"
            replace specialty_commodity = "ferrovanadium" if specialty_commodity == "ferrovanad"
            replace specialty_commodity = "heavy_mineral" if specialty_commodity == "heavy"

            label var commodity_prod_t_ "Quantity of commodity produced"
            drop commodity_year commodity_year3

            save "$temp_production/production_4_25.dta", replace
        
end

program combine_production_5_1
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_5_production_rank_value_all_1_2010_2022_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            //tostring C-AI, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * Rename and label variables
    rename A  prop_name
    rename B  prop_id
    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    * as_of_date
    local year = 2022
    foreach var of varlist C-O {
        local newname = "as_of_date_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    local year = 2022
    foreach var of varlist as_of_date_2022-as_of_date_2010 {
        label var `var' "Date on which the fiscal period ended (`year')"
        local year = `year' - 1
    }

    * global_rank_all_commodities
    local year = 2022
    foreach var of varlist P-AB {
        local newname = "global_rank_all_commodities_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    local year = 2022
    foreach var of varlist global_rank_all_commodities_2022-global_rank_all_commodities_2010 {
        label var `var' "Rank when compared to global production totals (`year')"
        local year = `year' - 1
    }

    // wide to long
    reshape long as_of_date_ global_rank_all_commodities_, i(prop_name prop_id) j(year)
    rename as_of_date_ as_of_date
    rename global_rank_all_commodities_ global_rank_all_commodities
    label var as_of_date "Date on which the fiscal period ended"
    label var global_rank_all_commodities "Rank when compared to global production totals"

    save "$temp_production/production_5_1.dta", replace

end

program combine_production_5_2
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_5_production_rank_value_all_2_2010_2022_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            //tostring C-AI, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * Rename and label variables
    rename A  prop_name
    rename B  prop_id
    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    * global_production_value_all_comm
    local year = 2022
    foreach var of varlist C-O {
        local newname = "global_prod_val_all_comm_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    local year = 2022
    foreach var of varlist global_prod_val_all_comm_2022-global_prod_val_all_comm_2010 {
        label var `var' "Value of production (`year')"
        local year = `year' - 1
    }

    * share_of_world_all_comm
    local year = 2022
    foreach var of varlist P-AB {
        local newname = "share_world_all_comm_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    local year = 2022
    foreach var of varlist share_world_all_comm_2022-share_world_all_comm_2010 {
        label var `var' "Production value as a percent of total estimated or third-party reported global production value (`year')"
        local year = `year' - 1
    }

    // wide to long
    reshape long global_prod_val_all_comm_ share_world_all_comm_, i(prop_name prop_id) j(year)
    rename global_prod_val_all_comm_ global_prod_val_all_commodities
    rename share_world_all_comm_ share_of_world_all_commodities
    label var global_prod_val_all_commodities "Value of production"
    label var share_of_world_all_commodities "Production value as a percent of total estimated or third-party reported global production value"

    save "$temp_production/production_5_2.dta", replace

end

