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

    save "$temp_production/production_4_8.dta", replace
end

program combine_production_4_9
    local commodities "alumina aluminum bauxite chromite chromium coal ferrochrome ferromanganese iron_ore manganese phosphate potash"
    local timespan "1991_2001 2002_2012 2013_2023"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "production_4_commodity_production_costs_all_in_costs_specialty_commodities_`timespan'_AfricaEmergingAsiaPacificLatinAmerica.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }





