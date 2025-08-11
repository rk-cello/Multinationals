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
global input_mine_econ_modeled "$input_metals_mining/properties_mine_econ_modeled_data"

* outputs
global output_property_level "$dir_cleaned/property_level"
global output_company_level "$dir_cleaned/company_level"

* intermediates
global temp_property_level "$dir_temp/property_level"
global temp_company_level "$dir_temp/company_level"
global temp_mine_econ_modeled "$dir_temp/temp_mine_econ_modeled"


************************************************************************

* roadmap
program main
    combine_econ_modeled_1
    combine_econ_modeled_2
    combine_econ_modeled_3
    combine_econ_modeled_4
    combine_econ_modeled_5
    combine_econ_modeled_6
    combine_econ_modeled_7
    combine_econ_modeled_8
    combine_econ_modeled_9
    combine_econ_modeled_10
    combine_econ_modeled_11
    combine_econ_modeled_12
    combine_econ_modeled_13
    combine_econ_modeled_14
    combine_econ_modeled_15
    combine_econ_modeled_16
    combine_econ_modeled_17
    combine_econ_modeled_18
end


program combine_econ_modeled_1
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_mine_econ_modeled/modeled_data_cash_flow_1_gross_revenue_`region'.xls"
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
        local newname = "non_ferrous_gross_rev_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }


    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    * Add labels for renamed variables
    local year = 2023
    foreach var of varlist non_ferrous_gross_rev_2023-non_ferrous_gross_rev_1991 {
        label var `var' "Gross revenue from commodities produced (`year')"
        local year = `year' - 1
    }

    reshape long non_ferrous_gross_rev_, i(prop_name prop_id) j(year)
    rename non_ferrous_gross_rev_ non_ferrous_gross_rev
    label var non_ferrous_gross_rev "Gross revenue from commodities produced"

    save "$temp_mine_econ_modeled/econ_modeled_1.dta", replace

end

program combine_econ_modeled_2
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_mine_econ_modeled/modeled_data_cash_flow_2_realization_costs_`region'.xls"
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
        local newname = "non_ferrous_realization_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }


    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    reshape long non_ferrous_realization_, i(prop_name prop_id) j(year)
    rename non_ferrous_realization_ non_ferrous_realization_costs
    label var non_ferrous_realization_costs "Realization costs in concentrate or intermediary product transport, port charges, ocean freight, smelting and refining costs"

    save "$temp_mine_econ_modeled/econ_modeled_2.dta", replace

end

program combine_econ_modeled_3
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_mine_econ_modeled/modeled_data_cash_flow_3_net_revenue_`region'.xls"
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
        local newname = "non_ferrous_net_rev_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }


    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    reshape long non_ferrous_net_rev_, i(prop_name prop_id) j(year)
    rename non_ferrous_net_rev_ non_ferrous_net_rev
    label var non_ferrous_net_rev "Gross revenue less realization costs"

    save "$temp_mine_econ_modeled/econ_modeled_3.dta", replace

end

program combine_econ_modeled_4
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_mine_econ_modeled/modeled_data_cash_flow_4_revenue_FOB_port_sales_`region'.xls"
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
        local newname = "rev_fob_port_sales_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }


    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    reshape long rev_fob_port_sales_, i(prop_name prop_id) j(year)
    rename rev_fob_port_sales_ rev_fob_port_sales
    label var rev_fob_port_sales "Gross revenue from commodities produced"

    save "$temp_mine_econ_modeled/econ_modeled_4.dta", replace

end

program combine_econ_modeled_5
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_mine_econ_modeled/modeled_data_cash_flow_5_port_loading_inland_transport_cost_`region'.xls"
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
        local newname = "port_loading_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }


    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    reshape long port_loading_, i(prop_name prop_id) j(year)
    rename port_loading_ port_load_inland_trans
    label var port_load_inland_trans "Realization costs in concentrate or intermediary product transport, port charges, ocean freight, smelting and refining costs"

    save "$temp_mine_econ_modeled/econ_modeled_5.dta", replace

end

program combine_econ_modeled_6
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_mine_econ_modeled/modeled_data_cash_flow_6_royalties_`region'.xls"
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
        local newname = "royalties_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }


    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    reshape long royalties_, i(prop_name prop_id) j(year)
    rename royalties_ royalties
    label var royalties "Royalties and front-end taxes (including sales tax, export tax and duties plus any other revenue-based taxes)"

    save "$temp_mine_econ_modeled/econ_modeled_6.dta", replace

end

program combine_econ_modeled_7
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_mine_econ_modeled/modeled_data_cash_flow_7_total_minesite_costs_`region'.xls"
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
        local newname = "minesite_and_pelletizing_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }


    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    reshape long minesite_and_pelletizing_, i(prop_name prop_id) j(year)
    rename minesite_and_pelletizing_ minesite_and_pelletizing
    label var minesite_and_pelletizing "Onsite mine, mill and solvent extraction and electrowinning (SX/EW) costs"

    save "$temp_mine_econ_modeled/econ_modeled_7.dta", replace

end

program combine_econ_modeled_8
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_mine_econ_modeled/modeled_data_cash_flow_8_total_pelletizing_cost_`region'.xls"
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
        local newname = "pelletizing_cost_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }


    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    reshape long pelletizing_cost_, i(prop_name prop_id) j(year)
    rename pelletizing_cost_ pelletizing_cost
    label var pelletizing_cost "Total mill costs to produce iron ore pellets including reagents and supplies"

    save "$temp_mine_econ_modeled/econ_modeled_8.dta", replace

end

program combine_econ_modeled_9
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_mine_econ_modeled/modeled_data_cash_flow_9_indirect_extraordinary_costs_`region'.xls"
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
        local newname = "indirect_extra_costs_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    reshape long indirect_extra_costs_, i(prop_name prop_id) j(year)
    rename indirect_extra_costs_ indirect_extra_costs
    label var indirect_extra_costs "Net indirect and extraordinary cash costs"

    save "$temp_mine_econ_modeled/econ_modeled_9.dta", replace

end

program combine_econ_modeled_10
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_mine_econ_modeled/modeled_data_cash_flow_10_working_capital_`region'.xls"
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
        local newname = "working_capital_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    reshape long working_capital_, i(prop_name prop_id) j(year)
    rename working_capital_ working_capital
    label var working_capital "Additional capital expenditure needed that is not sustaining, development or expansion related"

    save "$temp_mine_econ_modeled/econ_modeled_10.dta", replace

end

program combine_econ_modeled_11
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_mine_econ_modeled/modeled_data_cash_flow_11_sustaining_capex_`region'.xls"
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
        local newname = "sustaining_capex_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    reshape long sustaining_capex_, i(prop_name prop_id) j(year)
    rename sustaining_capex_ sustaining_capex
    label var sustaining_capex "Capital expenditures that are necessary to maintain or sustain current operations"

    save "$temp_mine_econ_modeled/econ_modeled_11.dta", replace

end

program combine_econ_modeled_12
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_mine_econ_modeled/modeled_data_cash_flow_12_development_expansion_capex_`region'.xls"
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
        local newname = "dev_and_expan_capex_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    reshape long dev_and_expan_capex_, i(prop_name prop_id) j(year)
    rename dev_and_expan_capex_ dev_and_expansion_capex
    label var dev_and_expansion_capex "Capital expenditures used to expand existing production or develop new production"

    save "$temp_mine_econ_modeled/econ_modeled_12.dta", replace

end
program combine_econ_modeled_13
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_mine_econ_modeled/modeled_data_cash_flow_13_pre-tax_cash_flow_`region'.xls"
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
        local newname = "pretax_cf_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    reshape long pretax_cf_, i(prop_name prop_id) j(year)
    rename pretax_cf_ pretax_cf
    label var pretax_cf "Cash flow net of all items except taxes"

    save "$temp_mine_econ_modeled/econ_modeled_13.dta", replace

end

program combine_econ_modeled_14
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_mine_econ_modeled/modeled_data_cash_flow_14_tax_`region'.xls"
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
        local newname = "tax_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    reshape long tax_, i(prop_name prop_id) j(year)
    rename tax_ tax
    label var tax "Estimated corporation tax charges based on estimated EBIT. This excludes carry over gains."

    save "$temp_mine_econ_modeled/econ_modeled_14.dta", replace

end

program combine_econ_modeled_15
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_mine_econ_modeled/modeled_data_cash_flow_15_post-tax_cash_flow_`region'.xls"
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
        local newname = "post_tax_cf_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    reshape long post_tax_cf_, i(prop_name prop_id) j(year)
    rename post_tax_cf_ post_tax_cf
    label var post_tax_cf "Cash flow available after estimated tax charges"

    save "$temp_mine_econ_modeled/econ_modeled_15.dta", replace

end

program combine_econ_modeled_16
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_mine_econ_modeled/modeled_data_cash_flow_16_corporate_tax_rate_`region'.xls"
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
        local newname = "corp_tax_rate_pct_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    reshape long corp_tax_rate_pct_, i(prop_name prop_id) j(year)
    rename corp_tax_rate_pct_ corp_tax_rate_pct
    label var corp_tax_rate_pct "Income tax provisions incurred as a percent of net income before taxes"

    save "$temp_mine_econ_modeled/econ_modeled_16.dta", replace

end

program combine_econ_modeled_17
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_mine_econ_modeled/modeled_data_cash_flow_17_depreciation_`region'.xls"
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
        local newname = "depr_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    reshape long depr_, i(prop_name prop_id) j(year)
    rename depr_ depr
    label var depr "Depreciation, depletion and amortization of property, equipment and other fixed assets as part of mining and processing operations"

    save "$temp_mine_econ_modeled/econ_modeled_17.dta", replace

end

program combine_econ_modeled_18
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_mine_econ_modeled/modeled_data_cash_flow_18_NPV_8percent_`region'.xls"
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
        local newname = "npv_8pct_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    reshape long npv_8pct_, i(prop_name prop_id) j(year)
    rename npv_8pct_ npv_discounted_at_8pct
    label var npv_discounted_at_8pct "Post-tax net present value of the operation using a 8% discount rate"

    save "$temp_mine_econ_modeled/econ_modeled_18.dta", replace

end