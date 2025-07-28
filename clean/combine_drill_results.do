**** construct property level cross-section data ****
**** notes ****
* some string variables should be converted to numeric
* 
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

* intermediates
global temp_drill_results "$dir_temp/temp_drill_results"


************************************************************************
cd "$input_metals_mining/drill_results_capital_costs"

program main
    combine_drill_results
    capital_costs_global
end

program combine_drill_results
    clear all
    set more off
    
    local countries "angola bolivia botswana brazil_other_stages brazil_production_stage burkinafaso cameroon chad china colombia cotedivoire djibouti drc ecuador eritrea ethiopia gabon ghana guinea honduras india indonesia kenya liberia madagascar malawi mali mauritania mongolia morocco mozambique namibia niger nigeria peru philippines repcongo rwanda senegal sierraleone southafrica sudan tanzania togo tunisia uganda zambia zimbabwe"
    tempfile temp_all
    save `temp_all', emptyok
    tempfile imported_file1_1
    tempfile imported_file1_2

            foreach country of local countries {
                local file1_1 "general_information/drill_results_1_general_info_1_`country'.xls"
                local file1_2 "general_information/drill_results_1_general_info_2_`country'.xls"
                local file2 "interval_details_value/drill_results_2_interval_details_value_`country'.xls"

                // Check if file exists; if not, continue to next iteration
                capture confirm file "`file1_1'"
                if _rc != 0 {
                    display "File `file1_1' does not exist, skipping."
                    continue
                }
                capture confirm file "`file1_2'"
                if _rc != 0 {
                    display "File `file1_2' does not exist, skipping."
                    continue
                }
                capture confirm file "`file2'"
                if _rc != 0 {
                    display "File `file2' does not exist, skipping."
                    continue
                }
                display "Processing: `country'"

                // Read file1 main data (from row 7 down)
                import excel "`file1_1'", cellrange(A7) clear
                tostring A D-F G I, replace
                // rename variables
                rename A prop_name
                rename B interval_id
                rename C prop_id
                rename D minesearch_id
                rename E meg_prop_id
                rename F primary_interval_commodity
                rename G interval_commodities
                rename H rptd_date
                rename I reporting_company
                rename J reporting_company_id
                // Label vars
                label variable prop_name "Name of the mine or facility"
                label variable interval_id "Exploration result interval OID"
                label variable prop_id "Unique key for the project"
                label variable minesearch_id "Unique project ID (Metals Economics Group)"
                label variable meg_prop_id "MEG's primary key for mining projects"
                label variable primary_interval_commodity "Main commodity extracted in interval"
                label variable interval_commodities "All commodities extracted in interval"
                label variable rptd_date "Date when exploration results were reported"
                label variable reporting_company "Full name of reporting company"
                label variable reporting_company_id "S&P unique ID for reporting company"

                save `imported_file1_1', replace

                // Read main data (from row 7 down)
                import excel "`file1_2'", cellrange(A7) clear
                tostring A C-H, replace
                // rename variables
                rename A prop_name
                rename B interval_id
                rename C stage_at_ann
                rename D hole_id
                rename E significant_interval
                rename F type_drilling
                rename G surface_underground
                rename H explor_purpose
                rename I dip
                rename J azimuth
                // Label vars
                label variable prop_name "Name of the mine or facility"
                label variable interval_id "Exploration result interval OID"
                label variable stage_at_ann "Development stages of a mining reserve"
                label variable hole_id "Identifier of drill hole reported by filer"
                label variable significant_interval "Indicates that an interval is of significant value and purpose"
                label variable type_drilling "Method of exploration carried out at a mine"
                label variable surface_underground "Location from which exploration was initiated at a mine"
                label variable explor_purpose "Purpose of an exploration attempt"
                label variable dip "Angle of the drill hole, relative to a flat horizontal surface"
                label variable azimuth "Compass bearing of the drill hole"

                merge 1:1 prop_name interval_id using `imported_file1_1'
                drop _merge
                save `imported_file1_2', replace

                // Read file2 main data (from row 7 down)
                import excel "`file2'", cellrange(A7) clear
                tostring A G, replace
                destring C-F H-I, replace force
                // rename variables
                rename A prop_name
                rename B interval_id
                rename C from
                rename D to
                rename E intercept_depth
                rename F interval
                rename G true_width
                rename H interval_value_m_usd_per_t
                rename I grade_value_usd_per_t
                // Label vars
                label variable prop_name "Name of the mine or facility"
                label variable interval_id "Exploration result interval OID"
                label variable from "Start point of an exploration result interval"
                label variable to "End point of an exploration result interval"
                label variable intercept_depth "Depth at which an interval begins"
                label variable interval "Interval length of an exploration result intersection"
                label variable true_width "Indicates that the interval length corresponds to the true width of the deposit"
                label variable interval_value_m_usd_per_t "USD value of interval on a per metric tonne basis, multiplied by the interval's width"
                label variable grade_value_usd_per_t "USD value of interval, calculated assuming reported grades are on a per metric tonne basis"

                merge 1:1 prop_name interval_id using `imported_file1_2'
                drop _merge
                
                // Append to the accumulating dataset
                append using `temp_all'
                save `temp_all', replace
            }

    save "$temp_drill_results/drill_results_1&2.dta", replace

end

program capital_costs_global
    clear all
    set more off
    import excel "capital_costs_Global.xls", cellrange(A7) clear

    // --- Rename variables ---
    rename A prop_name
    rename B capital_cost_id
    rename C prop_id
    rename D minesearch_id
    rename E capital_cost_name
    rename F capital_cost_type
    rename G greenfield_brownfield
    rename H open_pit_underground_dev
    rename I capital_cost_comments
    rename J date_announced
    rename K proj_comp_date_announce
    rename L proj_comp_yr_announce
    rename M proj_comp_qtr_announce
    rename N most_recent_proj_comp_date
    rename O actual_date_completed
    rename P date_canceled
    rename Q cost_announce
    rename R rptd_currency_announce
    rename S cost_most_recent_comp
    rename T rptd_currency_most_recent_comp
    rename U as_of_date_most_recent_cost
    rename V current_capital_cost_status
    rename W status_as_of

    // --- Label variables ---
    label variable prop_name "Name of the mine or facility"
    label variable capital_cost_id "Unique identifier of the capital cost record"
    label variable prop_id "Unique key for the project"
    label variable minesearch_id "Unique project ID (Metals Economics Group)"
    label variable capital_cost_name "Name of the capital cost"
    label variable capital_cost_type "Category of capital spending planned/completed"
    label variable greenfield_brownfield "Type of mining discovery"
    label variable open_pit_underground_dev "Mining operation carried out at a mine"
    label variable capital_cost_comments "Comments about the capital cost"
    label variable date_announced "Date on which the capital improvement project was first publicly announced"
    label variable proj_comp_date_announce "Date on which the capital improvements are projected for completion"
    label variable proj_comp_yr_announce "Year in which the capital improvements are projected for completion"
    label variable proj_comp_qtr_announce "Quarter in which the capital improvements are projected for completion"
    label variable most_recent_proj_comp_date "Date as of which the projected completion date is most recent"
    label variable actual_date_completed "Date on which the capital improvements were completed"
    label variable date_canceled "Date on which the capital improvements were abandoned"
    label variable cost_announce "The amount of capital being invested in this project (at announcement)"
    label variable rptd_currency_announce "Alphabetic code used to identify currencies pursuant to ISO-4217 (at announcement)"
    label variable cost_most_recent_comp "The amount of capital being invested in this project (most recent or completion)"
    label variable rptd_currency_most_recent_comp "Alphabetic code used to identify currencies pursuant to ISO-4217 (most recent or completion)"
    label variable as_of_date_most_recent_cost "Date as of which the improvement cost was valid"
    label variable current_capital_cost_status "The status of the capital improvement"
    label variable status_as_of "Date as of which the capital improvement entered this status"

    save "$temp_drill_results/capital_costs_global.dta", replace

end