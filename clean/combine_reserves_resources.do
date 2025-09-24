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
global input_reserves "$input_metals_mining/properties_reserves_resources"

* outputs
global output_property_level "$dir_cleaned/property_level"
global output_company_level "$dir_cleaned/company_level"

* intermediates
global temp_property_level "$dir_temp/property_level"
global temp_company_level "$dir_temp/company_level"
global temp_reserves "$dir_temp/temp_reserves_resources"


************************************************************************

* roadmap
program main
    combine_RR6

end

program combine_RR1_1
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_1_tonnage_volume_1_as_of_date_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            //tostring B, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local year = 2023
    foreach var of varlist C-AI {
        local newname = "r_and_r_as_of_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    reshape long r_and_r_as_of_, i(prop_name prop_id) j(year) string
    rename r_and_r_as_of_ r_and_r_as_of
    label var r_and_r_as_of "Date on which the mining reserve data was measured"
    replace year = "MstRctYear" if year == "2023"
    
    save "$temp_reserves/RR1_1.dta", replace

end

program combine_RR1_2
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_1_tonnage_volume_2_reserves_ore_tonnage_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local year = 2023
    foreach var of varlist C-AI {
        local newname = "resv_ore_t_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    reshape long resv_ore_t_, i(prop_name prop_id) j(year) string
    rename resv_ore_t_ resv_ore_tonnage
    label var resv_ore_tonnage "Mass of ore used to calculate total reserves"
    replace year = "MstRctYear" if year == "2023"
    
    save "$temp_reserves/RR1_2.dta", replace

end

program combine_RR1_3
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_1_tonnage_volume_3_measured_indicated_ore_tonnage_excl_reserves_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local year = 2023
    foreach var of varlist C-AI {
        local newname = "meas_ind_ore_t_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    reshape long meas_ind_ore_t_, i(prop_name prop_id) j(year) string
    rename meas_ind_ore_t_ meas_ind_ore_tonnage
    label var meas_ind_ore_tonnage "Mass of ore used to calculate measured and indicated resources"
    replace year = "MstRctYear" if year == "2023"
    
    save "$temp_reserves/RR1_3.dta", replace

end

program combine_RR1_4
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_1_tonnage_volume_4_inferred_resources_ore_tonnage_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local year = 2023
    foreach var of varlist C-Y {
        local newname = "inf_resrc_ore_t_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    reshape long inf_resrc_ore_t_, i(prop_name prop_id) j(year) string
    rename inf_resrc_ore_t_ inf_resrc_ore_tonnage
    label var inf_resrc_ore_tonnage "Mass of ore used to calculate resources containing inferred"
    replace year = "MstRctYear" if year == "2023"
    
    save "$temp_reserves/RR1_4.dta", replace

end

program combine_RR1_5
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_1_tonnage_volume_5_total_resources_ore_tonnage_excl_reserves_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local year = 2023
    foreach var of varlist C-AI {
        local newname = "total_resrc_ore_t_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    reshape long total_resrc_ore_t_, i(prop_name prop_id) j(year) string
    rename total_resrc_ore_t_ total_resrc_ore_tonnage
    label var total_resrc_ore_tonnage "Mass of ore used to calculate resources, including inferred"
    replace year = "MstRctYear" if year == "2023"
    
    save "$temp_reserves/RR1_5.dta", replace

end

program combine_RR1_6
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_1_tonnage_volume_6_reserves_resources_ore_tonnage_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local year = 2023
    foreach var of varlist C-AI {
        local newname = "r_and_r_ore_t_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    reshape long r_and_r_ore_t_, i(prop_name prop_id) j(year) string
    rename r_and_r_ore_t_ r_and_r_ore_tonnage
    label var r_and_r_ore_tonnage "Mass of ore used in the calculation of reserves and resources"
    replace year = "MstRctYear" if year == "2023"
    
    save "$temp_reserves/RR1_6.dta", replace

end

program combine_RR1_7
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_1_tonnage_volume_7_measured_indicated_ore_tonnage_incl_reserves_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local year = 2023
    foreach var of varlist C-AI {
        local newname = "meas_ind_ore_t_incl_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    reshape long meas_ind_ore_t_incl_, i(prop_name prop_id) j(year) string
    rename meas_ind_ore_t_incl_ meas_ind_ore_t_incl_resv
    label var meas_ind_ore_t_incl_resv "Mass of ore used to calculate measured and indicated resources inclusive of reserves"
    replace year = "MstRctYear" if year == "2023"
    
    save "$temp_reserves/RR1_7.dta", replace

end

program combine_RR1_8
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_1_tonnage_volume_8_reserves_ore_volume_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local year = 2023
    foreach var of varlist C-AI {
        local newname = "resv_ore_vol_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    reshape long resv_ore_vol_, i(prop_name prop_id) j(year) string
    rename resv_ore_vol_ resv_ore_volume
    label var resv_ore_volume "Volume of ore used to calculate total reserves"
    replace year = "MstRctYear" if year == "2023"
    
    save "$temp_reserves/RR1_8.dta", replace

end

program combine_RR1_9
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_1_tonnage_volume_9_measured_indicated_ore_volume_exlc_reserves_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local year = 2023
    foreach var of varlist C-Y {
        local newname = "meas_ind_ore_volume_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    reshape long meas_ind_ore_volume_, i(prop_name prop_id) j(year) string
    rename meas_ind_ore_volume_ meas_ind_ore_volume
    label var meas_ind_ore_volume "Volume of ore used to calculate measured and indicated resources excluding reserves"
    replace year = "MstRctYear" if year == "2023"
    
    save "$temp_reserves/RR1_9.dta", replace

end

program combine_RR1_10
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_1_tonnage_volume_10_inferred_ore_volume_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local year = 2023
    foreach var of varlist C-AI {
        local newname = "inf_ore_volume_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    reshape long inf_ore_volume_, i(prop_name prop_id) j(year) string
    rename inf_ore_volume_ inf_ore_volume
    label var inf_ore_volume "Volume of ore used to calculate inferred resources"
    replace year = "MstRctYear" if year == "2023"
    
    save "$temp_reserves/RR1_10.dta", replace

end

program combine_RR1_11
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_1_tonnage_volume_11_total_resources_ore_volume_excl_reserves_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local year = 2023
    foreach var of varlist C-Y {
        local newname = "total_resrc_ore_vol_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    reshape long total_resrc_ore_vol_, i(prop_name prop_id) j(year) string
    rename total_resrc_ore_vol_ total_resrc_ore_volume
    label var total_resrc_ore_volume "Volume of ore used to calculate resources, including inferred"
    replace year = "MstRctYear" if year == "2023"
    
    save "$temp_reserves/RR1_11.dta", replace

end

program combine_RR1_12
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_1_tonnage_volume_12_reserves_resources_ore_volume_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local year = 2023
    foreach var of varlist C-AI {
        local newname = "inf_ore_volume_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    reshape long inf_ore_volume_, i(prop_name prop_id) j(year) string
    rename inf_ore_volume_ inf_ore_volume
    label var inf_ore_volume "Volume of ore used to calculate inferred resources"
    replace year = "MstRctYear" if year == "2023"

    save "$temp_reserves/RR1_12.dta", replace

end

program combine_RR1_13
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_1_tonnage_volume_13_measured_indicated_ore_volume_incl_reserves_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local year = 2023
    foreach var of varlist C-AI {
        local newname = "meas_ind_ore_vol_incl_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    reshape long meas_ind_ore_vol_incl_, i(prop_name prop_id) j(year) string
    rename meas_ind_ore_vol_incl_ meas_ind_ore_vol_incl_resv
    label var meas_ind_ore_vol_incl_resv "Volume of ore used to calculate measured and indicated resources inclusive of reserves"
    replace year = "MstRctYear" if year == "2023"

    save "$temp_reserves/RR1_13.dta", replace

end

program combine_RR1_14
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_1_tonnage_volume_14_salable_reserves_ore_tonnage_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local year = 2023
    foreach var of varlist C-AI {
        local newname = "salable_resv_ore_t_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    reshape long salable_resv_ore_t_, i(prop_name prop_id) j(year) string
    rename salable_resv_ore_t_ salable_resv_ore_tonnage
    label var salable_resv_ore_tonnage "Mass of ore used to calculate salable reserves"
    replace year = "MstRctYear" if year == "2023"

    save "$temp_reserves/RR1_14.dta", replace

end

program combine_RR1_15
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_1_tonnage_volume_15_in-situ_value_reserves_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local year = 2023
    foreach var of varlist C-AI {
        local newname = "in_situ_value_resv_t_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    reshape long in_situ_value_resv_t_, i(prop_name prop_id) j(year) string
    rename in_situ_value_resv_t_ in_situ_value_resv_tonnage
    label var in_situ_value_resv_tonnage "Value of a project's mineable product identified as reserves"
    replace year = "MstRctYear" if year == "2023"

    save "$temp_reserves/RR1_15.dta", replace

end

program combine_RR1_16
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_1_tonnage_volume_16_in-situ_value_measured_indicated_excl_reserves_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local year = 2023
    foreach var of varlist C-AI {
        local newname = "in_situ_val_m_i_t_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    reshape long in_situ_val_m_i_t_, i(prop_name prop_id) j(year) string
    rename in_situ_val_m_i_t_ in_situ_value_m_and_i_t
    label var in_situ_value_m_and_i_t "Value of a project's mineable product identified as measured & indicated resources (excluding reserves)"
    replace year = "MstRctYear" if year == "2023"

    save "$temp_reserves/RR1_16.dta", replace

end

program combine_RR1_17
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_1_tonnage_volume_17_in-situ_value_inferred_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local year = 2023
    foreach var of varlist C-AI {
        local newname = "in_situ_value_inf_t_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    reshape long in_situ_value_inf_t_, i(prop_name prop_id) j(year) string
    rename in_situ_value_inf_t_ in_situ_value_inf_tonnage
    label var in_situ_value_inf_tonnage "Value of a project's mineable product identified as inferred resources"
    replace year = "MstRctYear" if year == "2023"

    save "$temp_reserves/RR1_17.dta", replace

end

program combine_RR1_18
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_1_tonnage_volume_18_in-situ_value_resources_exclu_reserves_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local year = 2023
    foreach var of varlist C-AI {
        local newname = "in_situ_value_resrc_t_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    reshape long in_situ_value_resrc_t_, i(prop_name prop_id) j(year) string
    rename in_situ_value_resrc_t_ in_situ_value_resrc_tonnage
    label var in_situ_value_resrc_tonnage "Value of a project's mineable product identified as resources, including inferred"
    replace year = "MstRctYear" if year == "2023"

    save "$temp_reserves/RR1_18.dta", replace

end

program combine_RR1_19
    clear
    local regions "Africa AsiaPacific EuropeMiddleEast LatinAmerica USCanada"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_1_tonnage_volume_19_in-situ_value_reserves_resources_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local year = 2023
    foreach var of varlist C-AI {
        local newname = "in_situ_value_r_and_r_t_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    reshape long in_situ_value_r_and_r_t_, i(prop_name prop_id) j(year) string
    rename in_situ_value_r_and_r_t_ in_situ_value_r_and_r_tonnage
    label var in_situ_value_r_and_r_tonnage "Value of a project's mineable product identified as any reserve or resource"
    replace year = "MstRctYear" if year == "2023"

    save "$temp_reserves/RR1_19.dta", replace

end

program combine_RR2_1
    clear
    local regions "Africa EmergingAsiaPacific LatinAmericaOthers Mexico Peru"
    local metals "gold palladium platinum rhodium silver"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_2_grade_contained_1_grade_reserves_precious_metals_1991_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    * all in cost
    local year = 2023
    unab vars : C-FK
    local i = 1
    foreach oldname of local vars {
        local metal : word `i' of `metals'
        local newname = "grd_resv_gpert_`year'_`metal'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        //label var `shortname' "Total per unit cost associated with production of metal in `year' (`metal')"

        local i = `i' + 1
        if (`i' > 5) {
            local i = 1
            local year = `year' - 1
         }
    }

    reshape long grd_resv_gpert_, i(prop_name prop_id) j(year_metal) string
    rename grd_resv_gpert_ grd_resv_gpert
    label var grd_resv_gpert "Grade of product in ore used to calculate reserves (g/tonne, oz)"
    split year_metal, parse("_")
    rename year_metal1 year
    rename year_metal2 metal
    //replace commodity = "heavy_mineral" if commodity == "heavy"
    drop year_metal
    //replace year = "MstRctYear" if year == "2023"

    save "$temp_reserves/RR2_1.dta", replace

end

program combine_RR2_2
    clear
    local regions "Africa EmergingAsiaPacific LatinAmericaOthers Mexico Peru"
    local metals "gold palladium platinum rhodium silver"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_2_grade_contained_2_contained_reserves_precious_metals_1991_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    * all in cost
    local year = 2023
    unab vars : C-FK
    local i = 1
    foreach oldname of local vars {
        local metal : word `i' of `metals'
        local newname = "contained_resv_oz_g_t_`year'_`metal'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'

        local i = `i' + 1
        if (`i' > 5) {
            local i = 1
            local year = `year' - 1
        }
    }

    reshape long contained_resv_oz_g_t_, i(prop_name prop_id) j(year_metal) string
    rename contained_resv_oz_g_t_ contained_resv_oz_g_t
    label var contained_resv_oz_g_t "The quantity of mineable product identified as reserves (g/tonne, oz)"
    split year_metal, parse("_")
    rename year_metal1 year
    rename year_metal2 metal
    replace metal = "palladium" if metal == "palla"
    replace metal = "platinum" if metal == "plati"
    replace metal = "rhodium" if metal == "rhodi"
    replace metal = "silver" if metal == "silve"
    drop year_metal

    save "$temp_reserves/RR2_2.dta", replace

end

program combine_RR2_7
    clear
    local regions "Africa EmergingAsiaPacific LatinAmericaOthers Mexico Peru"
    local metals "gold palladium platinum rhodium silver"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_2_grade_contained_7_grade_total_resources_excl_reserves_precious_metals_1991_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    * all in cost
    local year = 2023
    unab vars : C-FK
    local i = 1
    foreach oldname of local vars {
        local metal : word `i' of `metals'
        local newname = "grd_total_resrc_gpert_`year'_`metal'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'

        local i = `i' + 1
        if (`i' > 5) {
            local i = 1
            local year = `year' - 1
        }
    }

    reshape long grd_total_resrc_gpert_, i(prop_name prop_id) j(year_metal) string
    rename grd_total_resrc_gpert_ grd_total_resrc_gpert
    label var grd_total_resrc_gpert "Grade of product in ore used to calculate resources including inferred (g/tonne, oz)"
    split year_metal, parse("_")
    rename year_metal1 year
    rename year_metal2 metal
    replace metal = "palladium" if metal == "palla"
    replace metal = "platinum" if metal == "plati"
    replace metal = "rhodium" if metal == "rhodi"
    replace metal = "silver" if metal == "silve"
    drop year_metal

    save "$temp_reserves/RR2_7.dta", replace

end

program combine_RR2_8
    clear
    local regions "Africa EmergingAsiaPacific LatinAmericaOthers Mexico Peru"
    local metals "gold palladium platinum rhodium silver"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_2_grade_contained_8_contained_total_resources_excl_reserves_precious_metals_1991_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    * all in cost
    local year = 2023
    unab vars : C-FK
    local i = 1
    foreach oldname of local vars {
        local metal : word `i' of `metals'
        local newname = "contained_total_resrc_`year'_`metal'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'

        local i = `i' + 1
        if (`i' > 5) {
            local i = 1
            local year = `year' - 1
        }
    }

    reshape long contained_total_resrc_, i(prop_name prop_id) j(year_metal) string
    rename contained_total_resrc_ contained_total_resrc
    label var contained_total_resrc "The quantity of mineable product identified as resources, including inferred (g/tonne, oz)"
    split year_metal, parse("_")
    rename year_metal1 year
    rename year_metal2 metal
    replace metal = "palladium" if metal == "palla"
    replace metal = "platinum" if metal == "plati"
    replace metal = "rhodium" if metal == "rhodi"
    replace metal = "silver" if metal == "silve"
    drop year_metal

    save "$temp_reserves/RR2_8.dta", replace

end

program combine_RR2_9
    clear
    local regions "Africa EmergingAsiaPacific LatinAmericaOthers Mexico Peru"
    local metals "gold palladium platinum rhodium silver"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_2_grade_contained_9_grade_reserves_resources_precious_metals_1991_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    * all in cost
    local year = 2023
    unab vars : C-FK
    local i = 1
    foreach oldname of local vars {
        local metal : word `i' of `metals'
        local newname = "grd_r_and_r_gpert_`year'_`metal'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'

        local i = `i' + 1
        if (`i' > 5) {
            local i = 1
            local year = `year' - 1
        }
    }

    reshape long grd_r_and_r_gpert_, i(prop_name prop_id) j(year_metal) string
    rename grd_r_and_r_gpert_ grd_r_and_r_gpert
    label var grd_r_and_r_gpert "Grade of product in ore used to calculate total reserves and resources (g/tonne, oz)"
    split year_metal, parse("_")
    rename year_metal1 year
    rename year_metal2 metal
    /*
    replace metal = "palladium" if metal == "palla"
    replace metal = "platinum" if metal == "plati"
    replace metal = "rhodium" if metal == "rhodi"
    replace metal = "silver" if metal == "silve"
    */
    drop year_metal

    save "$temp_reserves/RR2_9.dta", replace

end

program combine_RR2_10
    clear
    local regions "Africa EmergingAsiaPacific LatinAmericaOthers Mexico Peru"
    local metals "gold palladium platinum rhodium silver"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_2_grade_contained_10_contained_reserves_resources_precious_metals_1991_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    * all in cost
    local year = 2023
    unab vars : C-FK
    local i = 1
    foreach oldname of local vars {
        local metal : word `i' of `metals'
        local newname = "contained_r_and_r_`year'_`metal'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'

        local i = `i' + 1
        if (`i' > 5) {
            local i = 1
            local year = `year' - 1
        }
    }

    reshape long contained_r_and_r_, i(prop_name prop_id) j(year_metal) string
    rename contained_r_and_r_ contained_r_and_r
    label var contained_r_and_r "The quantity of mineable product identified as any reserve or resource (g/tonne, oz)"
    split year_metal, parse("_")
    rename year_metal1 year
    rename year_metal2 metal
    /*
    replace metal = "palladium" if metal == "palla"
    replace metal = "platinum" if metal == "plati"
    replace metal = "rhodium" if metal == "rhodi"
    replace metal = "silver" if metal == "silve"
    */
    drop year_metal

    save "$temp_reserves/RR2_10.dta", replace

end

program combine_RR2_insitu_1
    clear
    local regions "Africa EmergingAsiaPacific LatinAmericaOthers Mexico Peru"
    local metals "gold palladium platinum rhodium silver"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_2_grade_contained_in-situ_value_reserves_precious_metals_1991_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    * all in cost
    local year = 2023
    unab vars : C-FK
    local i = 1
    foreach oldname of local vars {
        local metal : word `i' of `metals'
        local newname = "in_situ_value_resv_`year'_`metal'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'

        local i = `i' + 1
        if (`i' > 5) {
            local i = 1
            local year = `year' - 1
        }
    }

    reshape long in_situ_value_resv_, i(prop_name prop_id) j(year_metal) string
    rename in_situ_value_resv_ in_situ_value_resv
    label var in_situ_value_resv "Value of mineable product identified as reserves (g/tonne, oz)"
    split year_metal, parse("_")
    rename year_metal1 year
    rename year_metal2 metal
    /*
    replace metal = "palladium" if metal == "palla"
    replace metal = "platinum" if metal == "plati"
    replace metal = "rhodium" if metal == "rhodi"
    replace metal = "silver" if metal == "silve"
    */
    drop year_metal

    save "$temp_reserves/RR2_insitu_1.dta", replace

end

program combine_RR2_insitu_2
    clear
    local regions "Africa EmergingAsiaPacific LatinAmericaOthers Mexico Peru"
    local metals "gold palladium platinum rhodium silver"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_2_grade_contained_in-situ_value_resources_excl_reserves_precious_metals_1991_2023_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    * all in cost
    local year = 2023
    unab vars : C-FK
    local i = 1
    foreach oldname of local vars {
        local metal : word `i' of `metals'
        local newname = "insitu_val_resrc_`year'_`metal'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'

        local i = `i' + 1
        if (`i' > 5) {
            local i = 1
            local year = `year' - 1
        }
    }

    reshape long insitu_val_resrc_, i(prop_name prop_id) j(year_metal) string
    rename insitu_val_resrc_ insitu_val_resrc
    label var insitu_val_resrc "Value of mineable product identified as resources, including inferred (g/tonne, oz)"
    split year_metal, parse("_")
    rename year_metal1 year
    rename year_metal2 metal
    replace metal = "palladium" if metal == "palladi"
    replace metal = "platinum" if metal == "platinu"
    /*
    replace metal = "rhodium" if metal == "rhodi"
    replace metal = "silver" if metal == "silve"
    */
    drop year_metal

    save "$temp_reserves/RR2_insitu_2.dta", replace

end

program combine_RR2_insitu_3
    clear
    local regions "Africa EmergingAsiaPacific LatinAmericaOthers Mexico Peru"
    local metals "gold palladium platinum silver"

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_reserves/RR_2_grade_contained_in-situ_value_reserves_resources_precious_metals_1991_2000_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    * all in cost
    local year = 2000
    unab vars : C-AP
    local i = 1
    foreach oldname of local vars {
        local metal : word `i' of `metals'
        local newname = "insitu_value_r_and_r_`year'_`metal'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'

        local i = `i' + 1
        if (`i' > 4) {
            local i = 1
            local year = `year' - 1
        }
    }

    reshape long insitu_value_r_and_r_, i(prop_name prop_id) j(year_metal) string
    rename insitu_value_r_and_r_ insitu_value_r_and_r
    label var insitu_value_r_and_r "Value of mineable product identified as any reserve or resource (g/tonne, oz)"
    split year_metal, parse("_")
    rename year_metal1 year
    rename year_metal2 metal
    replace metal = "palladium" if metal == "pallad"
    replace metal = "platinum" if metal == "platin"
    /*
    replace metal = "rhodium" if metal == "rhodi"
    replace metal = "silver" if metal == "silve"
    */
    drop year_metal

    save "$temp_reserves/RR2_insitu_3.dta", replace

end

program combine_RR6 // time invariant data for reserves and resources
    clear all
    set more off

    local files "$input_reserves/RR_6_in-situ_evalutation_prices_Africa.xls" "$input_reserves/RR_6_in-situ_evalutation_prices_AsiaPacific.xls" "$input_reserves/RR_6_in-situ_evalutation_prices_EuropeMiddleEast.xls" "$input_reserves/RR_6_in-situ_evalutation_prices_LatinAmerica.xls" "$input_reserves/RR_6_in-situ_evalutation_prices_USCanada.xls"

    local first = 1
    foreach f of local files {
        import excel "`f'", cellrange(A4:AF4) firstrow clear
        local varlist
        foreach var of varlist _all {
            local varlist `varlist' `var'
        }
        import excel "`f'", cellrange(A7) firstrow clear
        rename (_all) (`varlist')
        foreach v of varlist _all {
            local lowername = lower("`v'")
            rename `v' `lowername'
        }
        // Set variable labels to full variable names
        foreach v of varlist _all {
            label variable `v' "`v'"
        }
        if `first' == 1 {
            tempfile base
            save `base'
            local first = 0
        }
        else {
            append using `base'
            save `base', replace
        }
    }
    
    save "$temp_reserves/RR6.dta", replace

end
