**** combine time invariant data and construct cross-section data ****
**** notes ****
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
global output_property_level "$dir_cleaned/S&P_cleaned/property_level"
global output_company_level "$dir_cleaned/S&P_cleaned/company_level"
global output_properties "$output_property_level/properties"

* intermediates
global temp_prop_details "$dir_temp/temp_prop_details"
global temp_production "$dir_temp/temp_production"
global temp_reserves_resources "$dir_temp/temp_reserves_resources"
global temp_tech_geo "$dir_temp/temp_tech_geo"
global temp_financings "$dir_temp/temp_financings"
global temp_most_recent_transactions "$dir_temp/temp_most_recent_transactions"
global temp_top_drill "$dir_temp/temp_top_drill"
global temp_claims "$dir_temp/temp_claims"
global temp_drill_results "$dir_temp/temp_drill_results"
global temp_transactions "$dir_temp/temp_transactions"
global temp_mine_econ_modeled "$dir_temp/temp_mine_econ_modeled"


************************************************************************

program main
    merge_time_variant_prop_details
    commodity_production_base_metals
    commodity_production_bulk_commodities
    commodity_production_precious_metals
    commodity_production_specialty_commodities
    merge_time_variant_production
    merge_time_variant_econ_modeled
    merge_time_variant_reserves_1
    merge_time_variant_reserves_2
    merge_time_variant_reserves_2_insitu
    merge_time_variant_reserves_3
    merge_time_variant_reserves_3_insitu_1
    merge_time_variant_reserves_3_insitu_2
    merge_time_variant_reserves_3_insitu_3
    merge_time_variant_reserves_4to5
end

***** property details ****
program merge_time_variant_prop_details
    clear all
    set more off

    * List of files to merge
    local files "$temp_prop_details/property_details_6_1.dta" "$temp_prop_details/property_details_6_2.dta" 

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use `first', clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        display "Merging file: `f'"
        merge 1:1 prop_name prop_id year using `f'
        drop _merge
    }

    // reorder to prop_name prop_id year and everythin else
    order prop_name prop_id year, first

    * Save the merged dataset
    save "$output_properties/properties_property_details_panel.dta", replace

end

***** production *****
program commodity_production_base_metals
    clear all
    set more off

    * List of files to append
    local files "$temp_production/production_4_12.dta" "$temp_production/production_4_13.dta" "$temp_production/production_4_14.dta" "$temp_production/production_4_15.dta"

    * append files
    use `: word 1 of `files'', clear
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        display "Appending file: `f'"
        append using `f'
    }

    replace base_metal = "ferronickel" if base_metal == "ferronicke"
    reshape wide commodity_prod_t_, i(prop_name prop_id year) j(base_metal) string

        label var commodity_prod_t_cobalt "Quantity of commodity produced (cobalt)"
        label var commodity_prod_t_copper "Quantity of commodity produced (copper)"
        label var commodity_prod_t_ferromolybdenum "Quantity of commodity produced (ferromolybdenum)"
        label var commodity_prod_t_ferronickel "Quantity of commodity produced (ferronickel)"
        label var commodity_prod_t_lead "Quantity of commodity produced (lead)"
        label var commodity_prod_t_molybdenum "Quantity of commodity produced (molybdenum)"
        label var commodity_prod_t_nickel "Quantity of commodity produced (nickel)"
        label var commodity_prod_t_tin "Quantity of commodity produced (tin)"
        label var commodity_prod_t_zinc "Quantity of commodity produced (zinc)"

    save "$temp_production/commodity_production_base_metals.dta", replace

end

program commodity_production_bulk_commodities
    clear all
    set more off

    * List of files to append
    local files "$temp_production/production_4_16.dta" "$temp_production/production_4_17.dta" "$temp_production/production_4_18.dta" "$temp_production/production_4_19.dta"

    * append files
    use `: word 1 of `files'', clear
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        display "Appending file: `f'"
        append using `f'
    }
    
    replace commodity = "ironore" if commodity == "iron_ore"
    duplicates drop prop_name prop_id year commodity commodity_prod_t_, force
    reshape wide commodity_prod_t_, i(prop_name prop_id year) j(commodity) string

        label var commodity_prod_t_alumina "Quantity of commodity produced (alumina)"
        label var commodity_prod_t_aluminum "Quantity of commodity produced (aluminum)"
        label var commodity_prod_t_bauxite "Quantity of commodity produced (bauxite)"
        label var commodity_prod_t_chromite "Quantity of commodity produced (chromite)"
        label var commodity_prod_t_chromium "Quantity of commodity produced (chromium)"
        label var commodity_prod_t_coal "Quantity of commodity produced (coal)"
        label var commodity_prod_t_ferrochrome "Quantity of commodity produced (ferrochrome)"
        label var commodity_prod_t_ferromanganese "Quantity of commodity produced (ferromanganese)"
        label var commodity_prod_t_ironore "Quantity of commodity produced (iron ore)"
        label var commodity_prod_t_manganese "Quantity of commodity produced (manganese)"
        label var commodity_prod_t_phosphate "Quantity of commodity produced (phosphate)"
        label var commodity_prod_t_potash "Quantity of commodity produced (potash)"

    save "$temp_production/commodity_production_bulk_commodities.dta", replace

end

program commodity_production_precious_metals
    clear all
    set more off

    * List of files to append
    
    local files "$temp_production/production_4_20.dta" "$temp_production/production_4_21.dta" "$temp_production/production_4_22.dta"* append files
    use `: word 1 of `files'', clear
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        display "Appending file: `f'"
        append using `f'
    }
    
    //replace commodity = "ironore" if commodity == "iron_ore"
    duplicates drop prop_name prop_id year commodity commodity_prod_t_, force
    reshape wide commodity_prod_t_, i(prop_name prop_id year) j(precious_metal) string

        label var commodity_prod_t_gold "Quantity of commodity produced (gold)"
        label var commodity_prod_t_palladium "Quantity of commodity produced (palladium)"
        label var commodity_prod_t_platinum "Quantity of commodity produced (platinum)"
        label var commodity_prod_t_silver "Quantity of commodity produced (silver)"

    save "$temp_production/commodity_production_precious_metals.dta", replace

end

program commodity_production_specialty_commodities
    clear all
    set more off

    * List of files to append
    local files "$temp_production/production_4_23.dta" "$temp_production/production_4_24.dta" "$temp_production/production_4_25.dta"

    * append files
    use `: word 1 of `files'', clear
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        display "Appending file: `f'"
        append using `f'
    }
    
    replace specialty_commodity = "lanthanides" if specialty_commodity == "lanthanide"
    //duplicates drop prop_name prop_id year commodity commodity_prod_t_, force
    reshape wide commodity_prod_t_, i(prop_name prop_id year) j(specialty_commodity) string

        label var commodity_prod_t_antimony "Quantity of commodity produced (antimony)"
        label var commodity_prod_t_ferrotungsten "Quantity of commodity produced (ferrotungsten)"
        label var commodity_prod_t_ferrovanadium "Quantity of commodity produced (ferrovanadium)"
        label var commodity_prod_t_graphite "Quantity of commodity produced (graphite)"
        label var commodity_prod_t_heavy_mineral "Quantity of commodity produced (heavy mineral sands)"
        label var commodity_prod_t_ilmenite "Quantity of commodity produced (ilmenite)"
        label var commodity_prod_t_lanthanides "Quantity of commodity produced (lanthanides)"
        label var commodity_prod_t_lithium "Quantity of commodity produced (lithium)"
        label var commodity_prod_t_niobium "Quantity of commodity produced (niobium)"
        label var commodity_prod_t_rutile "Quantity of commodity produced (rutile)"
        label var commodity_prod_t_scandium "Quantity of commodity produced (scandium)"
        label var commodity_prod_t_tantalum "Quantity of commodity produced (tantalum)"
        label var commodity_prod_t_titanium "Quantity of commodity produced (titanium)"
        label var commodity_prod_t_tungsten "Quantity of commodity produced (tungsten)"
        label var commodity_prod_t_vanadium "Quantity of commodity produced (vanadium)"
        label var commodity_prod_t_yttrium "Quantity of commodity produced (yttrium)"
        label var commodity_prod_t_zircon "Quantity of commodity produced (zircon)"

    save "$temp_production/commodity_production_specialty_commodities.dta", replace

end

program merge_time_variant_production
    clear all
    set more off

    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    cd "$dir_temp/temp_production"

    * First group of files (Ore production)
        local files1 "$temp_production/production_3_1.dta" "$temp_production/production_3_2.dta" "$temp_production/production_3_3.dta" "$temp_production/production_3_4.dta" "$temp_production/production_3_5.dta" "$temp_production/production_3_6.dta" "$temp_production/production_3_7.dta"
        local first1 : word 1 of `files1'
        use `first1', clear

        local nfiles1 : word count `files1'
        forvalues i = 2/`nfiles1' {
            local f : word `i' of `files1'
            display "Merging file: `f'"
            merge 1:1 prop_name prop_id year using `f'
            drop _merge
        }

        // rename and relabel variables
        local suffix "ore"
        foreach var of varlist mining_process_cost_per_t-production_certainty {
            local newname = "`var'_`suffix'"
            rename `var' `newname'
            label var `newname' "`: variable label `var'' (`suffix')"
        }

        save `temp_file', replace

        * Second group of files (Commodity production)
        local files2 "$temp_production/commodity_production_base_metals.dta" "$temp_production/commodity_production_bulk_commodities.dta" "$temp_production/commodity_production_precious_metals.dta" "$temp_production/commodity_production_specialty_commodities.dta"
        local first2 : word 1 of `files2'
        use `first2', clear

        local nfiles2 : word count `files2'
        forvalues i = 2/`nfiles2' {
            local f : word `i' of `files2'
            display "Merging file: `f'"
            merge 1:1 prop_name prop_id year using `f'
            drop _merge
        }

        // year variable to integer
        destring year, replace force

        merge 1:1 prop_name prop_id year using `temp_file'
        drop _merge
        save `temp_file', replace

        * Third group of files (Other)
        * List files
        local files3 "$temp_production/production_4_1.dta" "$temp_production/production_4_2.dta" "$temp_production/production_4_3.dta" "$temp_production/production_4_4.dta" ///
                "$temp_production/production_4_5.dta" "$temp_production/production_4_6.dta" "$temp_production/production_4_7.dta" "$temp_production/production_4_8.dta" ///
                "$temp_production/production_4_9.dta" "$temp_production/production_4_10.dta" "$temp_production/production_4_11.dta" ///
                "$temp_production/production_5_1.dta" "$temp_production/production_5_2.dta"

        * Load first file (master) and make year numeric once
        local first3 : word 1 of `files3'
        use `first3', clear
        destring year, replace force   // <- keep as numeric

        * Merge the rest
        local nfiles3 : word count `files3'
        forvalues i = 2/`nfiles3' {
            local f : word `i' of `files3'
            di as txt "Merging file: `f'"

            * Clean the using dataset so keys match types
            preserve
                use `f', clear
                destring year, replace force   // <- ensure numeric in using
                //duplicates drop prop_name prop_id year, force // remove duplicates
                tempfile clean_using
                save `clean_using'
            restore

            merge 1:1 prop_name prop_id year using `clean_using'
            drop _merge
        }


        // year variable to integer
        //destring year, replace force

        merge 1:1 prop_name prop_id year using `temp_file'
        drop _merge
        save `temp_file', replace

    order prop_name prop_id year, first

    * Save the merged dataset
    save "$output_properties/properties_production_panel.dta", replace

end

program merge_time_variant_econ_modeled
    clear all
    set more off

    * List of files to merge
    local files
    forvalues i = 1/18 {
        local files `files' "$temp_mine_econ_modeled/econ_modeled_`i'.dta"
    }

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use `first', clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        display "Merging file: `f'"
        merge 1:1 prop_name prop_id year using `f'
        drop _merge
    }

    // reorder to prop_name prop_id year and everythin else
    order prop_name prop_id year, first

    * Save the merged dataset
    save "$output_properties/properties_mine_econ_modeled_data_panel.dta", replace

end

program merge_time_variant_reserves_1
    clear all
    set more off

    * List of files to merge
    local files
    forvalues i = 1/19 {
        local files `files' "$temp_reserves_resources/RR1_`i'.dta"
    }

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use `first', clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        display "Merging file: `f'"
        merge 1:1 prop_name prop_id year using `f'
        drop _merge
    }

    // reorder to prop_name prop_id year and everythin else
    order prop_name prop_id year, first
    replace year = "2023" if year == "MstRctYear"
    destring year, replace

    * Save the merged dataset
    save "$output_properties/properties_reserves_resources_panel.dta", replace

end

program merge_time_variant_reserves_2
    clear all
    set more off

    * List of files to merge

    // build list of existing RR2 files
    local files ""
    forvalues i = 1/10 {
        local f "$temp_reserves_resources/RR2_`i'.dta"
        if fileexists("`f'") local files "`files' `f'"
    }
    // if no files found, exit gracefully
    /* if "`files'" == "" {
        di as err "No RR2 files found in $temp_reserves"
        exit 198
    } */

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use "`first'", clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        display "Merging file: `f'"
        merge 1:1 prop_name prop_id year metal using `f'
        drop _merge
    }
    
    // reorder to prop_name prop_id year and everythin else
    order prop_name prop_id year metal, first
    rename grd_resv_gpert grd_resv_
    rename contained_resv_oz_g_t contained_resv_
    rename grd_total_resrc_gpert grd_total_resrc_
    rename contained_total_resrc contained_total_resrc_
    rename grd_r_and_r_gpert grd_r_and_r_
    rename contained_r_and_r contained_r_and_r_

    //replace commodity = "ironore" if commodity == "iron_ore"
    //duplicates drop prop_name prop_id year commodity commodity_prod_t_, force
    reshape wide grd_resv contained_resv grd_total_resrc contained_total_resrc grd_r_and_r contained_r_and_r, i(prop_name prop_id year) j(metal) string
    destring year, replace

        label var grd_resv_gold "Grade of product in ore used to calculate reserves (g/t) (gold)"
        label var grd_resv_palladium "Grade of product in ore used to calculate reserves (g/t) (palladium)"
        label var grd_resv_platinum "Grade of product in ore used to calculate reserves (g/t) (platinum)"
        label var grd_resv_rhodium "Grade of product in ore used to calculate reserves (g/t) (rhodium)"
        label var grd_resv_silver "Grade of product in ore used to calculate reserves (g/t) (silver)"
        label var contained_resv_gold "Quantity of product identified as reserves (g/t) (gold)"
        label var contained_resv_palladium "Quantity of product identified as reserves (g/t) (palladium)"
        label var contained_resv_platinum "Quantity of product identified as reserves (g/t) (platinum)"
        label var contained_resv_rhodium "Quantity of product identified as reserves (g/t) (rhodium)"
        label var contained_resv_silver "Quantity of product identified as reserves (g/t) (silver)"
        label var grd_total_resrc_gold "Grade of product in ore used to calculate resources including inferred (g/t) (gold)"
        label var grd_total_resrc_palladium "Grade of product in ore used to calculate resources including inferred (g/t) (palladium)"
        label var grd_total_resrc_platinum "Grade of product in ore used to calculate resources including inferred (g/t) (platinum)"
        label var grd_total_resrc_rhodium "Grade of product in ore used to calculate resources including inferred (g/t) (rhodium)"
        label var grd_total_resrc_silver "Grade of product in ore used to calculate resources including inferred (g/t) (silver)"
        label var contained_total_resrc_gold "Quantity of product identified as resources including inferred (g/t) (gold)"
        label var contained_total_resrc_palladium "Quantity of product identified as resources including inferred (g/t) (palladium)"
        label var contained_total_resrc_platinum "Quantity of product identified as resources including inferred (g/t) (platinum)"
        label var contained_total_resrc_rhodium "Quantity of product identified as resources including inferred (g/t) (rhodium)"
        label var contained_total_resrc_silver "Quantity of product identified as resources including inferred (g/t) (silver)"
        label var grd_r_and_r_gold "Grade of product in ore used to calculate total reserves and resources (g/t) (gold)"
        label var grd_r_and_r_palladium "Grade of product in ore used to calculate total reserves and resources (g/t) (palladium)"
        label var grd_r_and_r_platinum "Grade of product in ore used to calculate total reserves and resources (g/t) (platinum)"
        label var grd_r_and_r_rhodium "Grade of product in ore used to calculate total reserves and resources (g/t) (rhodium)"
        label var grd_r_and_r_silver "Grade of product in ore used to calculate total reserves and resources (g/t) (silver)"
        label var contained_r_and_r_gold "Quantity of product identified as any reserve or resource (g/t) (gold)"
        label var contained_r_and_r_palladium "Quantity of product identified as any reserve or resource (g/t) (palladium)"
        label var contained_r_and_r_platinum "Quantity of product identified as any reserve or resource (g/t) (platinum)"
        label var contained_r_and_r_rhodium "Quantity of product identified as any reserve or resource (g/t) (rhodium)"
        label var contained_r_and_r_silver "Quantity of product identified as any reserve or resource (g/t) (silver)"
        

    * Save the merged dataset
    save "$output_properties/properties_reserves_resources_panel2.dta", replace

end

program merge_time_variant_reserves_2_insitu
    clear all
    set more off

    * List of files to merge

    // build list of existing RR2 files
    local files ""
    forvalues i = 1/5 {
        local f "$temp_reserves_resources/RR2_insitu_`i'.dta"
        if fileexists("`f'") local files "`files' `f'"
    }

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use "`first'", clear
    replace metal = "palladium" if metal == "palladiu"

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        display "Merging file: `f'"
        merge 1:1 prop_name prop_id year metal using `f'
        drop _merge
    }
    
    order prop_name prop_id year metal, first
    rename in_situ_value_resv insitu_val_resv_
    rename insitu_val_resrc insitu_val_resrc_  
    rename insitu_value_r_and_r insitu_val_r_and_r_
    
    reshape wide insitu_val_resv_ insitu_val_resrc_ insitu_val_r_and_r_, i(prop_name prop_id year) j(metal) string
    destring year, replace

        label var insitu_val_resv_gold "Value of mineable product identified as reserves (gold)"
        label var insitu_val_resv_palladium "Value of mineable product identified as reserves (palladium)"
        label var insitu_val_resv_platinum "Value of mineable product identified as reserves (platinum)"
        label var insitu_val_resv_rhodium "Value of mineable product identified as reserves (rhodium)"
        label var insitu_val_resv_silver "Value of mineable product identified as reserves (silver)"

        label var insitu_val_resrc_gold "Value of mineable product identified as resources, including inferred (gold)"
        label var insitu_val_resrc_palladium "Value of mineable product identified as resources, including inferred (palladium)"
        label var insitu_val_resrc_platinum "Value of mineable product identified as resources, including inferred (platinum)"
        label var insitu_val_resrc_rhodium "Value of mineable product identified as resources, including inferred (rhodium)"
        label var insitu_val_resrc_silver "Value of mineable product identified as resources, including inferred (silver)"

        label var insitu_val_r_and_r_gold "Value of mineable product identified as any reserve or resource (gold)"
        label var insitu_val_r_and_r_palladium "Value of mineable product identified as any reserve or resource (palladium)"
        label var insitu_val_r_and_r_platinum "Value of mineable product identified as any reserve or resource (platinum)"
        label var insitu_val_r_and_r_rhodium "Value of mineable product identified as any reserve or resource (rhodium)"
        label var insitu_val_r_and_r_silver "Value of mineable product identified as any reserve or resource (silver)"
        
    * Save the merged dataset
    save "$output_properties/properties_reserves_resources_panel3.dta", replace

end

program merge_time_variant_reserves_3
    clear all
    set more off

    * List of files to merge

    // build list of existing RR3 files
    local files ""
    forvalues i = 1/10 {
        local f "$temp_reserves_resources/RR3_`i'.dta"
        if fileexists("`f'") local files "`files' `f'"
    }

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use "`first'", clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        display "Merging file: `f'"
        merge 1:1 prop_name prop_id year metal using `f'
        drop _merge
    }
    
    // reorder to prop_name prop_id year and everythin else
    order prop_name prop_id year metal, first
    rename grd_resv_pct_t grd_resv_
    rename contained_resv_pct_t contained_resv_
    rename grd_total_resrc_pct_t grd_total_resrc_
    rename contained_tot_resrc_pct contained_tot_resrc_
    rename grd_r_and_r_pct_t grd_r_and_r_
    rename contained_r_and_r_pct contained_r_and_r_

    reshape wide grd_resv_ contained_resv_ grd_total_resrc_ contained_tot_resrc_ grd_r_and_r_ contained_r_and_r_, i(prop_name prop_id year) j(metal) string
    destring year, replace

        label var grd_resv_cobalt "Grade of product in ore used to calculate reserves (g/t) (cobalt)"
        label var grd_resv_copper "Grade of product in ore used to calculate reserves (g/t) (copper)"
        label var grd_resv_lead "Grade of product in ore used to calculate reserves (g/t) (lead)"
        label var grd_resv_molybdenum "Grade of product in ore used to calculate reserves (g/t) (molybdenum)"
        label var grd_resv_nickel "Grade of product in ore used to calculate reserves (g/t) (nickel)"
        label var grd_resv_tin "Grade of product in ore used to calculate reserves (g/t) (tin)"
        label var grd_resv_zinc "Grade of product in ore used to calculate reserves (g/t) (zinc)"

        label var contained_resv_cobalt "Quantity of product identified as reserves (g/t) (cobalt)"
        label var contained_resv_copper "Quantity of product identified as reserves (g/t) (copper)"
        label var contained_resv_lead "Quantity of product identified as reserves (g/t) (lead)"
        label var contained_resv_molybdenum "Quantity of product identified as reserves (g/t) (molybdenum)"
        label var contained_resv_nickel "Quantity of product identified as reserves (g/t) (nickel)"
        label var contained_resv_tin "Quantity of product identified as reserves (g/t) (tin)"
        label var contained_resv_zinc "Quantity of product identified as reserves (g/t) (zinc)"

        label var grd_total_resrc_cobalt "Grade of product in ore used to calculate resources including inferred (g/t) (cobalt)"
        label var grd_total_resrc_copper "Grade of product in ore used to calculate resources including inferred (g/t) (copper)"
        label var grd_total_resrc_lead "Grade of product in ore used to calculate resources including inferred (g/t) (lead)"
        label var grd_total_resrc_molybdenum "Grade of product in ore used to calculate resources including inferred (g/t) (molybdenum)"
        label var grd_total_resrc_nickel "Grade of product in ore used to calculate resources including inferred (g/t) (nickel)"
        label var grd_total_resrc_tin "Grade of product in ore used to calculate resources including inferred (g/t) (tin)"
        label var grd_total_resrc_zinc "Grade of product in ore used to calculate resources including inferred (g/t) (zinc)"

        label var contained_tot_resrc_cobalt "Quantity of product identified as resources including inferred (g/t) (cobalt)"
        label var contained_tot_resrc_copper "Quantity of product identified as resources including inferred (g/t) (copper)"
        label var contained_tot_resrc_lead "Quantity of product identified as resources including inferred (g/t) (lead)"
        label var contained_tot_resrc_molybdenum "Quantity of product identified as resources including inferred (g/t) (molybdenum)"
        label var contained_tot_resrc_nickel "Quantity of product identified as resources including inferred (g/t) (nickel)"
        label var contained_tot_resrc_tin "Quantity of product identified as resources including inferred (g/t) (tin)"
        label var contained_tot_resrc_zinc "Quantity of product identified as resources including inferred (g/t) (zinc)"
        
        label var grd_r_and_r_cobalt "Grade of product in ore used to calculate total reserves and resources (g/t) (cobalt)"
        label var grd_r_and_r_copper "Grade of product in ore used to calculate total reserves and resources (g/t) (copper)"
        label var grd_r_and_r_lead "Grade of product in ore used to calculate total reserves and resources (g/t) (lead)"    
        label var grd_r_and_r_molybdenum "Grade of product in ore used to calculate total reserves and resources (g/t) (molybdenum)"
        label var grd_r_and_r_nickel "Grade of product in ore used to calculate total reserves and resources (g/t) (nickel)"
        label var grd_r_and_r_tin "Grade of product in ore used to calculate total reserves and resources (g/t) (tin)"
        label var grd_r_and_r_zinc "Grade of product in ore used to calculate total reserves and resources (g/t) (zinc)"
        
        label var contained_r_and_r_cobalt "Quantity of product identified as any reserve or resource (g/t) (cobalt)"
        label var contained_r_and_r_copper "Quantity of product identified as any reserve or resource (g/t) (copper)"
        label var contained_r_and_r_lead "Quantity of product identified as any reserve or resource (g/t) (lead)"
        label var contained_r_and_r_molybdenum "Quantity of product identified as any reserve or resource (g/t) (molybdenum)"
        label var contained_r_and_r_nickel "Quantity of product identified as any reserve or resource (g/t) (nickel)"
        label var contained_r_and_r_tin "Quantity of product identified as any reserve or resource (g/t) (tin)"
        label var contained_r_and_r_zinc "Quantity of product identified as any reserve or resource (g/t) (zinc)"

    * Save the merged dataset
    save "$output_properties/properties_reserves_resources_panel4.dta", replace

end

program merge_time_variant_reserves_3_insitu_1
    clear all
    set more off

    * List of files to merge

    // build list of existing RR3_insitu files
    local files ""
    forvalues i = 1/5 {
        local f "$temp_reserves_resources/RR3_insitu_`i'.dta"
        if fileexists("`f'") local files "`files' `f'"
    }

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use "`first'", clear
    //replace metal = "palladium" if metal == "palladiu"

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        display "Merging file: `f'"
        merge 1:1 prop_name prop_id year metal using `f'
        drop _merge
    }
    
    order prop_name prop_id year metal, first
    rename in_situ_value_resv_pct insitu_val_resv_
    rename in_situ_value_resrc_pct insitu_val_resrc_  
    rename in_situ_val_r_and_r_pct insitu_val_r_and_r_
    
    reshape wide insitu_val_resv_ insitu_val_resrc_ insitu_val_r_and_r_, i(prop_name prop_id year) j(metal) string
    destring year, replace

        label var insitu_val_resv_nickel "Value of mineable product identified as reserves (nickel)"
        label var insitu_val_resv_tin "Value of mineable product identified as reserves (tin)"
        label var insitu_val_resv_zinc "Value of mineable product identified as reserves (zinc)"
        label var insitu_val_resv_cobalt "Value of mineable product identified as reserves (cobalt)"
        label var insitu_val_resv_copper "Value of mineable product identified as reserves (copper)"
        label var insitu_val_resv_lead "Value of mineable product identified as reserves (lead)"
        label var insitu_val_resv_molybdenum "Value of mineable product identified as reserves (molybdenum)"

        label var insitu_val_resrc_nickel "Value of mineable product identified as resources, including inferred (nickel)"
        label var insitu_val_resrc_tin "Value of mineable product identified as resources, including inferred (tin)"
        label var insitu_val_resrc_zinc "Value of mineable product identified as resources, including inferred (zinc)"
        label var insitu_val_resrc_cobalt "Value of mineable product identified as resources, including inferred (cobalt)"
        label var insitu_val_resrc_copper "Value of mineable product identified as resources, including inferred (copper)"
        label var insitu_val_resrc_lead "Value of mineable product identified as resources, including inferred (lead)"
        label var insitu_val_resrc_molybdenum "Value of mineable product identified as resources, including inferred (molybdenum)"

        label var insitu_val_r_and_r_nickel "Value of mineable product identified as any reserve or resource (nickel)"
        label var insitu_val_r_and_r_tin "Value of mineable product identified as any reserve or resource (tin)"
        label var insitu_val_r_and_r_zinc "Value of mineable product identified as any reserve or resource (zinc)"
        label var insitu_val_r_and_r_cobalt "Value of mineable product identified as any reserve or resource (cobalt)"
        label var insitu_val_r_and_r_copper "Value of mineable product identified as any reserve or resource (copper)"
        label var insitu_val_r_and_r_lead "Value of mineable product identified as any reserve or resource (lead)"
        label var insitu_val_r_and_r_molybdenum "Value of mineable product identified as any reserve or resource (molybdenum)"
        
    * Save the merged dataset
    save "$output_properties/properties_reserves_resources_panel5.dta", replace

end

program merge_time_variant_reserves_3_insitu_2
    clear all
    set more off

    * List of files to merge

    // build list of existing RR3_insitu files
    local files ""
    forvalues i = 6/8 {
        local f "$temp_reserves_resources/RR3_insitu_`i'.dta"
        if fileexists("`f'") local files "`files' `f'"
    }

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use "`first'", clear
    //replace metal = "palladium" if metal == "palladiu"

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        display "Merging file: `f'"
        merge 1:1 prop_name prop_id year metal using `f'
        drop _merge
    }
    
    order prop_name prop_id year metal, first 
    rename in_situ_value_r_and_r_pct insitu_val_r_and_r_
    
    reshape wide insitu_val_r_and_r_, i(prop_name prop_id year) j(metal) string
    destring year, replace

        label var insitu_val_r_and_r_bauxite "Value of mineable product identified as any reserve or resource (bauxite)"
        label var insitu_val_r_and_r_beryllium "Value of mineable product identified as any reserve or resource (beryllium)"
        label var insitu_val_r_and_r_chromite "Value of mineable product identified as any reserve or resource (chromite)"
        label var insitu_val_r_and_r_chromium "Value of mineable product identified as any reserve or resource (chromium)"
        label var insitu_val_r_and_r_iron_ore "Value of mineable product identified as any reserve or resource (iron ore)"
        label var insitu_val_r_and_r_manganese "Value of mineable product identified as any reserve or resource (manganese)"
        label var insitu_val_r_and_r_phosphate "Value of mineable product identified as any reserve or resource (phosphate)"
        label var insitu_val_r_and_r_potash "Value of mineable product identified as any reserve or resource (potash)"
        
    * Save the merged dataset
    save "$output_properties/properties_reserves_resources_panel6.dta", replace

end

program merge_time_variant_reserves_3_insitu_3
    clear all
    set more off

    * List of files to merge

    // build list of existing RR3_insitu files
    local files ""
    forvalues i = 9/11 {
        local f "$temp_reserves_resources/RR3_insitu_`i'.dta"
        if fileexists("`f'") local files "`files' `f'"
    }

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use "`first'", clear
    //replace metal = "palladium" if metal == "palladiu"

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        display "Merging file: `f'"
        merge 1:1 prop_name prop_id year metal using `f'
        drop _merge
    }
    
    order prop_name prop_id year metal, first 
    rename in_situ_value_r_and_r_pct insitu_val_r_and_r_
    replace metal = "sands" if metal == "heavy_mineral_sands"
    
    reshape wide insitu_val_r_and_r_, i(prop_name prop_id year) j(metal) string
    destring year, replace

        label var insitu_val_r_and_r_antimony "Value of mineable product identified as any reserve or resource (antimony)"
        label var insitu_val_r_and_r_graphite "Value of mineable product identified as any reserve or resource (graphite)"
        label var insitu_val_r_and_r_sands "Value of mineable product identified as any reserve or resource (heavy mineral sands)"
        label var insitu_val_r_and_r_ilmenite "Value of mineable product identified as any reserve or resource (ilmenite)"
        label var insitu_val_r_and_r_lanthanides "Value of mineable product identified as any reserve or resource (lanthanides)"
        label var insitu_val_r_and_r_lithium "Value of mineable product identified as any reserve or resource (lithium)"
        label var insitu_val_r_and_r_niobium "Value of mineable product identified as any reserve or resource (niobium)"
        label var insitu_val_r_and_r_rutile "Value of mineable product identified as any reserve or resource (rutile)"
        label var insitu_val_r_and_r_scandium "Value of mineable product identified as any reserve or resource (scandium)"
        label var insitu_val_r_and_r_tantalum "Value of mineable product identified as any reserve or resource (tantalum)"
        label var insitu_val_r_and_r_titanium "Value of mineable product identified as any reserve or resource (titanium)"
        label var insitu_val_r_and_r_tungsten "Value of mineable product identified as any reserve or resource (tungsten)"
        label var insitu_val_r_and_r_vanadium "Value of mineable product identified as any reserve or resource (vanadium)"
        label var insitu_val_r_and_r_yttrium "Value of mineable product identified as any reserve or resource (yttrium)"
        label var insitu_val_r_and_r_zircon "Value of mineable product identified as any reserve or resource (zircon)"
        
    * Save the merged dataset
    save "$output_properties/properties_reserves_resources_panel7.dta", replace

end

program merge_time_variant_reserves_4to5
    clear all
    set more off

    * List of files to merge

    local files "$temp_reserves_resources/RR4_1.dta" "$temp_reserves_resources/RR4_2.dta" "$temp_reserves_resources/RR4_insitu_1.dta" "$temp_reserves_resources/RR5_1.dta" "$temp_reserves_resources/RR5_insitu_1.dta"

    * Use the first file as the master dataset
    //local first : word 1 of `files'
    use "$temp_reserves_resources/RR4_1.dta", clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        display "Merging file: `f'"
        merge 1:1 prop_name prop_id year using `f'
        drop _merge
    }

    destring year, replace
    * Save the merged dataset
    save "$output_properties/properties_reserves_resources_panel8.dta", replace

end

program merge_time_variant_reserves_all
    clear all
    set more off

    * List of files to merge
    local files "$output_properties/properties_reserves_resources_panel.dta" ///
                "$output_properties/properties_reserves_resources_panel2.dta" ///
                "$output_properties/properties_reserves_resources_panel3.dta" ///
                "$output_properties/properties_reserves_resources_panel4.dta" ///
                "$output_properties/properties_reserves_resources_panel5.dta" ///
                "$output_properties/properties_reserves_resources_panel6.dta" ///
                "$output_properties/properties_reserves_resources_panel7.dta" ///
                "$output_properties/properties_reserves_resources_panel8.dta"

    * Use the first file as the master dataset
    use "$output_properties/properties_reserves_resources_panel.dta", clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        display "Merging file: `f'"
        merge 1:1 prop_name prop_id year using `f'
        drop _merge
    }

    // reorder to prop_name prop_id year and everythin else
    order prop_name prop_id year, first

    * Save the merged dataset
    save "$output_properties/properties_reserves_resources_panel_all.dta", replace
end

program merge_time_variant
    clear all
    set more off

    * List of files to merge
    local files "$output_properties/properties_property_details_panel.dta" ///
                "$output_properties/properties_production_panel.dta" ///
                "$output_properties/properties_mine_econ_modeled_data_panel.dta" ///
                "$output_properties/properties_reserves_resources_panel_all.dta" ///
                

    * Use the first file as the master dataset
    use "$output_properties/properties_property_details_panel.dta", clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        display "Merging file: `f'"
        merge 1:1 prop_name prop_id year using `f'
        drop _merge
    }

    // reorder to prop_name prop_id year and everythin else
    order prop_name prop_id year, first

    * Save the merged dataset
    save "$output_property_level/property_level_panel_data.dta", replace
end