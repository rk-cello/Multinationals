**** combine time invariant data and construct cross-section data ****
**** notes ****
**** environment ****
clear all
set more off

* directories
global dir_raw "../../../data/raw"
global dir_temp "../../../data/temp"
global dir_cleaned "../../../data/raw_cleaned"

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