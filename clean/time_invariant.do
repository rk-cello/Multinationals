**** combine time invariant data and construct cross-section data ****
**** notes ****
* some cross-section data are not at the property level, and thus excluded from the property_level output folder
* these include...
* claims: claim owner level data linked to properties
* drill results: drill results level data linked to properties
* capital costs: capital cost level data linked to properties
* trasactions: transaction level
**** environment ****
clear all
set more off

* directories
global dir_raw "../../../data/raw"
global dir_temp "../../../data/temp"
global dir_cleaned "../../../data/raw_cleaned/S&P_cleaned"

* inputs
global input_metals_mining "$dir_raw/data_S&P/metals_mining"

* outputs
global output_property_level "$dir_cleaned/property_level"
global output_company_level "$dir_cleaned/company_level"
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

************************************************************************

program main
    merge_time_invariant_prop_details //done
    merge_time_invariant_production //done
    merge_time_invariant_reserves //done
    merge_time_invariant_tech_geo //done
    merge_time_invariant_financings //done
    merge_time_invariant_most_recent_transactions //done
    merge_time_invariant_top_drill_results //done
    merge_time_invariant_claims //done
    merge_time_invariant_drill_results //done
    merge_time_invariant_capital_costs //done
    merge_time_invariant_transactions //done
    merge_time_invariant 
end

**** PROPERTY DETAILS DATA MERGE ****

program merge_time_invariant_prop_details

    clear all
    set more off

    * List of files to merge
    local files "$temp_prop_details/property_details_1.dta" "$temp_prop_details/property_details_2.dta" "$temp_prop_details/property_details_3_1.dta" "$temp_prop_details/property_details_3_2.dta" "$temp_prop_details/property_details_4.dta" "$temp_prop_details/property_details_5_1.dta" "$temp_prop_details/property_details_5_2.dta" "$temp_prop_details/property_details_5_3.dta" "$temp_prop_details/property_details_5_4.dta" "$temp_prop_details/property_details_5_5.dta" "$temp_prop_details/property_details_5_6.dta" "$temp_prop_details/property_details_5_7.dta" "$temp_prop_details/property_details_5_8.dta" "$temp_prop_details/property_details_5_9.dta" "$temp_prop_details/property_details_5_10.dta" "$temp_prop_details/property_details_5_11.dta" "$temp_prop_details/property_details_5_12.dta" "$temp_prop_details/property_details_5_13.dta" "$temp_prop_details/property_details_5_14.dta" "$temp_prop_details/property_details_5_15.dta" "$temp_prop_details/property_details_7_1.dta" "$temp_prop_details/property_details_7_2.dta" "$temp_prop_details/property_details_7_3.dta" "$temp_prop_details/property_details_8.dta" "$temp_prop_details/property_details_9_1.dta" "$temp_prop_details/property_details_9_2.dta" "$temp_prop_details/property_details_9_3.dta" "$temp_prop_details/property_details_9_4.dta" "$temp_prop_details/property_details_9_5.dta" ///
        /* "$temp_prop_details/property_details_10_1.dta" "$temp_prop_details/property_details_10_2.dta" "$temp_prop_details/property_details_10_3.dta" /// */ // omitted due to time variant data
        "$temp_prop_details/property_details_11_1.dta" "$temp_prop_details/property_details_11_2.dta"

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use `first', clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        display "Merging file: `f'"
        merge 1:1 prop_name prop_id using `f'
        drop _merge
    }

    * Save the merged dataset
    save "$output_properties/properties_property_details_crosssection.dta", replace

end


program merge_time_invariant_production
    
    clear all
    set more off

    * List of files to merge
    local files "$temp_production/production_1.dta" "$temp_production/production_2_1.dta" "$temp_production/production_2_2.dta" "$temp_production/production_2_3.dta" "$temp_production/production_2_4.dta" "$temp_production/production_2_5.dta" "$temp_production/production_2_6.dta"

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use `first', clear

    rename mill_capacity_tonnes_per_day mill_capacity_t_per_dy
    rename mill_capacity_tonnes_per_year mill_capacity_t_per_yr
    rename mill_capacity_cubic_m_per_day mill_capacity_cubic_m_per_dy
    rename mill_capacity_cubic_m_per_year mill_capacity_cubic_m_per_yr
    rename mining_produ_general_comments mining_prod_gen_comments
    rename mining_processing_cost_mt mining_process_cost_mt
    rename mining_processing_cost_cubic_m mining_process_cost_cubic_m
    local suffix "ore"
    foreach var of varlist start_up_yr-mining_process_cost_cubic_m {
        local newname = "`var'_`suffix'"
        rename `var' `newname'
        label var `newname' "`: variable label `var'' (`suffix')"
    }

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        merge 1:1 prop_name prop_id using `f'
        drop _merge
    }

    * Save the merged dataset
    save "$output_properties/properties_production_crosssection.dta", replace

end

program merge_time_invariant_reserves

    use "$temp_reserves_resources/RR6.dta"
    save "$output_properties/properties_reserves_resources_crosssection.dta", replace

end

program merge_time_invariant_tech_geo

    use "$temp_tech_geo/tech_geo.dta"
    save "$output_properties/properties_technical_geology_crosssection.dta", replace

end

program merge_time_invariant_financings
    
    clear all
    set more off

    * List of files to merge
    local files "$temp_financings/financings_1_1.dta" "$temp_financings/financings_1_2.dta" "$temp_financings/financings_1_3.dta" "$temp_financings/financings_2.dta"

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use `first', clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        merge 1:1 prop_name prop_id using `f'
        drop _merge
    }

    * Save the merged dataset
    save "$output_properties/properties_financings_crosssection.dta", replace

end

program merge_time_invariant_most_recent_transactions
    
    clear all
    set more off

    * List of files to merge
    local files "$temp_most_recent_transactions/transaction_details_1.dta" "$temp_most_recent_transactions/transaction_details_2.dta" "$temp_most_recent_transactions/transaction_details_3.dta" "$temp_most_recent_transactions/transaction_details_4.dta" "$temp_most_recent_transactions/transaction_details_5.dta"

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use `first', clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        merge 1:1 prop_name prop_id using `f'
        drop _merge
    }

    * Save the merged dataset
    save "$output_properties/properties_most_recent_transactions_crosssection.dta", replace

end

program merge_time_invariant_top_drill_results
    
    clear all
    set more off

    * List of files to merge
    local files "$temp_top_drill/top_drill_1_1.dta" "$temp_top_drill/top_drill_1_2.dta" "$temp_top_drill/top_drill_1_3.dta" "$temp_top_drill/top_drill_2_1.dta" "$temp_top_drill/top_drill_2_2.dta" "$temp_top_drill/top_drill_2_3.dta" "$temp_top_drill/top_drill_2_4.dta" "$temp_top_drill/top_drill_3_1.dta" "$temp_top_drill/top_drill_3_2.dta" "$temp_top_drill/top_drill_3_3.dta" "$temp_top_drill/top_drill_3_4.dta" "$temp_top_drill/top_drill_3_bulk_1.dta" "$temp_top_drill/top_drill_3_bulk_2.dta" "$temp_top_drill/top_drill_3_bulk_3.dta" "$temp_top_drill/top_drill_3_bulk_4.dta" "$temp_top_drill/top_drill_3_U3O8.dta" "$temp_top_drill/most_recent_drill.dta"

    * Use the first file as the master dataset
    local first : word 1 of `files'
    use `first', clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        merge 1:1 prop_name prop_id using `f'
        drop _merge
    }

    * Save the merged dataset
    save "$output_properties/properties_top_drill_results_crosssection.dta", replace

end

program merge_time_invariant_claims

    use "$temp_claims/claims_linked_to_properties.dta"
    rename RelatedProperty prop_name
    rename PropertyKey prop_id
    tostring prop_id, replace
    save "$dir_cleaned/claims_crosssection.dta", replace
    
end

program merge_time_invariant_drill_results

    use "$temp_drill_results/drill_results_1&2.dta", clear
    tostring prop_id, replace
    save "$dir_cleaned/drill_results_crosssection.dta", replace

end

program merge_time_invariant_capital_costs

    use "$temp_drill_results/capital_costs_global.dta", clear
    save "$dir_cleaned/capital_costs_crosssection.dta", replace

end

program merge_time_invariant_transactions
    clear all
    set more off

    * List of files to merge
    
    local files "$temp_transactions/transactions_1.dta" "$temp_transactions/transactions_2.dta" "$temp_transactions/transactions_3.dta" "$temp_transactions/transactions_4.dta" "$temp_transactions/transactions_5.dta" "$temp_transactions/transactions_6.dta" "$temp_transactions/transactions_7.dta" "$temp_transactions/transactions_8.dta" "$temp_transactions/transactions_9.dta" "$temp_transactions/transactions_10.dta" "$temp_transactions/transactions_11.dta"
    * Use the first file as the master dataset
    local first : word 1 of `files'
    use `first', clear

    * Loop through the rest and merge
    local nfiles : word count `files'
    forvalues i = 2/`nfiles' {
        local f : word `i' of `files'
        merge 1:1 sptr_target_name sptr_mi_transaction_id using `f'
        drop _merge
    }

    rename AW transaction_geography
    * Save the merged dataset
    save "$dir_cleaned/transactions_crosssection.dta", replace

end

program merge_time_invariant 
    clear all
    use "$output_properties/properties_property_details_crosssection.dta"
    merge 1:1 prop_name prop_id using "$output_properties/properties_production_crosssection.dta", nogenerate
    merge 1:1 prop_name prop_id using "$output_properties/properties_reserves_resources_crosssection.dta", nogenerate
    merge 1:1 prop_name prop_id using "$output_properties/properties_technical_geology_crosssection.dta", nogenerate
    merge 1:1 prop_name prop_id using "$output_properties/properties_financings_crosssection.dta", nogenerate
    merge 1:1 prop_name prop_id using "$output_properties/properties_most_recent_transactions_crosssection.dta", nogenerate
    merge 1:1 prop_name prop_id using "$output_properties/properties_top_drill_results_crosssection.dta", nogenerate
    
    //merge 1:1 prop_name prop_id using "$output_property_level/claims_crosssection.dta", nogenerate
    // variables prop_name prop_id do not uniquely identify observations in the using data

    //merge 1:1 prop_name prop_id using "$output_property_level/drill_results_crosssection.dta", nogenerate
    // variables prop_name prop_id do not uniquely identify observations in the using data

    /* merge 1:1 prop_name prop_id using , nogenerate
    merge 1:1 prop_name prop_id using , nogenerate */

    save "$output_property_level/property_level_crosssection_data.dta", replace
    // does not include capital costs and transactions 

end

program rename_long_vars
    clear all
    use "$output_property_level/property_level_crosssection_data.dta"
    * ----------------------------------------------------------------------
    * STATA SCRIPT: BULK RENAME FOR LONG VARIABLES
    * ----------------------------------------------------------------------

    * 1. METRICS & COSTS (The "Prefixes")
    * We shorten the description of the cost/grade to make room for the element name.
    * ----------------------------------------------------------------------
    * "All-in Sustaining Cost" -> aisc
    rename *all_sustain_cost* *aisc*

    * "Total Production Cost" -> tot_cost
    rename *total_prod_cost* *tot_cost*

    * "Production Capacity" -> prod_cap
    rename *production_capacity* *prod_cap*

    * "Millhead Grade" -> mill_gr
    rename *millhead_grade* *mill_gr*

    * "Evaluation Price" -> eval_px
    rename *evaluation_price* *eval_px*

    * "Cash Cost" -> c_cost
    rename *cash_cost* *c_cost*

    * "Production Cost" -> prod_cost
    rename *production_cost* *prod_cost*

    * "Current" -> cur
    rename *current_* *cur_*

    * "Recov Rate" -> rec_rt
    rename *recov_rate* *rec_rt*


    * 2. GEOLOGICAL INTERVALS (The "Middle" chunks)
    * These are critical to shorten because they are often followed by numbers.
    * ----------------------------------------------------------------------
    * "Interval Grade Pct" -> int_gr_pct
    rename *interval_grade_pct* *int_gr_pct*

    * "Grade Equiv X Interval" -> greq_x_int
    rename *grade_equiv_x_int* *greq_x_int*
    rename *grade_eq_x_int* *greq_x_int* 
    
    * "Grade X Interval" -> gr_x_int
    rename *grade_x_interval* *gr_x_int*
    rename *grade_x_int* *gr_x_int*

    * "Primary Interval" -> prim_int
    rename *primary_interval* *prim_int*


    * 3. DATES & DEALS
    * ----------------------------------------------------------------------
    * "Completion Termination" -> comp_term
    rename *completion_termination* *comp_term*

    * "Deal Pct Acquired" -> pct_acq
    rename *deal_pct_acquired* *pct_acq*

    * "Total Deal Value" -> tot_deal_val
    rename *total_deal_value* *tot_deal_val*

    * "Announce" -> ann
    rename *announce* *ann*

    * "Complete" -> comp
    rename *complete* *comp*

    rename *evaluation* *eval*


    * 4. COMMODITIES (The "Suffixes")
    * Shortening long chemical names is the most effective way to save space.
    * ----------------------------------------------------------------------
    rename *ferromolybdenum* *femoly*
    rename *ferromanganese* *femang*
    rename *ferromanganes* *femang*
    rename *ferrotungsten* *fetung*
    rename *ferrovanadium* *fevan*
    rename *ferronickel* *fenick*
    rename *ferrochrome* *fechro*
    rename *ferrochrom* *fechro*

    rename *heavy_mineral* *hvy_min*
    rename *lanthanides* *lanth*
    rename *lanthanide* *lanth*
    rename *molybdenum* *moly*
    rename *phosphate* *phos*
    rename *palladium* *pld*
    rename *platinum* *plt*
    rename *aluminum* *alum*
    rename *alumina* *almia*
    rename *titanium* *ti*
    rename *tungsten* *tung*
    rename *vanadium* *van*
    rename *scandium* *scan*
    rename *graphite* *graph*
    rename *tantalum* *tant*
    /* rename *magnesium* *mag* */
    rename *manganese* *mang*

    rename *comments* *comm*


    * 5. ESG / SCORES
    * ----------------------------------------------------------------------
    rename *technological* *tech*
    rename *environmental* *env*
    rename *operational* *ops*


    * ------------------------------------------------------------------
    * 1. SPECIFIC LONG VARS (Fixing the known outliers first)
    * ------------------------------------------------------------------
    capture rename date_most_recent_drill_results drill_date_recent
    capture rename transport_method_coal_details trans_meth_coal_det
    capture rename mill_capacity_cubic_m_per_dy_ore mill_cap_m3_day
    capture rename mill_capacity_cubic_m_per_yr_ore mill_cap_m3_yr
    capture rename mining_process_cost_cubic_m_ore mine_cost_m3

    * ------------------------------------------------------------------
    * 2. SHORTEN "ACTIVITIES" (The ends of the variable names)
    * We do this BEFORE shortening prefixes to ensure we catch the long phrases.
    * ------------------------------------------------------------------
    rename *blasting_and_explosives* *blast*
    rename *blasting_and_explosi* *blast*
    rename *engineering_procurement* *epcm*
    rename *engineering_procurem* *epcm*
    rename *infrastructure_and* *infra*
    rename *independent_project* *ind_proj*
    rename *prefeasibility_study* *pfs*
    rename *feasibility_study* *fs*
    rename *scoping_study* *scop*
    rename *quality_assurance* *qaqc*
    rename *tailings_and_waste* *tail*
    rename *remote_location* *rem_loc*
    rename *resources_estimation* *res_est*
    rename *grade_control* *gr_ctrl*
    rename *mine_design* *mine_des*
    rename *processing_design* *proc_des*
    rename *expansion_assessment* *exp_ass*
    rename *contract_mining* *cont_min*
    rename *contract_processing* *cont_proc*
    rename *data_management* *data_mgr*
    rename *social_impact* *social*
    rename *transportation* *trans*
    rename *metallurgical* *met*
    rename *geotechnical* *geotech*
    rename *hydrological* *hydro*
    /* rename *environmental* *env* */
    rename *remediation* *remed*
    rename *optimization* *opt*
    rename *development* *dev*
    rename *exploration* *expl*
    rename *geophysics* *geophys*

    * ------------------------------------------------------------------
    * 3. SHORTEN PREFIXES
    * ------------------------------------------------------------------

    * Contractor -> contr
    rename contractor_* contr_*

    * Operator -> op
    rename operator_* op_*

    * Owner -> own
    /* rename owner_* own_* */
    
    * Royalty Holder -> roy_hldr
    rename royalty_holder_* roy_hldr_*

    * "Begin Year/Month" -> beg_yr / beg_mo
    rename begin_yr_* beg_yr_*
    rename begin_mo_* beg_mo_*

    * "Project End" -> pend
    rename proj_end_* pend_*
    rename end_yr_* end_yr_* // "end_yr" is short enough, but "proj_end" is long

    * "Cash and Equivalents" -> cash_eq
    rename cash_and_equiv_* cash_eq_*

    * "App 5B Net Increase" -> app5b_inc
    rename app5b_net_increase_* app5b_inc_*

    * "Investor Relations" -> inv_rel
    rename *investor_relations* *inv_rel*

    * "Chairman of Board" -> chair
    rename *chairman_of_board* *chair*

    * "Trading Symbol Exchange" -> sym_exch
    rename *trading_symbol_exchange* *sym_exch*
    rename *trading_symbol_ex* *sym_exch*

    * "Debt to Total Cap" -> debt_cap
    rename *total_debt_to_total_cap* *debt_cap*

    * "Price to Earn" -> pe_ratio
    rename *price_to_earn* *pe_ratio*
    rename *price_earn* *pe_ratio*

    * "Planning" -> "Plan"
    rename *planning* *plan*
    rename *planni* *plan*
    rename *plann* *plan*

    * ------------------------------------------------------------------
    * 4. FINAL VERIFICATION LOOP
    * ------------------------------------------------------------------

    /* use "$output_property_level/property_level_crosssection_data_renamed.dta", clear */
    
    di " "
    di "--- CHECKING FOR REMAINING LONG VARIABLES ---"
    local count = 0
    foreach v of varlist * {
        if length("`v'") > 27 {
            display as error "WARNING: `v' is still " length("`v'") " chars long."
            local count = `count' + 1
        }
    }

    if `count' == 0 {
        di as txt "SUCCESS! All variables are now safe for reshaping."
    }
    else {
        di as error "There are still `count' long variables. Check the list above."
    }

    save "$output_property_level/property_level_crosssection_data_renamed.dta", replace
end