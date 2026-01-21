**** construct company level cross-section data ****
**** notes ****
// company_id (owner) | property_id | property_name ...
**1.Steps**
* first generate company IDs at the property level and create map between property ID/name and company ID
* merge map file to each cross-section data (not at property level) that includes property ID/name
* collapse each cross-section data to company level using the map file

**2.Company identifiers in raw data**
* operator_snl_instn_key
* owner_snl_instn_key (Current ownership details)

**** environment ****
clear all
set more off
set maxvar 32767

* directories
global dir_raw "../../../data/raw"
global dir_temp "../../../data/temp"
global dir_cleaned "../../../data/raw_cleaned/S&P_cleaned"

* inputs
global input_property_level "$dir_cleaned/property_level"

* outputs
global output_company_level "$dir_cleaned/company_level"

* intermediates
/* global temp_property_level "$dir_temp/property_level"
global temp_company_level "$dir_temp/company_level"
global temp_reserves "$dir_temp/temp_reserves_resources" */

************************************************************************

program main

end

program generate_company_id
    use "$input_property_level/property_level_crosssection_data.dta", clear
    keep operator_snl_instn_key
    duplicates drop
    drop if operator_snl_instn_key == "NA" | operator_snl_instn_key == ""
    /* gen company_id = operator_snl_instn_key
    destring company_id, replace  // this shows company_id has no missing values */
    rename operator_snl_instn_key company_id
    save "$dir_temp/company_id_list.dta", replace
end

/* program company_property_map
    use "$input_property_level/property_level_crosssection_data.dta", clear
    keep prop_name prop_id owner_snl_instn_key_1-owner_snl_instn_key_8

    foreach v of varlist owner_snl_instn_key_* {
        replace `v' = "" if `v' == "NA"
        destring `v', replace
    }
    reshape long owner_snl_instn_key_, i(prop_name prop_id) j(owner_slot)
    drop if missing(owner_snl_instn_key_)
    rename owner_snl_instn_key_ company_id
    tostring company_id, replace
    save "$dir_temp/company_property_map.dta", replace
end */


program owner_company_reshape
    use "$input_property_level/property_level_crosssection_data.dta", clear
    *------------------------------------------------------------
    * 0) Make a unique id for each wide row
    *------------------------------------------------------------
    egen prop_row = group(prop_id prop_name), label
    isid prop_row

    *------------------------------------------------------------
    * 1a) Collect ALL variables whose *labels* end in "(Owner #)"
    *------------------------------------------------------------
    * rename pattern offender
    forvalues i = 1/8 {
        rename owner_price_to_earn_after_extra`i' owner_price_to_earnings_extra_`i'
    }

    local owner_vars
    foreach v of varlist _all {
        local lab : variable label `v'
        if regexm("`lab'", "\((Owner|OWNER|owner) [0-9]+\)$") {
            local owner_vars `owner_vars' `v'
        }
    }
    di "Owner-slot variables found: `owner_vars'"

    /* local offenders
    foreach v of local owner_vars {
        if regexm("`v'","[0-9]+$") & !regexm("`v'","_[0-9]+$") {
            local offenders `offenders' `v'
        }
    }
    di "Offenders (end in digit but not _digit): `offenders'" */

    *------------------------------------------------------------
    * 1b) Add owner-slot variables that follow the name pattern *_1..*_8
    *     but do NOT have "(Owner #)" in the label (e.g., owner_location_1)
    *------------------------------------------------------------
    ds owner_*_*
    local candidates `r(varlist)'

    foreach v of local candidates {
        * must end with _#
        if !regexm("`v'", "_[0-9]+$") continue

        * already included? skip
        if strpos(" `owner_vars' ", " `v' ") continue

        * optional: exclude known non-slot fields that contain "owners" (counts/lists)
        if inlist("`v'", "num_royalty_owners", "owner_list") continue

        * add it
        local owner_vars `owner_vars' `v'
    }

    di "Owner-slot variables found (labels + name-pattern): `owner_vars'"


    *------------------------------------------------------------
    * 2) Convert that variable list into a UNIQUE stub list
    *    (strip trailing _1, _2, ... from variable names)
    *------------------------------------------------------------
    local stubs
    foreach v of local owner_vars {
        local stub = regexr("`v'", "_[0-9]+$", "")
        if !strpos(" `stubs' ", " `stub' ") local stubs `stubs' `stub'
    }
    di "Stubs to reshape: `stubs'"

    local stubs_u
    foreach s of local stubs {
        local stubs_u `stubs_u' `s'_
    }
    di "Stubs to reshape (underscore style): `stubs_u'"

    *------------------------------------------------------------
    * 3) Reshape wide -> long for ALL those stubs
    *------------------------------------------------------------
    reshape long `stubs_u', i(prop_row) j(owner_slot)

    save "$dir_temp/property_crosssection_reshaped.dta", replace

    *------------------------------------------------------------
    * 4) Drop empty owner slots
    *------------------------------------------------------------
    drop if owner_snl_instn_key_ == "NA" | owner_snl_instn_key_ == ""
    rename owner_snl_instn_key_ company_id  
    order company_id prop_row prop_id prop_name owner_slot

    save "$dir_temp/owner_company_level_crosssection.dta", replace

    *------------------------------------------------------------
    * 5) Drop royalty owner variables (not needed for owner company data)
    *------------------------------------------------------------
    drop *royalty*

    save "$dir_temp/owner_company_level_crosssection.dta", replace
end



program royalty_company_reshape
    use "$input_property_level/property_level_crosssection_data.dta", clear
    *------------------------------------------------------------
    * 0) Make a unique id for each wide row
    *------------------------------------------------------------
    egen prop_row = group(prop_id prop_name), label
    isid prop_row

    *------------------------------------------------------------
    * 1) Collect ALL variables whose *labels* end in "(Royalty #)"
    *------------------------------------------------------------

    local royalty_vars
    foreach v of varlist _all {
        local lab : variable label `v'
        if regexm("`lab'", "\((Royalty|ROYALTY|royalty) [0-11]+\)$") {
            local royalty_vars `royalty_vars' `v'
        }
    }
    di "Royalty-slot variables found: `royalty_vars'"

    *------------------------------------------------------------
    * 2) Convert that variable list into a UNIQUE stub list
    *    (strip trailing _1, _2, ... from variable names)
    *------------------------------------------------------------
    local stubs
    foreach v of local royalty_vars {
        local stub = regexr("`v'", "_[0-11]+$", "")
        if !strpos(" `stubs' ", " `stub' ") local stubs `stubs' `stub'
    }
    di "Stubs to reshape: `stubs'"

    local stubs_u
    foreach s of local stubs {
        local stubs_u `stubs_u' `s'_
    }
    di "Stubs to reshape (underscore style): `stubs_u'"

    *------------------------------------------------------------
    * 3) Reshape wide -> long for ALL those stubs
    *------------------------------------------------------------
    reshape long `stubs_u', i(prop_row) j(royalty_slot)

    save "$dir_temp/property_crosssection_reshaped2.dta", replace

    *------------------------------------------------------------
    * 4) Drop empty royalty slots
    *------------------------------------------------------------
    drop if royalty_snl_instn_key_ == "NA" | royalty_snl_instn_key_ == ""
    rename royalty_snl_instn_key_ company_id  
    order company_id prop_row prop_id prop_name royalty_slot

    *------------------------------------------------------------
    * 5) Drop owner variables (not needed for royalty company data)
    *------------------------------------------------------------
    drop *owner* current_controlling_own_pct_1-current_controlling_own_pct_8

    save "$dir_temp/royalty_company_level_crosssection.dta", replace

    *------------------------------------------------------------
    * 6) Reshape wide to company level
    *------------------------------------------------------------
    /* order company_id royalty_slot */
    clear all
    use "$dir_temp/royalty_company_level_crosssection.dta", replace
    drop prop_row
    rename royalty_slot slot
    rename operator_investor_relations_bio operator_investor_relat_bio
    rename operator_chairman_of_board_bio operator_chair_of_board_bio

    * Identify the varlist you plan to reshape
    ds prop_id-date_most_recent_drill_results
    local reshape_vars `r(varlist)'

    * Find strL variables inside that range
    ds prop_id-date_most_recent_drill_results, has(type strL)
    local strL_vars `r(varlist)'

    * For each strL var: encode to numeric id with value label
    *    - creates <var>_id
    *    - drops original strL var
    *    - updates reshape varlist to use the *_id instead

    foreach v of local strL_vars {
        di as txt "Converting strL variable to numeric id: `v' -> `v'_id"

        * Create numeric id with value labels preserving the original text
        egen `v'_id = group(`v'), label

        * Drop the unsupported strL variable
        drop `v'

        * Replace `v' with `v'_id in the reshape varlist
        local reshape_vars : subinstr local reshape_vars "`v'" "`v'_id", word all
    }

    bysort company_id (prop_id): gen prop_num = _n // indexing properties for each company
    order company_id prop_num
    label var prop_num "Royalty-owning property index for each company"
    label var slot "Royalty owner slot number of the property"

    /* greshape wide `reshape_vars', i(company_id) j(prop_num) */

    tempfile base out part
    save `base', replace

    local step 250
    local n : word count `reshape_vars'
    local first 1

    forvalues a = 1(`step')`n' {
        
        local b = `a' + `step' - 1
        if `b' > `n' local b = `n'

        * --- FIX START: Create the chunk using a loop ---
        local chunk ""
        forvalues k = `a'/`b' {
            local var : word `k' of `reshape_vars'
            local chunk `chunk' `var'
        }
        * --- FIX END ---

        use `base', clear
        
        * Ensure you only keep the ID, the J variable, and the vars to reshape
        * (Otherwise greshape/reshape might error or keep unwanted vars)
        keep company_id prop_num `chunk'
        
        greshape wide `chunk', i(company_id) j(prop_num)

        save `part', replace

        * Logic to initialize or merge
        if `first' == 1 {
            save `out', replace
            local first 0
        }
        else {
            use `out', clear
            merge 1:1 company_id using `part', nogen
            save `out', replace
        }
    }

    use `out', clear


    save "$dir_temp/royalty_company_level_crosssection.dta", replace

    /* reshape wide `reshape_vars', i(company_id) j(royalty_slot) */

    * Example: decode one of the created ids after reshape
    * decode longtext_id1, gen(longtext1)

end



program merge_property_details
    use "$dir_temp/company_property_map.dta", clear
    merge 1:m prop_id using "$input_property_level/property_level_crosssection_data.dta", nogenerate
    drop if missing(prop_name)
    save "$output_company_level/property_details_company_level.dta", replace
end
