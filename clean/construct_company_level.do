**** construct company level cross-section data ****
**** notes ****
* run "ssc install gtools" if not installed
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
global dir_output "../../../output"

* inputs
global input_property_level "$dir_cleaned/property_level"

* outputs
global output_company_level "$dir_cleaned/company_level"
global dir_datadist "$dir_output/fig/data_dist"

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


program owner_company_reshape
    use "$input_property_level/property_level_crosssection_data_renamed.dta", clear
    *------------------------------------------------------------
    * 0) Make a unique id for each wide row
    *------------------------------------------------------------
    egen prop_row = group(prop_id prop_name), label
    isid prop_row

    *------------------------------------------------------------
    * 1a) Collect ALL variables whose *labels* end in "(Owner #)"
    *------------------------------------------------------------

    local owner_vars
    foreach v of varlist _all {
        local lab : variable label `v'
        if regexm("`lab'", "\((Owner|OWNER|owner) [0-9]+\)$") {
            local owner_vars `owner_vars' `v'
        }
    }
    di "Owner-slot variables found: `owner_vars'"

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

    *------------------------------------------------------------
    * 5) Drop owner variables related to royalty (not needed for royalty company data)
    *------------------------------------------------------------
    drop *roy*

    *------------------------------------------------------------
    * 6) Reshape wide to company level
    *------------------------------------------------------------
    drop prop_row
    rename owner_slot slot

    * Find strL variables inside that range
    /* ds prop_id-owner_pe_ratio_after_extra8_, has(type strL)
    local strL_vars `r(varlist)'

    foreach v of local strL_vars {
        * Drop the unsupported strL variable
        drop `v'
    }

    * Identify the varlist you plan to reshape
    ds prop_id-owner_pe_ratio_after_extra8_
    local reshape_vars `r(varlist)' */

    bysort company_id (prop_id): gen prop_num = _n // indexing properties for each company
    order company_id prop_num
    label var prop_num "Owning property index for each company"
    label var slot "Owning slot number of the property"

    drop *owner_pe_ratio_after_extra*

    save "$dir_temp/owner_company_level_crosssection.dta", replace

/* 
    ds prop_id-owner_pe_ratio_after_extra8_
    local reshape_vars `r(varlist)'
    greshape wide `reshape_vars', i(company_id) j(prop_num) */

    use "$dir_temp/owner_company_level_crosssection.dta"

    * 1. Calculate the total properties per company
    * We do this BEFORE the loop so it's calculated on the full dataset

    levelsof snl_global_region, local(regions)

    foreach r of local regions {
        preserve
        keep if snl_global_region == "`r'"

        bysort company_id: gen prop_count = _N
        
        * Keep only the target companies (>1000)
        keep if prop_count > 100
        
        * Deduplicate so we have one row per company
        duplicates drop company_id, force
        
        count
        * IF there are very few companies (e.g., less than 20), make a named bar chart
        if r(N) > 0 & r(N) < 20 {
            * Note: Replace 'company_name' with your actual variable name for the company name
            graph hbar (sum) prop_count, over(company_id, sort(1) descending label(labsize(small))) ///
                ytitle("Number of Properties") ///
                title("Top Portfolios: `r'") ///
                blabel(bar) ///  <-- This adds the exact number at the end of the bar
                saving("$dir_datadist/bar_propnum_large_`r'.gph", replace)
                
            graph export "$dir_datadist/bar_propnum_large_`r'.png", replace
        }
        * ELSE if there are many companies, stick with the histogram
        else if r(N) >= 20 {
            histogram prop_count, frequency start(1000) ///
                xtitle("Number of Properties") ///
                title("Distribution: `r'") ///
                saving("$dir_datadist/hist_propnum_large_`r'.gph", replace)
            graph export "$dir_datadist/hist_propnum_large_`r'.png", replace
        }
        
        restore
    }



    foreach r of local regions {
        preserve
        
            * 2. Filter for the specific region
            keep if snl_global_region == "`r'"
            
            * 3. Deduplicate: Keep only one row per company
            * Now the dataset represents "Companies", not "Properties"
            duplicates drop company_id, force
            
            * 4. Create the Plot
            * 'discrete' tells Stata the x-axis values are integers (1, 2, 3...)
            * 'freq' makes the y-axis the count of companies
            histogram prop_count, discrete freq ///
                xtitle("Number of Properties Owned") ///
                ytitle("Count of Companies") ///
                title("Portfolio Size Distribution: `r'") ///
                gap(10) ///
                saving("$dir_datadist/hist_propnum_`r'.gph", replace)
                
            graph export "$dir_datadist/hist_propnum_`r'.png", replace
            
        restore
    }


    * 1. Calculate total properties per company (on full dataset)
    /* bysort company_id: gen prop_count = _N */

    levelsof snl_global_region, local(regions)

    foreach r of local regions {
        preserve
            
            * 2. Filter for Region AND High-Volume Portfolios (>1000)
            keep if snl_global_region == "`r'"
            keep if prop_count > 1000
            
            * 3. Check if there is data to plot
            * (Prevents the script from crashing if a region has no companies > 1000)
            count
            if r(N) > 0 {
            
                * 4. Deduplicate: One row per Company
                duplicates drop company_id, force
                
                * 5. Create the Histogram
                * We remove 'discrete' because values like 2000, 3000, 4000 are continuous ranges
                * frequency: Y-axis is number of companies
                * start(2000): Anchors the x-axis at your cutoff
                histogram prop_count, frequency ///
                    start(1000) ///
                    xtitle("Number of Properties (>1000)") ///
                    ytitle("Count of Companies") ///
                    title("Large Portfolios Distribution: `r'") ///
                    saving("$dir_datadist/hist_propnum_large_`r'.gph", replace)
                    
                graph export "$dir_datadist/hist_propnum_large_`r'.png", replace
            }
            else {
                display "Skipping `r': No companies with >1000 properties."
            }
            
        restore
    }



*------------------------------------------------------------
    * 1. Identify all unique regions and store them in a local macro called 'regions'
    levelsof snl_global_region, local(regions)

    foreach r of local regions {
        /* historgram prop_num if snl_global_region == "`r'", discrete frequency title("Property Count Distribution in `r' Region")  */
        graph bar count, over(prop_num) if snl_global_region == "`r'"///
        saving("$dir_datadist/hist_propnum_`r'.gph", replace)
        graph export "$dir_datadist/hist_propnum_`r'.png", replace        
    }

    * 2. Loop through each specific region
    foreach r of local regions {
        
        * Preserve the current state (the full dataset)
        preserve
        
        * Keep only the observations for the current region in the loop
        * Note: If snl_global_region is numeric, remove the quotes around `r'
        keep if snl_global_region == "`r'"
        
        ds prop_id-owner_pe_ratio_after_extra8_
        local reshape_vars `r(varlist)'
        
        * Note: Ensure gtools is installed (ssc install gtools)
        greshape wide `reshape_vars', i(company_id) j(prop_num)
        
        * 3. Save the data with a unique filename for this region
        * We use simple quotes for the filename to handle potential spaces in region names
        save "$output_company_level/owner_company_crosssection_`r'.dta", replace
        
        * Restore the full dataset to process the next region
        restore
    }

    ** Africa
        preserve
        
        keep if snl_global_region == "Africa"
        
        ds prop_id-owner_pe_ratio_after_extra8_
        local reshape_vars `r(varlist)'
        
        greshape wide `reshape_vars', i(company_id) j(prop_num)
        
        save "$output_company_level/owner_company_crosssection_Africa.dta", replace
        
        * Restore the full dataset to process the next region
        restore

    ** Latin America and Caribbean
        preserve
        
        keep if snl_global_region == "Latin America and Caribbean"
        
        ds prop_id-owner_pe_ratio_after_extra8_
        local reshape_vars `r(varlist)'
        
        greshape wide `reshape_vars', i(company_id) j(prop_num)
        
        save "$output_company_level/owner_company_crosssection_Latin.dta", replace
        
        * Restore the full dataset to process the next region
        restore

    * --- CHUNKING LOOP ---
        tempfile base out part
        save `base', replace

        local step 50
        local n : word count `reshape_vars'
        local first 1

        forvalues a = 1(`step')`n' {
            
            local b = `a' + `step' - 1
            if `b' > `n' local b = `n'

            * Build the chunk based on the CLEAN reshape_vars list
            local chunk ""
            forvalues k = `a'/`b' {
                local var : word `k' of `reshape_vars'
                local chunk `chunk' `var'
            }

            use `base', clear
            
            * Keep only what is needed for this specific reshape
            keep company_id prop_num `chunk'
            
            * Reshape
            greshape wide `chunk', i(company_id) j(prop_num)

            save `part', replace

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
    

    save "$output_company_level/owner_company_crosssection.dta", replace
end



program royalty_company_reshape
    use "$input_property_level/property_level_crosssection_data_renamed.dta", clear
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

    //save "$dir_temp/property_crosssection_reshaped2.dta", replace

    *------------------------------------------------------------
    * 4) Drop empty royalty slots
    *------------------------------------------------------------
    drop if royalty_snl_instn_key_ == "NA" | royalty_snl_instn_key_ == ""
    rename royalty_snl_instn_key_ company_id  
    order company_id prop_row prop_id prop_name royalty_slot

    *------------------------------------------------------------
    * 5) Drop owner variables (not needed for royalty company data)
    *------------------------------------------------------------
    drop *owner* cur_controlling_own_pct_1-cur_controlling_own_pct_8

    save "$dir_temp/royalty_company_level_crosssection.dta", replace
    use "$dir_temp/royalty_company_level_crosssection.dta", clear

    *------------------------------------------------------------
    * 6) Reshape wide to company level
    *------------------------------------------------------------
    /* order company_id royalty_slot */
    /* clear all
    use "$dir_temp/royalty_company_level_crosssection.dta", replace */
    drop prop_row
    rename royalty_slot slot
    /* rename operator_investor_relations_bio operator_investor_relat_bio
    rename operator_chairman_of_board_bio operator_chair_of_board_bio */

    * Find strL variables inside that range
    ds prop_id-drill_date_recent, has(type strL)
    local strL_vars `r(varlist)'

    * For each strL var: encode to numeric id with value label
    *    - creates <var>_id
    *    - drops original strL var
    *    - updates reshape varlist to use the *_id instead

    foreach v of local strL_vars {
        //di as txt "Converting strL variable to numeric id: `v' -> `v'_id"

        * Create numeric id with value labels preserving the original text
        //egen `v'_id = group(`v'), label

        * Drop the unsupported strL variable
        drop `v'

        * Replace `v' with `v'_id in the reshape varlist
        //local reshape_vars : subinstr local reshape_vars "`v'" "`v'_id", word all
    }

    * Identify the varlist you plan to reshape
    ds prop_id-drill_date_recent 
    local reshape_vars `r(varlist)'

    bysort company_id (prop_id): gen prop_num = _n // indexing properties for each company
    order company_id prop_num
    label var prop_num "Royalty-owning property index for each company"
    label var slot "Royalty owner slot number of the property"

    ds prop_id-drill_date_recent
    local reshape_vars `r(varlist)'

    tempfile base
    save `base', replace

    * Define the output directory (Ensure this global is defined before running)
    * global dir_temp "C:/path/to/your/temp/folder" 

    local step 15
    local n : word count `reshape_vars'

    forvalues a = 1(`step')`n' {
        
        local b = `a' + `step' - 1
        if `b' > `n' local b = `n'

        * Create the chunk of variables
        local chunk ""
        forvalues k = `a'/`b' {
            local var : word `k' of `reshape_vars'
            local chunk `chunk' `var'
        }

        use `base', clear
        
        * Keep ID, J variable, and the specific chunk vars
        keep company_id prop_num `chunk'
        
        * Perform the reshape
        greshape wide `chunk', i(company_id) j(prop_num)

        * Save the individual chunk to the directory
        * The filename includes `a` to ensure uniqueness (e.g., chunk_1.dta, chunk_16.dta)
        save "$output_company_level/royalty_company_crosssection_chunk_`a'.dta", replace
    }
    /* greshape wide `reshape_vars', i(company_id) j(prop_num)
    save "$dir_temp/royalty_company_level_crosssection.dta", replace */



    *----------------

    tempfile base out part
    save `base', replace

    local step 15
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

end

*------------------------------------------------------------
    clear all
    use "$dir_temp/royalty_company_level_crosssection.dta", replace

    drop prop_row
    rename royalty_slot slot
    rename operator_investor_relations_bio operator_investor_relat_bio
    rename operator_chairman_of_board_bio operator_chair_of_board_bio

    * 1. Get the initial list of all variables in the range
    ds prop_id-date_most_recent_drill_results
    local all_original_vars `r(varlist)'

    * 2. Identify strL variables to encode
    ds prop_id-date_most_recent_drill_results, has(type strL)
    local strL_vars `r(varlist)'

    * 3. Create a clean "final_vars" list
    local reshape_vars ""

    foreach v of local all_original_vars {
        * Check if this variable is in our strL list
        local is_strL : list v in strL_vars
        
        if `is_strL' {
            di as txt "Converting strL: `v' -> `v'_id"
            egen `v'_id = group(`v'), label
            drop `v'
            
            * Append the NEW name to our final list
            local reshape_vars `reshape_vars' `v'_id
        }
        else {
            * Append the ORIGINAL name to our final list
            local reshape_vars `reshape_vars' `v'
        }
    }

    * Setup the indexing for reshape
    bysort company_id (prop_id): gen prop_num = _n 
    order company_id prop_num

    * --- CHUNKING LOOP ---
    tempfile base out part
    save `base', replace

    local step 100
    local n : word count `reshape_vars'
    local first 1

    forvalues a = 1(`step')`n' {
        
        local b = `a' + `step' - 1
        if `b' > `n' local b = `n'

        * Build the chunk based on the CLEAN reshape_vars list
        local chunk ""
        forvalues k = `a'/`b' {
            local var : word `k' of `reshape_vars'
            local chunk `chunk' `var'
        }

        use `base', clear
        
        * Keep only what is needed for this specific reshape
        keep company_id prop_num `chunk'
        
        * Reshape
        greshape wide `chunk', i(company_id) j(prop_num)

        save `part', replace

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

*------------------------------------------------------------
    clear all
    use "$dir_temp/royalty_company_level_crosssection.dta", replace

    * 1. Basic Cleanup
    drop prop_row
    rename royalty_slot slot

    * --- FIX 1: PREVENT NAME OVERFLOW ---
    * Rename the specific long variable that is causing the crash
    rename operator_investor_relations_bio operator_investor_relat_bio
    rename operator_chairman_of_board_bio operator_chair_of_board_bio
    rename operator_investor_relations_name operator_investor_relat_name
    rename date_most_recent_drill_results drill_date_recent 
    rename contractor_verified_contract_min contractor_verified_con_min
    rename contractor_verified_contract_pro contractor_verified_con_pro
    forvalues i = 1/8 {
        rename cash_and_equiv_most_recent_yr_`i' cash_and_equiv_recent_yr_`i'
        rename cash_and_equiv_most_recent_qtr_`i' cash_and_equiv_recent_qtr_`i'
        rename  app5b_net_increase_cash_qtr_`i' app5b_increase_cash_qtr_`i'
    }

    * (Optional) Safety loop: Rename ANY variable > 28 chars to ensure suffix space
    foreach v of varlist * {
        local len = length("`v'")
        if `len' > 28 {
            local newname = substr("`v'", 1, 28)
            rename `v' `newname'
            di as txt "Renamed long variable: `v' -> `newname'"
        }
    }

    * 2. Identify variables to reshape
    * Note: We use the NEW name 'drill_date_recent' here
    ds prop_id-drill_date_recent
    local all_original_vars `r(varlist)'

    * 3. Identify ALL string variables (str# AND strL) in that range
    ds prop_id-drill_date_recent, has(type string)
    local string_vars `r(varlist)'

    local reshape_vars ""

    foreach v of local all_original_vars {
        
        * Check if this variable is in our string list
        local is_string : list v in string_vars
        
        if `is_string' {
            di as txt "Encoding string: `v' -> `v'_id"
            
            * Use 'capture' in case the variable is already numeric (safety)
            capture confirm numeric variable `v'
            if _rc {
                egen `v'_id = group(`v'), label missing
                drop `v'
                local reshape_vars `reshape_vars' `v'_id
            }
            else {
                local reshape_vars `reshape_vars' `v'
            }
        }
        else {
            local reshape_vars `reshape_vars' `v'
        }
    }

    * Setup Indexing
    bysort company_id (prop_id): gen prop_num = _n 
    order company_id prop_num

    * --- CHUNKING LOOP ---
    tempfile base out part
    save `base', replace

    local step 100
    local n : word count `reshape_vars'
    local first 1

    forvalues a = 1(`step')`n' {
        
        local b = `a' + `step' - 1
        if `b' > `n' local b = `n'

        local chunk ""
        forvalues k = `a'/`b' {
            local var : word `k' of `reshape_vars'
            local chunk `chunk' `var'
        }

        use `base', clear
        
        * Keep only strictly necessary variables to prevent conflicts
        keep company_id prop_num `chunk'
        
        * Use standard 'reshape' if greshape continues to fail (it's slower but safer for debugging)
        * If this works, you can switch back to 'greshape'
        greshape wide `chunk', i(company_id) j(prop_num)

        save `part', replace

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

*------------------------------------------------------------


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
