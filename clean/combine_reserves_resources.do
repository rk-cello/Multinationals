**** construct property level cross-section data ****
**** notes ****
* some string variables should be converted to numeric
* this script appends each property details data
* historical data is year data, but should be separately treated from time-invariant info
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
global temp_reserves "$dir_temp/temp_reserves_resources"


************************************************************************
cd "$input_metals_mining/properties_reserves_resources"

* roadmap
program main
    combine_RR6

end


program combine_RR6 // time invariant data for reserves and resources
    clear all
    set more off

    local files RR_6_in-situ_evalutation_prices_Africa.xls RR_6_in-situ_evalutation_prices_AsiaPacific.xls RR_6_in-situ_evalutation_prices_EuropeMiddleEast.xls RR_6_in-situ_evalutation_prices_LatinAmerica.xls RR_6_in-situ_evalutation_prices_USCanada.xls

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
