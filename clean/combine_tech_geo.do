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
global input_tech_geo "$input_metals_mining/properties_technical_geology"

* outputs
global output_property_level "$dir_cleaned/property_level"
global output_company_level "$dir_cleaned/company_level"

* intermediates
global temp_property_level "$dir_temp/property_level"
global temp_company_level "$dir_temp/company_level"
global temp_tech_geo "$dir_temp/temp_tech_geo"


************************************************************************

* roadmap
program main
    combine_tech_geo

end

program combine_tech_geo
    clear all
    set more off

    local files "$input_tech_geo/technical_geology_Africa.xls" "$input_tech_geo/technical_geology_AsiaPacific.xls" "$input_tech_geo/technical_geology_EuropeMiddleEast.xls" "$input_tech_geo/technical_geology_LatinAmerica.xls" "$input_tech_geo/technical_geology_USCanada.xls"
    tempfile all
    local first = 1

    foreach f of local files {
        import excel using "`f'", cellrange(A4:Z4) firstrow clear
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
            tostring h, replace
            append using `base'
            save `base', replace
        }
    }

   * Rename zone_name ~ f to zone_name_z1 ~ zone_name_z4
    rename zone_name zone_name_z1
    rename d zone_name_z2
    rename e zone_name_z3
    rename f zone_name_z4

    * Update labels
    label variable zone_name_z1 "zone_name (Zone 1)"
    label variable zone_name_z2 "zone_name (Zone 2)"
    label variable zone_name_z3 "zone_name (Zone 3)"
    label variable zone_name_z4 "zone_name (Zone 4)"

    * Rename geology_region ~ n to geology_region_z1 ~ geology_region_z4
    rename geology_region geology_region_z1
    rename h geology_region_z2
    rename i geology_region_z3
    rename j geology_region_z4

    * Update labels
    label variable geology_region_z1 "geology_region (Zone 1)"
    label variable geology_region_z2 "geology_region (Zone 2)"
    label variable geology_region_z3 "geology_region (Zone 3)"
    label variable geology_region_z4 "geology_region (Zone 4)"

    * Rename geology_comments ~ r to geology_comments_z1 ~ geology_comments_z4
    rename geology_comments geology_comments_z1
    rename l geology_comments_z2
    rename m geology_comments_z3
    rename n geology_comments_z4

    * Update labels
    label variable geology_comments_z1 "geology_comments (Zone 1)"
    label variable geology_comments_z2 "geology_comments (Zone 2)"
    label variable geology_comments_z3 "geology_comments (Zone 3)"
    label variable geology_comments_z4 "geology_comments (Zone 4)"

    * Rename avg_depth_geologic_dep ~ r to avg_depth_geologic_dep_z1 ~ avg_depth_geologic_dep_z4
    rename avg_depth_geologic_dep avg_depth_geologic_dep_z1
    rename p avg_depth_geologic_dep_z2
    rename q avg_depth_geologic_dep_z3
    rename r avg_depth_geologic_dep_z4

    * Update labels
    label variable avg_depth_geologic_dep_z1 "avg_depth_geologic_dep (Zone 1)"
    label variable avg_depth_geologic_dep_z2 "avg_depth_geologic_dep (Zone 2)"
    label variable avg_depth_geologic_dep_z3 "avg_depth_geologic_dep (Zone 3)"
    label variable avg_depth_geologic_dep_z4 "avg_depth_geologic_dep (Zone 4)"

    * Rename geologic_ore_body_type ~ v to geologic_ore_body_type_z1 ~ geologic_ore_body_type_z4
    rename geologic_ore_body_type geologic_ore_body_type_z1
    rename t geologic_ore_body_type_z2
    rename u geologic_ore_body_type_z3
    rename v geologic_ore_body_type_z4

    * Update labels
    label variable geologic_ore_body_type_z1 "geologic_ore_body_type (Zone 1)"
    label variable geologic_ore_body_type_z2 "geologic_ore_body_type (Zone 2)"
    label variable geologic_ore_body_type_z3 "geologic_ore_body_type (Zone 3)"
    label variable geologic_ore_body_type_z4 "geologic_ore_body_type (Zone 4)"

    * Rename ore_minerals ~ y to ore_minerals_z1 ~ ore_minerals_z4
    rename ore_minerals ore_minerals_z1
    rename x ore_minerals_z2
    rename y ore_minerals_z3
    rename z ore_minerals_z4

    * Update labels
    label variable ore_minerals_z1 "ore_minerals (Zone 1)"
    label variable ore_minerals_z2 "ore_minerals (Zone 2)"
    label variable ore_minerals_z3 "ore_minerals (Zone 3)"
    label variable ore_minerals_z4 "ore_minerals (Zone 4)"

    save "$temp_tech_geo/tech_geo.dta", replace

end