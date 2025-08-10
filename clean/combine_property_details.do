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
global input_property_details "$input_metals_mining/properties_property_details"

* outputs
global output_property_level "$dir_cleaned/property_level"
global output_company_level "$dir_cleaned/company_level"

* intermediates
global temp_property_level "$dir_temp/property_level"
global temp_company_level "$dir_temp/company_level"
global temp_prop_details "$dir_temp/temp_prop_details"


************************************************************************


* roadmap
program main
    combine_property_details_1
    combine_property_details_2
    combine_property_details_3_1
    combine_property_details_3_2
    combine_property_details_4
    combine_property_details_5_1
    combine_property_details_5_2
    combine_property_details_5_3
    combine_property_details_5_4
    combine_property_details_5_5
    combine_property_details_5_6
    combine_property_details_5_7
    combine_property_details_5_8
    combine_property_details_5_9
    combine_property_details_5_10
    combine_property_details_5_11
    combine_property_details_5_12
    combine_property_details_5_13
    combine_property_details_5_14
    combine_property_details_5_15
    combine_property_details_6_1
    combine_property_details_6_2
    combine_property_details_7_1
    combine_property_details_7_2
    combine_property_details_7_3
    combine_property_details_8
    combine_property_details_9_1
    combine_property_details_9_2
    combine_property_details_9_3
    combine_property_details_9_4
    combine_property_details_9_5
    combine_property_details_10_1
    combine_property_details_10_2
    combine_property_details_10_3
    combine_property_details_11_1
    combine_property_details_11_2
end

**** property details ****
program combine_property_details_1

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_1_general_info_commodities_location_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  also_known_as
    rename D  primary_commodity
    rename E  dev_stage
    rename F  actv_status
    rename G  minesearch_id
    rename H  intierra_live_ppty_id
    rename I  meg_prop_id
    rename J  yr_source_date
    rename K  mo_source_date
    rename L  mo_yr_source_date
    rename M  mine_type1
    rename N  mine_type2
    rename O  mine_type3
    rename P  subproperty
    rename Q  sub_prop_parent_id
    rename R  sub_prop_parent_name
    rename S  commodities_list
    rename T  commodity1
    rename U  commodity2
    rename V  commodity3
    rename W  commodity4
    rename X  commodity5
    rename Y  commodity6
    rename Z  state_province
    rename AA country_name
    rename AB snl_global_region
    rename AC latitude
    rename AD longitude
    rename AE coordinate_accuracy
    rename AF distance_from

    * label variables
    label var prop_name             "Name of the mine or facility"
    label var prop_id               "Unique key for the project"
    label var also_known_as         "List of property aliases"
    label var primary_commodity     "Main material extracted"
    label var dev_stage             "Development stage of the project"
    label var actv_status           "Current activity status"
    label var minesearch_id         "Unique MineSearch ID"  
    label var intierra_live_ppty_id "The unique identifier for a property in the IntierraLive product"
    label var meg_prop_id           "MEG's new primary key for mining projects"
    label var yr_source_date        "Year of the most recent source information about the project"
    label var mo_source_date        "Month of the most recent source information about the project"
    label var mo_yr_source_date     "Month and year of the most recent source information about the project"
    label var mine_type1            "Mining operation carried out at a mine (Type 1)"
    label var mine_type2            "Mining operation carried out at a mine (Type 2)"
    label var mine_type3            "Mining operation carried out at a mine (Type 3)"
    label var subproperty           "Indicates that the property is a component of another mining property"
    label var sub_prop_parent_id    "Key of the mining project identified as parent of the subproperty"
    label var sub_prop_parent_name  "Name of a claim or group of claims about which mining data is reported"
    label var commodities_list      "A list of the project's current products"
    label var commodity1            "Material extracted during the course of mining operations (Commodity 1)"
    label var commodity2            "Material extracted during the course of mining operations (Commodity 2)"
    label var commodity3            "Material extracted during the course of mining operations (Commodity 3)"
    label var commodity4            "Material extracted during the course of mining operations (Commodity 4)"
    label var commodity5            "Material extracted during the course of mining operations (Commodity 5)"
    label var commodity6            "Material extracted during the course of mining operations (Commodity 6)"
    label var state_province        "Common name of a country division"
    label var country_name          "Common English country name"
    label var snl_global_region     "Name of the global region"
    label var latitude              "Latitude"
    label var longitude             "Longitude"
    label var coordinate_accuracy   "Accuracy of latitude and longitude"
    label var distance_from         "Distance from the nearest town"

    save "$temp_prop_details/property_details_1.dta", replace
end
program combine_property_details_2
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_2_coal_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            keep A-K
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  coal_uses
    rename D  economic_coal_seam_count
    rename E  total_coal_seam_count
    rename F  transport_method_coal_details
    rename G  shipping_port_coal_detail
    rename H  coal_seam
    rename I  coal_rank
    rename J  coal_rank_abbrev
    rename K  coal_group

    * label variables
    label var prop_name                  "Name of the mine or facility"
    label var prop_id                    "Unique key for the project"    
    label var coal_uses                  "Use to which coal produced is put"
    label var economic_coal_seam_count   "Coal seams that are economically feasible to mine"
    label var total_coal_seam_count      "Coal seams occurring at a mine, including those deemed uneconomic"
    label var transport_method_coal_details "Method of transportation for coal"
    label var shipping_port_coal_detail  "Name of a shipping port for coal"
    label var coal_seam                  "Name of the coal seam"
    label var coal_rank                  "Standard classifications of coals by rank, as defined by ASTM D-388"
    label var coal_rank_abbrev           "Abbreviated standard classifications of coals by rank, as defined by ASTM D-388"
    label var coal_group                 "Standard classifications of coals by rank, as defined by ASTM D-388"

    save "$temp_prop_details/property_details_2.dta", replace
end
program combine_property_details_3_1
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_3_operator_1_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  operator_name
    rename D  operator_snl_instn_key
    rename E  operator_common_name
    rename F  operator_company_name_abbrev
    rename G  operator_date_closing_price
    rename H  operator_mkt_cap
    rename I  operator_price_to_ltm_eps
    rename J  operator_tev
    rename K  operator_tev_to_ltm_ebitda
    rename L  operator_total_debt_to_total_cap
    rename M  operator_price_earn_after_extra // as operator_price_to_earn_after_extra is too long for var name
    rename N  operator_tev_to_ebitda
    rename O  operator_period_ended
    rename P  operator_working_capital
    rename Q  operator_total_cap_at_bv
    rename R  operator_total_debt
    rename S  operator_current_liab
    rename T  operator_hq
    rename U  operator_address1
    rename V  operator_address2
    rename W  operator_city_state
    rename X  operator_state
    rename Y  operator_location
    rename Z  operator_frgn_province
    rename AA operator_country
    rename AB operator_zip
    rename AC operator_postal_code
    rename AD operator_global_region

    * label variables
    label var prop_name                     "Name of the mine or facility"
    label var prop_id                       "Unique key for the project"
    label var operator_name                 "Complete name of the institution, as specified in its charter"
    label var operator_snl_instn_key        "S&P's unique key to identify institutions (corporations, partnerships, etc.)"
    label var operator_common_name          "Most recognized company name"
    label var operator_company_name_abbrev  "Shortened version of the company's name"
    label var operator_date_closing_price   "Date (and time, for intraday prices) of the pricing information"
    label var operator_mkt_cap              "Aggregate market capitalization of all common equity issues"
    label var operator_price_to_ltm_eps     "Price as a multiple of last-twelve-months earnings per share"
    label var operator_tev                  "Market capitalization of ongoing operations, less cash and equivalents"
    label var operator_tev_to_ltm_ebitda    "Total enterprise value as a multiple of last-twelve-month EBITDA"
    label var operator_total_debt_to_total_cap "Debt as a percent of total market capitalization"
    label var operator_price_earn_after_extra "Price as a multiple of earnings per share after extraordinary items"
    label var operator_tev_to_ebitda        "Total enterprise value as a multiple of EBITDA"
    label var operator_period_ended         "Ending date of the fiscal period, standardized to the last day of the month"
    label var operator_working_capital      "Current assets net of current liabilities"
    label var operator_total_cap_at_bv      "Book value of all forms of capital (equity, debt, mezzanine items)"
    label var operator_total_debt           "Aggregate unpaid principal balance owed under financial obligations"
    label var operator_current_liab         "Accounts payable and other obligations due within the next 12 months"
    label var operator_hq                   "City where the operator is headquartered"
    label var operator_address1             "First line of the operator's address"
    label var operator_address2             "Second line of the operator's address"
    label var operator_city_state           "City and state or province of the operator"
    label var operator_state                "State postal code, as defined by U.S. FIPS 5-2"
    label var operator_location             "City and state (U.S.) or city and country (non-U.S.) of the operator's HQ"
    label var operator_frgn_province        "Province or major political subdivision for non-U.S. addresses"
    label var operator_country              "Common English name of the operator's country"
    label var operator_zip                  "U.S. Postal Service Zip Code"
    label var operator_postal_code          "Postal code or routing code for non-U.S. addresses"
    label var operator_global_region        "Name of the global region where the operator is located"    
    
    save "$temp_prop_details/property_details_3_1.dta", replace
end
program combine_property_details_3_2
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_3_operator_2_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  operator_phone
    rename D  operator_website
    rename E  operator_ticker
    rename F  operator_exchange
    rename G  operator_trading_symbol_exchange
    rename H  operator_ceo_name
    rename I  operator_ceo_age
    rename J  operator_ceo_biography
    rename K  operator_cfo_name
    rename L  operator_cfo_age
    rename M  operator_cfo_biography
    rename N  operator_coo_name
    rename O  operator_coo_age
    rename P  operator_coo_biography
    rename Q  operator_president_name
    rename R  operator_president_age
    rename S  operator_president_biography
    rename T  operator_investor_relations_name // shortened to fit
    rename U  operator_investor_relations_age // shortened to fit
    rename V  operator_investor_relations_bio // shortened to fit
    rename W  operator_chairman_of_board_name
    rename X  operator_chairman_of_board_age
    rename Y  operator_chairman_of_board_bio // shortened to fit
    rename Z  operator_vp_head_explor_name // shortened to fit
    rename AA operator_vp_head_explor_age // shortened to fit
    rename AB operator_vp_head_explor_bio // shortened to fit
    rename AC operator_chief_geologist_name
    rename AD operator_chief_geologist_age
    rename AE operator_chief_geologist_bio // shortened to fit

    * label variables
    label var prop_name                          "Name of the mine or facility"
    label var prop_id                            "Unique key for the project"
    label var operator_phone                     "Phone number"
    label var operator_website                   "URL of a site found on the world-wide web"
    label var operator_ticker                    "Ticker or trading symbol of this security as used on the issue's primary securities exchange"
    label var operator_exchange                  "Abbreviated name of the securities exchange or OTC Tier"
    label var operator_trading_symbol_exchange   "Ticker and abbreviated name of the securities exchange or OTC Tier"
    label var operator_ceo_name                  "Operator CEO's Name"
    label var operator_ceo_age                   "Operator CEO's Age"
    label var operator_ceo_biography             "Operator CEO's Biography"
    label var operator_cfo_name                  "Operator CFO's Name"
    label var operator_cfo_age                   "Operator CFO's Age"
    label var operator_cfo_biography             "Operator CFO's Biography"
    label var operator_coo_name                  "Operator COO's Name"
    label var operator_coo_age                   "Operator COO's Age"
    label var operator_coo_biography             "Operator COO's Biography"
    label var operator_president_name            "Operator President's Name"
    label var operator_president_age             "Operator President's Age"
    label var operator_president_biography       "Operator President's Biography"
    label var operator_investor_relations_name   "Operator Investor Relations Professional's Name"
    label var operator_investor_relations_age    "Operator Investor Relations Professional's Age"
    label var operator_investor_relations_bio    "Operator Investor Relations Professional's Biography"
    label var operator_chairman_of_board_name    "Operator Chairman of the Board's Name"
    label var operator_chairman_of_board_age     "Operator Chairman of the Board's Age"
    label var operator_chairman_of_board_bio     "Operator Chairman of the Board's Biography"
    label var operator_vp_head_explor_name       "Operator VP/Head of Exploration's Name"
    label var operator_vp_head_explor_age        "Operator VP/Head of Exploration's Age"
    label var operator_vp_head_explor_bio        "Operator VP/Head of Exploration's Biography"
    label var operator_chief_geologist_name      "Operator Chief Geologist's Name"
    label var operator_chief_geologist_age       "Operator Chief Geologist's Age"
    label var operator_chief_geologist_bio       "Operator Chief Geologist's Biography"

    save "$temp_prop_details/property_details_3_2.dta", replace
end
program combine_property_details_4
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_4_ownership_info_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  num_royalty_owners
    rename D  owner_list
    rename E  num_royalty_holders
    rename F  royalty_holder_list

    * label variables
    label var prop_name              "Name of the mine or facility"
    label var prop_id                "Unique key for the project"
    label var num_royalty_owners     "Number of ownership stakes held by institutions in a mining project"
    label var owner_list             "A list of the project's current owners, including the amount owned"
    label var num_royalty_holders    "Number of royalties which are held against a mining project"
    label var royalty_holder_list    "A list of the project's current royalty holders, including percent of revenue"

    save "$temp_prop_details/property_details_4.dta", replace
end
program combine_property_details_5_1
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_5_ownership_details_1_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring H I J X Y Z AF AG AH, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  owner_name_1
    rename D  owner_name_2
    rename E  owner_name_3
    rename F  owner_name_4
    rename G  owner_name_5
    rename H  owner_name_6
    rename I  owner_name_7
    rename J  owner_name_8
    rename K  owner_snl_instn_key_1
    rename L  owner_snl_instn_key_2
    rename M  owner_snl_instn_key_3
    rename N  owner_snl_instn_key_4
    rename O  owner_snl_instn_key_5
    rename P  owner_snl_instn_key_6
    rename Q  owner_snl_instn_key_7
    rename R  owner_snl_instn_key_8
    rename S  current_owner_common_name_1
    rename T  current_owner_common_name_2
    rename U  current_owner_common_name_3
    rename V  current_owner_common_name_4
    rename W  current_owner_common_name_5
    rename X  current_owner_common_name_6
    rename Y  current_owner_common_name_7
    rename Z  current_owner_common_name_8
    rename AA current_owner_comp_name_abbr_1 // shortened to fit
    rename AB current_owner_comp_name_abbr_2 // shortened to fit
    rename AC current_owner_comp_name_abbr_3 // shortened to fit
    rename AD current_owner_comp_name_abbr_4 // shortened to fit
    rename AE current_owner_comp_name_abbr_5 // shortened to fit
    rename AF current_owner_comp_name_abbr_6 // shortened to fit
    rename AG current_owner_comp_name_abbr_7 // shortened to fit
    rename AH current_owner_comp_name_abbr_8 // shortened to fit

    * label variables
    label var prop_name                             "Name of the mine or facility"
    label var prop_id                               "Unique key for the project"
    label var owner_name_1                          "Complete name of the institution (Owner 1)"
    label var owner_name_2                          "Complete name of the institution (Owner 2)"
    label var owner_name_3                          "Complete name of the institution (Owner 3)"
    label var owner_name_4                          "Complete name of the institution (Owner 4)"
    label var owner_name_5                          "Complete name of the institution (Owner 5)"
    label var owner_name_6                          "Complete name of the institution (Owner 6)"
    label var owner_name_7                          "Complete name of the institution (Owner 7)"
    label var owner_name_8                          "Complete name of the institution (Owner 8)"
    label var owner_snl_instn_key_1                 "S&P's unique key to identify institutions (Owner 1)"
    label var owner_snl_instn_key_2                 "S&P's unique key to identify institutions (Owner 2)"
    label var owner_snl_instn_key_3                 "S&P's unique key to identify institutions (Owner 3)"
    label var owner_snl_instn_key_4                 "S&P's unique key to identify institutions (Owner 4)"
    label var owner_snl_instn_key_5                 "S&P's unique key to identify institutions (Owner 5)"
    label var owner_snl_instn_key_6                 "S&P's unique key to identify institutions (Owner 6)"
    label var owner_snl_instn_key_7                 "S&P's unique key to identify institutions (Owner 7)"
    label var owner_snl_instn_key_8                 "S&P's unique key to identify institutions (Owner 8)"
    label var current_owner_common_name_1           "Most recognized company name (Owner 1)"
    label var current_owner_common_name_2           "Most recognized company name (Owner 2)"
    label var current_owner_common_name_3           "Most recognized company name (Owner 3)"
    label var current_owner_common_name_4           "Most recognized company name (Owner 4)"
    label var current_owner_common_name_5           "Most recognized company name (Owner 5)"
    label var current_owner_common_name_6           "Most recognized company name (Owner 6)"
    label var current_owner_common_name_7           "Most recognized company name (Owner 7)"
    label var current_owner_common_name_8           "Most recognized company name (Owner 8)"
    label var current_owner_comp_name_abbr_1   "Shortened version of the company's name (Owner 1)"
    label var current_owner_comp_name_abbr_2   "Shortened version of the company's name (Owner 2)"
    label var current_owner_comp_name_abbr_3   "Shortened version of the company's name (Owner 3)"
    label var current_owner_comp_name_abbr_4   "Shortened version of the company's name (Owner 4)"
    label var current_owner_comp_name_abbr_5   "Shortened version of the company's name (Owner 5)"
    label var current_owner_comp_name_abbr_6   "Shortened version of the company's name (Owner 6)"
    label var current_owner_comp_name_abbr_7   "Shortened version of the company's name (Owner 7)"
    label var current_owner_comp_name_abbr_8   "Shortened version of the company's name (Owner 8)"

    save "$temp_prop_details/property_details_5_1.dta", replace
end
program combine_property_details_5_2
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_5_ownership_details_2_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring H I J AE AF AG AH, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  owner_type_1
    rename D  owner_type_2
    rename E  owner_type_3
    rename F  owner_type_4
    rename G  owner_type_5
    rename H  owner_type_6
    rename I  owner_type_7
    rename J  owner_type_8
    rename K  owner_pct_1
    rename L  owner_pct_2
    rename M  owner_pct_3
    rename N  owner_pct_4
    rename O  owner_pct_5
    rename P  owner_pct_6
    rename Q  owner_pct_7
    rename R  owner_pct_8
    rename S  current_controlling_own_pct_1
    rename T  current_controlling_own_pct_2
    rename U  current_controlling_own_pct_3
    rename V  current_controlling_own_pct_4
    rename W  current_controlling_own_pct_5
    rename X  current_controlling_own_pct_6
    rename Y  current_controlling_own_pct_7
    rename Z  current_controlling_own_pct_8
    rename AA owner_hq_1
    rename AB owner_hq_2
    rename AC owner_hq_3
    rename AD owner_hq_4
    rename AE owner_hq_5
    rename AF owner_hq_6
    rename AG owner_hq_7
    rename AH owner_hq_8

    * label variables
    label var prop_name                     "Name of the mine or facility"
    label var prop_id                       "Unique key for the project"
    label var owner_type_1                  "Forms of financial interest (Owner 1)"
    label var owner_type_2                  "Forms of financial interest (Owner 2)"
    label var owner_type_3                  "Forms of financial interest (Owner 3)"
    label var owner_type_4                  "Forms of financial interest (Owner 4)"
    label var owner_type_5                  "Forms of financial interest (Owner 5)"
    label var owner_type_6                  "Forms of financial interest (Owner 6)"
    label var owner_type_7                  "Forms of financial interest (Owner 7)"
    label var owner_type_8                  "Forms of financial interest (Owner 8)"
    label var owner_pct_1                   "Actual or potential ownership interest (Owner 1)"
    label var owner_pct_2                   "Actual or potential ownership interest (Owner 2)"
    label var owner_pct_3                   "Actual or potential ownership interest (Owner 3)"
    label var owner_pct_4                   "Actual or potential ownership interest (Owner 4)"
    label var owner_pct_5                   "Actual or potential ownership interest (Owner 5)"
    label var owner_pct_6                   "Actual or potential ownership interest (Owner 6)"
    label var owner_pct_7                   "Actual or potential ownership interest (Owner 7)"
    label var owner_pct_8                   "Actual or potential ownership interest (Owner 8)"
    label var current_controlling_own_pct_1 "Percentage of controlling ownership (Owner 1)"
    label var current_controlling_own_pct_2 "Percentage of controlling ownership (Owner 2)"
    label var current_controlling_own_pct_3 "Percentage of controlling ownership (Owner 3)"
    label var current_controlling_own_pct_4 "Percentage of controlling ownership (Owner 4)"
    label var current_controlling_own_pct_5 "Percentage of controlling ownership (Owner 5)"
    label var current_controlling_own_pct_6 "Percentage of controlling ownership (Owner 6)"
    label var current_controlling_own_pct_7 "Percentage of controlling ownership (Owner 7)"
    label var current_controlling_own_pct_8 "Percentage of controlling ownership (Owner 8)"
    label var owner_hq_1                    "Headquarter city (Owner 1)"
    label var owner_hq_2                    "Headquarter city (Owner 2)"
    label var owner_hq_3                    "Headquarter city (Owner 3)"
    label var owner_hq_4                    "Headquarter city (Owner 4)"
    label var owner_hq_5                    "Headquarter city (Owner 5)"
    label var owner_hq_6                    "Headquarter city (Owner 6)"
    label var owner_hq_7                    "Headquarter city (Owner 7)"
    label var owner_hq_8                    "Headquarter city (Owner 8)"

    save "$temp_prop_details/property_details_5_2.dta", replace
end
program combine_property_details_5_3
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_5_ownership_details_3_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring C-AH, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  owner_address1_1
    rename D  owner_address1_2
    rename E  owner_address1_3
    rename F  owner_address1_4
    rename G  owner_address1_5
    rename H  owner_address1_6
    rename I  owner_address1_7
    rename J  owner_address1_8
    rename K  owner_address2_1
    rename L  owner_address2_2
    rename M  owner_address2_3
    rename N  owner_address2_4
    rename O  owner_address2_5
    rename P  owner_address2_6
    rename Q  owner_address2_7
    rename R  owner_address2_8
    rename S  owner_city_state_1
    rename T  owner_city_state_2
    rename U  owner_city_state_3
    rename V  owner_city_state_4
    rename W  owner_city_state_5
    rename X  owner_city_state_6
    rename Y  owner_city_state_7
    rename Z  owner_city_state_8
    rename AA owner_state_1
    rename AB owner_state_2
    rename AC owner_state_3
    rename AD owner_state_4
    rename AE owner_state_5
    rename AF owner_state_6
    rename AG owner_state_7
    rename AH owner_state_8

    * label variables
    label var prop_name                 "Name of the mine or facility"
    label var prop_id                   "Unique key for the project"
    label var owner_address1_1          "First line of an address (Owner 1)"
    label var owner_address1_2          "First line of an address (Owner 2)"
    label var owner_address1_3          "First line of an address (Owner 3)"
    label var owner_address1_4          "First line of an address (Owner 4)"
    label var owner_address1_5          "First line of an address (Owner 5)"
    label var owner_address1_6          "First line of an address (Owner 6)"
    label var owner_address1_7          "First line of an address (Owner 7)"
    label var owner_address1_8          "First line of an address (Owner 8)"
    label var owner_address2_1          "Second line of an address (Owner 1)"
    label var owner_address2_2          "Second line of an address (Owner 2)"
    label var owner_address2_3          "Second line of an address (Owner 3)"
    label var owner_address2_4          "Second line of an address (Owner 4)"
    label var owner_address2_5          "Second line of an address (Owner 5)"
    label var owner_address2_6          "Second line of an address (Owner 6)"
    label var owner_address2_7          "Second line of an address (Owner 7)"
    label var owner_address2_8          "Second line of an address (Owner 8)"
    label var owner_city_state_1        "City and state or province (Owner 1)"
    label var owner_city_state_2        "City and state or province (Owner 2)"
    label var owner_city_state_3        "City and state or province (Owner 3)"
    label var owner_city_state_4        "City and state or province (Owner 4)"
    label var owner_city_state_5        "City and state or province (Owner 5)"
    label var owner_city_state_6        "City and state or province (Owner 6)"
    label var owner_city_state_7        "City and state or province (Owner 7)"
    label var owner_city_state_8        "City and state or province (Owner 8)"
    label var owner_state_1             "State postal code (Owner 1)"
    label var owner_state_2             "State postal code (Owner 2)"
    label var owner_state_3             "State postal code (Owner 3)"
    label var owner_state_4             "State postal code (Owner 4)"
    label var owner_state_5             "State postal code (Owner 5)"
    label var owner_state_6             "State postal code (Owner 6)"
    label var owner_state_7             "State postal code (Owner 7)"
    label var owner_state_8             "State postal code (Owner 8)"

    save "$temp_prop_details/property_details_5_3.dta", replace

end
program combine_property_details_5_4
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_5_ownership_details_4_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring C-AH, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  owner_location_1
    rename D  owner_location_2
    rename E  owner_location_3
    rename F  owner_location_4
    rename G  owner_location_5
    rename H  owner_location_6
    rename I  owner_location_7
    rename J  owner_location_8
    rename K  owner_frgn_province_1
    rename L  owner_frgn_province_2
    rename M  owner_frgn_province_3
    rename N  owner_frgn_province_4
    rename O  owner_frgn_province_5
    rename P  owner_frgn_province_6
    rename Q  owner_frgn_province_7
    rename R  owner_frgn_province_8
    rename S  owner_country_1
    rename T  owner_country_2
    rename U  owner_country_3
    rename V  owner_country_4
    rename W  owner_country_5
    rename X  owner_country_6
    rename Y  owner_country_7
    rename Z  owner_country_8
    rename AA owner_zip_1
    rename AB owner_zip_2
    rename AC owner_zip_3
    rename AD owner_zip_4
    rename AE owner_zip_5
    rename AF owner_zip_6
    rename AG owner_zip_7
    rename AH owner_zip_8

    * label variables
    label var prop_name                 "Name of the mine or facility"
    label var prop_id                   "Unique key for the project"
    label var owner_location_1          "City and state (U.S.) or city and country (non-U.S.) where the company is headquartered (Owner 1)"
    label var owner_location_2          "City and state (U.S.) or city and country (non-U.S.) where the company is headquartered (Owner 2)"
    label var owner_location_3          "City and state (U.S.) or city and country (non-U.S.) where the company is headquartered (Owner 3)"
    label var owner_location_4          "City and state (U.S.) or city and country (non-U.S.) where the company is headquartered (Owner 4)"
    label var owner_location_5          "City and state (U.S.) or city and country (non-U.S.) where the company is headquartered (Owner 5)"
    label var owner_location_6          "City and state (U.S.) or city and country (non-U.S.) where the company is headquartered (Owner 6)"
    label var owner_location_7          "City and state (U.S.) or city and country (non-U.S.) where the company is headquartered (Owner 7)"
    label var owner_location_8          "City and state (U.S.) or city and country (non-U.S.) where the company is headquartered (Owner 8)"
    label var owner_frgn_province_1      "Province or major political subdivision for non-U.S. addresses (Owner 1)"
    label var owner_frgn_province_2      "Province or major political subdivision for non-U.S. addresses (Owner 2)"
    label var owner_frgn_province_3      "Province or major political subdivision for non-U.S. addresses (Owner 3)"
    label var owner_frgn_province_4      "Province or major political subdivision for non-U.S. addresses (Owner 4)"
    label var owner_frgn_province_5      "Province or major political subdivision for non-U.S. addresses (Owner 5)"
    label var owner_frgn_province_6      "Province or major political subdivision for non-U.S. addresses (Owner 6)"
    label var owner_frgn_province_7      "Province or major political subdivision for non-U.S. addresses (Owner 7)"
    label var owner_frgn_province_8      "Province or major political subdivision for non-U.S. addresses (Owner 8)"
    label var owner_country_1            "Country where the owner is headquartered (Owner 1)"
    label var owner_country_2            "Country where the owner is headquartered (Owner 2)"
    label var owner_country_3            "Country where the owner is headquartered (Owner 3)"
    label var owner_country_4            "Country where the owner is headquartered (Owner 4)"
    label var owner_country_5            "Country where the owner is headquartered (Owner 5)"
    label var owner_country_6            "Country where the owner is headquartered (Owner 6)"
    label var owner_country_7            "Country where the owner is headquartered (Owner 7)"
    label var owner_country_8            "Country where the owner is headquartered (Owner 8)"
    label var owner_zip_1                "U.S. Postal Service Zip Code (Owner 1)"
    label var owner_zip_2                "U.S. Postal Service Zip Code (Owner 2)"
    label var owner_zip_3                "U.S. Postal Service Zip Code (Owner 3)"
    label var owner_zip_4                "U.S. Postal Service Zip Code (Owner 4)"
    label var owner_zip_5                "U.S. Postal Service Zip Code (Owner 5)"
    label var owner_zip_6                "U.S. Postal Service Zip Code (Owner 6)"
    label var owner_zip_7                "U.S. Postal Service Zip Code (Owner 7)"
    label var owner_zip_8                "U.S. Postal Service Zip Code (Owner 8)"

    save "$temp_prop_details/property_details_5_4.dta", replace
end
program combine_property_details_5_5
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok
    foreach region of local regions {
        local file_name "$input_property_details/property_details_5_ownership_details_5_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring C-AH, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }
    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  owner_postal_code_1
    rename D  owner_postal_code_2
    rename E  owner_postal_code_3
    rename F  owner_postal_code_4
    rename G  owner_postal_code_5
    rename H  owner_postal_code_6
    rename I  owner_postal_code_7
    rename J  owner_postal_code_8
    rename K  owner_global_region_1
    rename L  owner_global_region_2
    rename M  owner_global_region_3
    rename N  owner_global_region_4
    rename O  owner_global_region_5
    rename P  owner_global_region_6
    rename Q  owner_global_region_7
    rename R  owner_global_region_8
    rename S  owner_phone_1
    rename T  owner_phone_2
    rename U  owner_phone_3
    rename V  owner_phone_4
    rename W  owner_phone_5
    rename X  owner_phone_6
    rename Y  owner_phone_7
    rename Z  owner_phone_8
    rename AA owner_website_1
    rename AB owner_website_2
    rename AC owner_website_3
    rename AD owner_website_4
    rename AE owner_website_5
    rename AF owner_website_6
    rename AG owner_website_7
    rename AH owner_website_8
    * label variables
    label var prop_name                 "Name of the mine or facility"
    label var prop_id                   "Unique key for the project"
    label var owner_postal_code_1       "Postal code or routing code for non-U.S. addresses (Owner 1)"
    label var owner_postal_code_2       "Postal code or routing code for non-U.S. addresses (Owner 2)"
    label var owner_postal_code_3       "Postal code or routing code for non-U.S. addresses (Owner 3)"
    label var owner_postal_code_4       "Postal code or routing code for non-U.S. addresses (Owner 4)"
    label var owner_postal_code_5       "Postal code or routing code for non-U.S. addresses (Owner 5)"
    label var owner_postal_code_6       "Postal code or routing code for non-U.S. addresses (Owner 6)"
    label var owner_postal_code_7       "Postal code or routing code for non-U.S. addresses (Owner 7)"
    label var owner_postal_code_8       "Postal code or routing code for non-U.S. addresses (Owner 8)"
    label var owner_global_region_1     "Global region where the owner is headquartered (Owner 1)"
    label var owner_global_region_2     "Global region where the owner is headquartered (Owner 2)"
    label var owner_global_region_3     "Global region where the owner is headquartered (Owner 3)"
    label var owner_global_region_4     "Global region where the owner is headquartered (Owner 4)"
    label var owner_global_region_5     "Global region where the owner is headquartered (Owner 5)"
    label var owner_global_region_6     "Global region where the owner is headquartered (Owner 6)"
    label var owner_global_region_7     "Global region where the owner is headquartered (Owner 7)"
    label var owner_global_region_8     "Global region where the owner is headquartered (Owner 8)"
    label var owner_phone_1             "Headquarter phone number (Owner 1)"
    label var owner_phone_2             "Headquarter phone number (Owner 2)"
    label var owner_phone_3             "Headquarter phone number (Owner 3)"
    label var owner_phone_4             "Headquarter phone number (Owner 4)"
    label var owner_phone_5             "Headquarter phone number (Owner 5)"
    label var owner_phone_6             "Headquarter phone number (Owner 6)"
    label var owner_phone_7             "Headquarter phone number (Owner 7)"
    label var owner_phone_8             "Headquarter phone number (Owner 8)"
    label var owner_website_1           "Owner website URL (Owner 1)"
    label var owner_website_2           "Owner website URL (Owner 2)"
    label var owner_website_3           "Owner website URL (Owner 3)"
    label var owner_website_4           "Owner website URL (Owner 4)"
    label var owner_website_5           "Owner website URL (Owner 5)"
    label var owner_website_6           "Owner website URL (Owner 6)"
    label var owner_website_7           "Owner website URL (Owner 7)"
    label var owner_website_8           "Owner website URL (Owner 8)"

    save "$temp_prop_details/property_details_5_5.dta", replace
end
program combine_property_details_5_6
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_5_ownership_details_6_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }
    format C-J %tdnn/dd/CCYY //format date variables

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  owner_date_closing_price_1
    rename D  owner_date_closing_price_2
    rename E  owner_date_closing_price_3
    rename F  owner_date_closing_price_4
    rename G  owner_date_closing_price_5
    rename H  owner_date_closing_price_6
    rename I  owner_date_closing_price_7
    rename J  owner_date_closing_price_8
    rename K  owner_mkt_cap_1
    rename L  owner_mkt_cap_2
    rename M  owner_mkt_cap_3
    rename N  owner_mkt_cap_4
    rename O  owner_mkt_cap_5
    rename P  owner_mkt_cap_6
    rename Q  owner_mkt_cap_7
    rename R  owner_mkt_cap_8
    rename S  owner_price_to_ltm_eps_1
    rename T  owner_price_to_ltm_eps_2
    rename U  owner_price_to_ltm_eps_3
    rename V  owner_price_to_ltm_eps_4
    rename W  owner_price_to_ltm_eps_5
    rename X  owner_price_to_ltm_eps_6
    rename Y  owner_price_to_ltm_eps_7
    rename Z  owner_price_to_ltm_eps_8
    rename AA owner_tev_1
    rename AB owner_tev_2
    rename AC owner_tev_3
    rename AD owner_tev_4
    rename AE owner_tev_5
    rename AF owner_tev_6
    rename AG owner_tev_7
    rename AH owner_tev_8

    * label variables
    label var prop_name                     "Name of the mine or facility"
    label var prop_id                       "Unique key for the project"
    label var owner_date_closing_price_1    "Date of closing price (Owner 1)"
    label var owner_date_closing_price_2    "Date of closing price (Owner 2)"
    label var owner_date_closing_price_3    "Date of closing price (Owner 3)"
    label var owner_date_closing_price_4    "Date of closing price (Owner 4)"
    label var owner_date_closing_price_5    "Date of closing price (Owner 5)"
    label var owner_date_closing_price_6    "Date of closing price (Owner 6)"
    label var owner_date_closing_price_7    "Date of closing price (Owner 7)"
    label var owner_date_closing_price_8    "Date of closing price (Owner 8)"
    label var owner_mkt_cap_1               "Market capitalization (Owner 1)"
    label var owner_mkt_cap_2               "Market capitalization (Owner 2)"
    label var owner_mkt_cap_3               "Market capitalization (Owner 3)"
    label var owner_mkt_cap_4               "Market capitalization (Owner 4)"
    label var owner_mkt_cap_5               "Market capitalization (Owner 5)"
    label var owner_mkt_cap_6               "Market capitalization (Owner 6)"
    label var owner_mkt_cap_7               "Market capitalization (Owner 7)"
    label var owner_mkt_cap_8               "Market capitalization (Owner 8)"
    label var owner_price_to_ltm_eps_1      "Price to LTM EPS (Owner 1)"
    label var owner_price_to_ltm_eps_2      "Price to LTM EPS (Owner 2)"
    label var owner_price_to_ltm_eps_3      "Price to LTM EPS (Owner 3)"
    label var owner_price_to_ltm_eps_4      "Price to LTM EPS (Owner 4)"
    label var owner_price_to_ltm_eps_5      "Price to LTM EPS (Owner 5)"
    label var owner_price_to_ltm_eps_6      "Price to LTM EPS (Owner 6)"
    label var owner_price_to_ltm_eps_7      "Price to LTM EPS (Owner 7)"
    label var owner_price_to_ltm_eps_8      "Price to LTM EPS (Owner 8)"
    label var owner_tev_1                   "Total enterprise value (Owner 1)"
    label var owner_tev_2                   "Total enterprise value (Owner 2)"
    label var owner_tev_3                   "Total enterprise value (Owner 3)"
    label var owner_tev_4                   "Total enterprise value (Owner 4)"
    label var owner_tev_5               "Total enterprise value (Owner 5)"
    label var owner_tev_6               "Total enterprise value (Owner 6)"
    label var owner_tev_7               "Total enterprise value (Owner 7)"
    label var owner_tev_8                   "Total enterprise value (Owner 8)"

    save "$temp_prop_details/property_details_5_6.dta", replace
end
program combine_property_details_5_7
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_5_ownership_details_7_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  owner_tev_to_ltm_ebitda_1
    rename D  owner_tev_to_ltm_ebitda_2
    rename E  owner_tev_to_ltm_ebitda_3
    rename F  owner_tev_to_ltm_ebitda_4
    rename G  owner_tev_to_ltm_ebitda_5
    rename H  owner_tev_to_ltm_ebitda_6
    rename I  owner_tev_to_ltm_ebitda_7
    rename J  owner_tev_to_ltm_ebitda_8
    rename K  owner_total_debt_to_total_cap_1
    rename L  owner_total_debt_to_total_cap_2
    rename M  owner_total_debt_to_total_cap_3
    rename N  owner_total_debt_to_total_cap_4
    rename O  owner_total_debt_to_total_cap_5
    rename P  owner_total_debt_to_total_cap_6
    rename Q  owner_total_debt_to_total_cap_7
    rename R  owner_total_debt_to_total_cap_8
    rename S  owner_price_to_earn_after_extra1
    rename T  owner_price_to_earn_after_extra2
    rename U  owner_price_to_earn_after_extra3
    rename V  owner_price_to_earn_after_extra4
    rename W  owner_price_to_earn_after_extra5
    rename X  owner_price_to_earn_after_extra6
    rename Y  owner_price_to_earn_after_extra7
    rename Z  owner_price_to_earn_after_extra8
    rename AA owner_tev_to_ebitda_1
    rename AB owner_tev_to_ebitda_2
    rename AC owner_tev_to_ebitda_3
    rename AD owner_tev_to_ebitda_4
    rename AE owner_tev_to_ebitda_5
    rename AF owner_tev_to_ebitda_6
    rename AG owner_tev_to_ebitda_7
    rename AH owner_tev_to_ebitda_8

    * label variables
    label var prop_name                     "Name of the mine or facility"
    label var prop_id                       "Unique key for the project"
    label var owner_tev_to_ltm_ebitda_1     "TEV to LTM EBITDA (Owner 1)"
    label var owner_tev_to_ltm_ebitda_2     "TEV to LTM EBITDA (Owner 2)"
    label var owner_tev_to_ltm_ebitda_3     "TEV to LTM EBITDA (Owner 3)"
    label var owner_tev_to_ltm_ebitda_4     "TEV to LTM EBITDA (Owner 4)"
    label var owner_tev_to_ltm_ebitda_5     "TEV to LTM EBITDA (Owner 5)"
    label var owner_tev_to_ltm_ebitda_6     "TEV to LTM EBITDA (Owner 6)"
    label var owner_tev_to_ltm_ebitda_7     "TEV to LTM EBITDA (Owner 7)"
    label var owner_tev_to_ltm_ebitda_8     "TEV to LTM EBITDA (Owner 8)"
    label var owner_total_debt_to_total_cap_1 "Total debt to total capitalization (Owner 1)"
    label var owner_total_debt_to_total_cap_2 "Total debt to total capitalization (Owner 2)"
    label var owner_total_debt_to_total_cap_3 "Total debt to total capitalization (Owner 3)"
    label var owner_total_debt_to_total_cap_4 "Total debt to total capitalization (Owner 4)"
    label var owner_total_debt_to_total_cap_5 "Total debt to total capitalization (Owner 5)"
    label var owner_total_debt_to_total_cap_6 "Total debt to total capitalization (Owner 6)"
    label var owner_total_debt_to_total_cap_7 "Total debt to total capitalization (Owner 7)"
    label var owner_total_debt_to_total_cap_8 "Total debt to total capitalization (Owner 8)"
    label var owner_price_to_earn_after_extra1 "Price to earnings after extra (Owner 1)"
    label var owner_price_to_earn_after_extra2 "Price to earnings after extra (Owner 2)"
    label var owner_price_to_earn_after_extra3 "Price to earnings after extra (Owner 3)"
    label var owner_price_to_earn_after_extra4 "Price to earnings after extra (Owner 4)"
    label var owner_price_to_earn_after_extra5 "Price to earnings after extra (Owner 5)"
    label var owner_price_to_earn_after_extra6 "Price to earnings after extra (Owner 6)"
    label var owner_price_to_earn_after_extra7 "Price to earnings after extra (Owner 7)"
    label var owner_price_to_earn_after_extra8 "Price to earnings after extra (Owner 8)"
    label var owner_tev_to_ebitda_1         "TEV to EBITDA (Owner 1)"
    label var owner_tev_to_ebitda_2         "TEV to EBITDA (Owner 2)"
    label var owner_tev_to_ebitda_3         "TEV to EBITDA (Owner 3)"
    label var owner_tev_to_ebitda_4         "TEV to EBITDA (Owner 4)"
    label var owner_tev_to_ebitda_5     "TEV to EBITDA (Owner 5)"
    label var owner_tev_to_ebitda_6     "TEV to EBITDA (Owner 6)"
    label var owner_tev_to_ebitda_7     "TEV to EBITDA (Owner 7)"
    label var owner_tev_to_ebitda_8     "TEV to EBITDA (Owner 8)"

    save "$temp_prop_details/property_details_5_7.dta", replace
end
program combine_property_details_5_8
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_5_ownership_details_8_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }
    format C-J %tdnn/dd/CCYY

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  owner_period_ended_1
    rename D  owner_period_ended_2
    rename E  owner_period_ended_3
    rename F  owner_period_ended_4
    rename G  owner_period_ended_5
    rename H  owner_period_ended_6
    rename I  owner_period_ended_7
    rename J  owner_period_ended_8
    rename K  owner_working_capital_1
    rename L  owner_working_capital_2
    rename M  owner_working_capital_3
    rename N  owner_working_capital_4
    rename O  owner_working_capital_5
    rename P  owner_working_capital_6
    rename Q  owner_working_capital_7
    rename R  owner_working_capital_8
    rename S  owner_total_cap_at_bv_1
    rename T  owner_total_cap_at_bv_2
    rename U  owner_total_cap_at_bv_3
    rename V  owner_total_cap_at_bv_4
    rename W  owner_total_cap_at_bv_5
    rename X  owner_total_cap_at_bv_6
    rename Y  owner_total_cap_at_bv_7
    rename Z  owner_total_cap_at_bv_8
    rename AA owner_total_debt_1
    rename AB owner_total_debt_2
    rename AC owner_total_debt_3
    rename AD owner_total_debt_4
    rename AE owner_total_debt_5
    rename AF owner_total_debt_6
    rename AG owner_total_debt_7
    rename AH owner_total_debt_8

    * label variables
    label var prop_name                     "Name of the mine or facility"
    label var prop_id                       "Unique key for the project"
    label var owner_period_ended_1          "Period ended (Owner 1)"
    label var owner_period_ended_2          "Period ended (Owner 2)"
    label var owner_period_ended_3          "Period ended (Owner 3)"
    label var owner_period_ended_4          "Period ended (Owner 4)"
    label var owner_period_ended_5          "Period ended (Owner 5)"
    label var owner_period_ended_6          "Period ended (Owner 6)"
    label var owner_period_ended_7          "Period ended (Owner 7)"
    label var owner_period_ended_8          "Period ended (Owner 8)"
    label var owner_working_capital_1       "Working capital (Owner 1)"
    label var owner_working_capital_2       "Working capital (Owner 2)"
    label var owner_working_capital_3       "Working capital (Owner 3)"
    label var owner_working_capital_4       "Working capital (Owner 4)"
    label var owner_working_capital_5       "Working capital (Owner 5)"
    label var owner_working_capital_6       "Working capital (Owner 6)"
    label var owner_working_capital_7       "Working capital (Owner 7)"
    label var owner_working_capital_8       "Working capital (Owner 8)"
    label var owner_total_cap_at_bv_1       "Total capitalization at book value (Owner 1)"
    label var owner_total_cap_at_bv_2       "Total capitalization at book value (Owner 2)"
    label var owner_total_cap_at_bv_3       "Total capitalization at book value (Owner 3)"
    label var owner_total_cap_at_bv_4       "Total capitalization at book value (Owner 4)"
    label var owner_total_cap_at_bv_5       "Total capitalization at book value (Owner 5)"
    label var owner_total_cap_at_bv_6       "Total capitalization at book value (Owner 6)"
    label var owner_total_cap_at_bv_7       "Total capitalization at book value (Owner 7)"
    label var owner_total_cap_at_bv_8       "Total capitalization at book value (Owner 8)"
    label var owner_total_debt_1            "Total debt (Owner 1)"
    label var owner_total_debt_2            "Total debt (Owner 2)"
    label var owner_total_debt_3            "Total debt (Owner 3)"
    label var owner_total_debt_4            "Total debt (Owner 4)"
    label var owner_total_debt_5            "Total debt (Owner 5)"
    label var owner_total_debt_6            "Total debt (Owner 6)"
    label var owner_total_debt_7            "Total debt (Owner 7)"
    label var owner_total_debt_8            "Total debt (Owner 8)"

    save "$temp_prop_details/property_details_5_8.dta", replace
end
program combine_property_details_5_9
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_5_ownership_details_9_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  owner_current_liab_1
    rename D  owner_current_liab_2
    rename E  owner_current_liab_3
    rename F  owner_current_liab_4
    rename G  owner_current_liab_5
    rename H  owner_current_liab_6
    rename I  owner_current_liab_7
    rename J  owner_current_liab_8
    rename K  cash_and_equiv_most_recent_yr_1 // shortened to fit
    rename L  cash_and_equiv_most_recent_yr_2
    rename M  cash_and_equiv_most_recent_yr_3
    rename N  cash_and_equiv_most_recent_yr_4
    rename O  cash_and_equiv_most_recent_yr_5
    rename P  cash_and_equiv_most_recent_yr_6
    rename Q  cash_and_equiv_most_recent_yr_7
    rename R  cash_and_equiv_most_recent_yr_8
    rename S  cash_and_equiv_most_recent_qtr_1
    rename T  cash_and_equiv_most_recent_qtr_2
    rename U  cash_and_equiv_most_recent_qtr_3
    rename V  cash_and_equiv_most_recent_qtr_4
    rename W  cash_and_equiv_most_recent_qtr_5
    rename X  cash_and_equiv_most_recent_qtr_6
    rename Y  cash_and_equiv_most_recent_qtr_7
    rename Z  cash_and_equiv_most_recent_qtr_8

    * label variables
    label var prop_name                             "Name of the mine or facility"
    label var prop_id                               "Unique key for the project"
    label var owner_current_liab_1                 "Current liabilities (Owner 1)"
    label var owner_current_liab_2                 "Current liabilities (Owner 2)"
    label var owner_current_liab_3                 "Current liabilities (Owner 3)"
    label var owner_current_liab_4                 "Current liabilities (Owner 4)"
    label var owner_current_liab_5                 "Current liabilities (Owner 5)"
    label var owner_current_liab_6                 "Current liabilities (Owner 6)"
    label var owner_current_liab_7                 "Current liabilities (Owner 7)"
    label var owner_current_liab_8                 "Current liabilities (Owner 8)"
    label var cash_and_equiv_most_recent_yr_1 "Cash & cash equivalents - Most Recent Year (Owner 1)"
    label var cash_and_equiv_most_recent_yr_2 "Cash & cash equivalents - Most Recent Year (Owner 2)"
    label var cash_and_equiv_most_recent_yr_3 "Cash & cash equivalents - Most Recent Year (Owner 3)"
    label var cash_and_equiv_most_recent_yr_4 "Cash & cash equivalents - Most Recent Year (Owner 4)"
    label var cash_and_equiv_most_recent_yr_5 "Cash & cash equivalents - Most Recent Year (Owner 5)"
    label var cash_and_equiv_most_recent_yr_6 "Cash & cash equivalents - Most Recent Year (Owner 6)"
    label var cash_and_equiv_most_recent_yr_7 "Cash & cash equivalents - Most Recent Year (Owner 7)"
    label var cash_and_equiv_most_recent_yr_8 "Cash & cash equivalents - Most Recent Year (Owner 8)"
    label var cash_and_equiv_most_recent_qtr_1 "Cash & cash equivalents - Most Recent Quarter (Owner 1)"
    label var cash_and_equiv_most_recent_qtr_2 "Cash & cash equivalents - Most Recent Quarter (Owner 2)"
    label var cash_and_equiv_most_recent_qtr_3 "Cash & cash equivalents - Most Recent Quarter (Owner 3)"
    label var cash_and_equiv_most_recent_qtr_4 "Cash & cash equivalents - Most Recent Quarter (Owner 4)"
    label var cash_and_equiv_most_recent_qtr_5 "Cash & cash equivalents - Most Recent Quarter (Owner 5)"
    label var cash_and_equiv_most_recent_qtr_6 "Cash & cash equivalents - Most Recent Quarter (Owner 6)"
    label var cash_and_equiv_most_recent_qtr_7 "Cash & cash equivalents - Most Recent Quarter (Owner 7)"
    label var cash_and_equiv_most_recent_qtr_8 "Cash & cash equivalents - Most Recent Quarter (Owner 8)"

    save "$temp_prop_details/property_details_5_9.dta", replace
end
program combine_property_details_5_10
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_5_ownership_details_10_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  app5b_cash_begin_yr_1
    rename D  app5b_cash_begin_yr_2
    rename E  app5b_cash_begin_yr_3
    rename F  app5b_cash_begin_yr_4
    rename G  app5b_cash_begin_yr_5
    rename H  app5b_cash_begin_yr_6
    rename I  app5b_cash_begin_yr_7
    rename J  app5b_cash_begin_yr_8
    rename K  app5b_cash_begin_qtr_1
    rename L  app5b_cash_begin_qtr_2
    rename M  app5b_cash_begin_qtr_3
    rename N  app5b_cash_begin_qtr_4
    rename O  app5b_cash_begin_qtr_5
    rename P  app5b_cash_begin_qtr_6
    rename Q  app5b_cash_begin_qtr_7
    rename R  app5b_cash_begin_qtr_8
    rename S  app5b_cash_end_yr_1
    rename T  app5b_cash_end_yr_2
    rename U  app5b_cash_end_yr_3
    rename V  app5b_cash_end_yr_4
    rename W  app5b_cash_end_yr_5
    rename X  app5b_cash_end_yr_6
    rename Y  app5b_cash_end_yr_7
    rename Z  app5b_cash_end_yr_8
    rename AA app5b_cash_end_qtr_1
    rename AB app5b_cash_end_qtr_2
    rename AC app5b_cash_end_qtr_3
    rename AD app5b_cash_end_qtr_4
    rename AE app5b_cash_end_qtr_5
    rename AF app5b_cash_end_qtr_6
    rename AG app5b_cash_end_qtr_7
    rename AH app5b_cash_end_qtr_8

    * label variables
    label var prop_name                     "Name of the mine or facility"
    label var prop_id                       "Unique key for the project"
    label var app5b_cash_begin_yr_1         "App5B: Cash at Beginning of Period - Most Recent Year (Owner 1)"
    label var app5b_cash_begin_yr_2         "App5B: Cash at Beginning of Period - Most Recent Year (Owner 2)"
    label var app5b_cash_begin_yr_3         "App5B: Cash at Beginning of Period - Most Recent Year (Owner 3)"
    label var app5b_cash_begin_yr_4         "App5B: Cash at Beginning of Period - Most Recent Year (Owner 4)"
    label var app5b_cash_begin_yr_5         "App5B: Cash at Beginning of Period - Most Recent Year (Owner 5)"
    label var app5b_cash_begin_yr_6         "App5B: Cash at Beginning of Period - Most Recent Year (Owner 6)"
    label var app5b_cash_begin_yr_7         "App5B: Cash at Beginning of Period - Most Recent Year (Owner 7)"
    label var app5b_cash_begin_yr_8         "App5B: Cash at Beginning of Period - Most Recent Year (Owner 8)"
    label var app5b_cash_begin_qtr_1        "App5B: Cash at Beginning of Period - Most Recent Quarter (Owner 1)"
    label var app5b_cash_begin_qtr_2        "App5B: Cash at Beginning of Period - Most Recent Quarter (Owner 2)"
    label var app5b_cash_begin_qtr_3        "App5B: Cash at Beginning of Period - Most Recent Quarter (Owner 3)"
    label var app5b_cash_begin_qtr_4        "App5B: Cash at Beginning of Period - Most Recent Quarter (Owner 4)"
    label var app5b_cash_begin_qtr_5        "App5B: Cash at Beginning of Period - Most Recent Quarter (Owner 5)"
    label var app5b_cash_begin_qtr_6        "App5B: Cash at Beginning of Period - Most Recent Quarter (Owner 6)"
    label var app5b_cash_begin_qtr_7        "App5B: Cash at Beginning of Period - Most Recent Quarter (Owner 7)"
    label var app5b_cash_begin_qtr_8        "App5B: Cash at Beginning of Period - Most Recent Quarter (Owner 8)"
    label var app5b_cash_end_yr_1           "App5B: Cash at End of Period - Most Recent Year (Owner 1)"
    label var app5b_cash_end_yr_2           "App5B: Cash at End of Period - Most Recent Year (Owner 2)"
    label var app5b_cash_end_yr_3           "App5B: Cash at End of Period - Most Recent Year (Owner 3)"
    label var app5b_cash_end_yr_4           "App5B: Cash at End of Period - Most Recent Year (Owner 4)"
    label var app5b_cash_end_yr_5           "App5B: Cash at End of Period - Most Recent Year (Owner 5)"
    label var app5b_cash_end_yr_6           "App5B: Cash at End of Period - Most Recent Year (Owner 6)"
    label var app5b_cash_end_yr_7           "App5B: Cash at End of Period - Most Recent Year (Owner 7)"
    label var app5b_cash_end_yr_8           "App5B: Cash at End of Period - Most Recent Year (Owner 8)"
    label var app5b_cash_end_qtr_1          "App5B: Cash at End of Period - Most Recent Quarter (Owner 1)"
    label var app5b_cash_end_qtr_2          "App5B: Cash at End of Period - Most Recent Quarter (Owner 2)"
    label var app5b_cash_end_qtr_3          "App5B: Cash at End of Period - Most Recent Quarter (Owner 3)"
    label var app5b_cash_end_qtr_4          "App5B: Cash at End of Period - Most Recent Quarter (Owner 4)"
    label var app5b_cash_end_qtr_5          "App5B: Cash at End of Period - Most Recent Quarter (Owner 5)"
    label var app5b_cash_end_qtr_6          "App5B: Cash at End of Period - Most Recent Quarter (Owner 6)"
    label var app5b_cash_end_qtr_7          "App5B: Cash at End of Period - Most Recent Quarter (Owner 7)"
    label var app5b_cash_end_qtr_8          "App5B: Cash at End of Period - Most Recent Quarter (Owner 8)"

    save "$temp_prop_details/property_details_5_10.dta", replace
end
program combine_property_details_5_11
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_5_ownership_details_11_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  app5b_net_increase_cash_yr_1
    rename D  app5b_net_increase_cash_yr_2
    rename E  app5b_net_increase_cash_yr_3
    rename F  app5b_net_increase_cash_yr_4
    rename G  app5b_net_increase_cash_yr_5
    rename H  app5b_net_increase_cash_yr_6
    rename I  app5b_net_increase_cash_yr_7
    rename J  app5b_net_increase_cash_yr_8
    rename K  app5b_net_increase_cash_qtr_1
    rename L  app5b_net_increase_cash_qtr_2
    rename M  app5b_net_increase_cash_qtr_3
    rename N  app5b_net_increase_cash_qtr_4
    rename O  app5b_net_increase_cash_qtr_5
    rename P  app5b_net_increase_cash_qtr_6
    rename Q  app5b_net_increase_cash_qtr_7
    rename R  app5b_net_increase_cash_qtr_8
    rename S  app5b_est_cf_next_qtr_1
    rename T  app5b_est_cf_next_qtr_2
    rename U  app5b_est_cf_next_qtr_3
    rename V  app5b_est_cf_next_qtr_4
    rename W  app5b_est_cf_next_qtr_5
    rename X  app5b_est_cf_next_qtr_6
    rename Y  app5b_est_cf_next_qtr_7
    rename Z  app5b_est_cf_next_qtr_8

    * label variables
    label var prop_name                     "Name of the mine or facility"
    label var prop_id                       "Unique key for the project"
    label var app5b_net_increase_cash_yr_1  "App5B: Net Increase in Cash Held - Most Recent Year (Owner 1)"
    label var app5b_net_increase_cash_yr_2  "App5B: Net Increase in Cash Held - Most Recent Year (Owner 2)"
    label var app5b_net_increase_cash_yr_3  "App5B: Net Increase in Cash Held - Most Recent Year (Owner 3)"
    label var app5b_net_increase_cash_yr_4  "App5B: Net Increase in Cash Held - Most Recent Year (Owner 4)"
    label var app5b_net_increase_cash_yr_5  "App5B: Net Increase in Cash Held - Most Recent Year (Owner 5)"
    label var app5b_net_increase_cash_yr_6  "App5B: Net Increase in Cash Held - Most Recent Year (Owner 6)"
    label var app5b_net_increase_cash_yr_7  "App5B: Net Increase in Cash Held - Most Recent Year (Owner 7)"
    label var app5b_net_increase_cash_yr_8  "App5B: Net Increase in Cash Held - Most Recent Year (Owner 8)"
    label var app5b_net_increase_cash_qtr_1 "App5B: Net Increase in Cash Held - Most Recent Quarter (Owner 1)"
    label var app5b_net_increase_cash_qtr_2 "App5B: Net Increase in Cash Held - Most Recent Quarter (Owner 2)"
    label var app5b_net_increase_cash_qtr_3 "App5B: Net Increase in Cash Held - Most Recent Quarter (Owner 3)"
    label var app5b_net_increase_cash_qtr_4 "App5B: Net Increase in Cash Held - Most Recent Quarter (Owner 4)"
    label var app5b_net_increase_cash_qtr_5 "App5B: Net Increase in Cash Held - Most Recent Quarter (Owner 5)"
    label var app5b_net_increase_cash_qtr_6 "App5B: Net Increase in Cash Held - Most Recent Quarter (Owner 6)"
    label var app5b_net_increase_cash_qtr_7 "App5B: Net Increase in Cash Held - Most Recent Quarter (Owner 7)"
    label var app5b_net_increase_cash_qtr_8 "App5B: Net Increase in Cash Held - Most Recent Quarter (Owner 8)"
    label var app5b_est_cf_next_qtr_1       "App5B: Est Cash Outflow, Next Qtr (Owner 1)"
    label var app5b_est_cf_next_qtr_2       "App5B: Est Cash Outflow, Next Qtr (Owner 2)"
    label var app5b_est_cf_next_qtr_3       "App5B: Est Cash Outflow, Next Qtr (Owner 3)"
    label var app5b_est_cf_next_qtr_4       "App5B: Est Cash Outflow, Next Qtr (Owner 4)"
    label var app5b_est_cf_next_qtr_5       "App5B: Est Cash Outflow, Next Qtr (Owner 5)"
    label var app5b_est_cf_next_qtr_6       "App5B: Est Cash Outflow, Next Qtr (Owner 6)"
    label var app5b_est_cf_next_qtr_7       "App5B: Est Cash Outflow, Next Qtr (Owner 7)"
    label var app5b_est_cf_next_qtr_8       "App5B: Est Cash Outflow, Next Qtr (Owner 8)"

    save "$temp_prop_details/property_details_5_11.dta", replace
end
program combine_property_details_5_12
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_5_ownership_details_12_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  net_oper_rev_yr_1
    rename D  net_oper_rev_yr_2
    rename E  net_oper_rev_yr_3
    rename F  net_oper_rev_yr_4
    rename G  net_oper_rev_yr_5
    rename H  net_oper_rev_yr_6
    rename I  net_oper_rev_yr_7
    rename J  net_oper_rev_yr_8
    rename K  net_oper_rev_qtr_1
    rename L  net_oper_rev_qtr_2
    rename M  net_oper_rev_qtr_3
    rename N  net_oper_rev_qtr_4
    rename O  net_oper_rev_qtr_5
    rename P  net_oper_rev_qtr_6
    rename Q  net_oper_rev_qtr_7
    rename R  net_oper_rev_qtr_8
    rename S  net_oper_profit_tax_yr_1
    rename T  net_oper_profit_tax_yr_2
    rename U  net_oper_profit_tax_yr_3
    rename V  net_oper_profit_tax_yr_4
    rename W  net_oper_profit_tax_yr_5
    rename X  net_oper_profit_tax_yr_6
    rename Y  net_oper_profit_tax_yr_7
    rename Z  net_oper_profit_tax_yr_8
    rename AA net_oper_profit_tax_qtr_1
    rename AB net_oper_profit_tax_qtr_2
    rename AC net_oper_profit_tax_qtr_3
    rename AD net_oper_profit_tax_qtr_4
    rename AE net_oper_profit_tax_qtr_5
    rename AF net_oper_profit_tax_qtr_6
    rename AG net_oper_profit_tax_qtr_7
    rename AH net_oper_profit_tax_qtr_8

    * label variables
    label var prop_name                     "Name of the mine or facility"
    label var prop_id                       "Unique key for the project"
    label var net_oper_rev_yr_1             "Net Operating Revenue - Most Recent Year (Owner 1)"
    label var net_oper_rev_yr_2             "Net Operating Revenue - Most Recent Year (Owner 2)"
    label var net_oper_rev_yr_3             "Net Operating Revenue - Most Recent Year (Owner 3)"
    label var net_oper_rev_yr_4             "Net Operating Revenue - Most Recent Year (Owner 4)"
    label var net_oper_rev_yr_5             "Net Operating Revenue - Most Recent Year (Owner 5)"
    label var net_oper_rev_yr_6             "Net Operating Revenue - Most Recent Year (Owner 6)"
    label var net_oper_rev_yr_7             "Net Operating Revenue - Most Recent Year (Owner 7)"
    label var net_oper_rev_yr_8             "Net Operating Revenue - Most Recent Year (Owner 8)"
    label var net_oper_rev_qtr_1            "Net Operating Revenue - Most Recent Quarter (Owner 1)"
    label var net_oper_rev_qtr_2            "Net Operating Revenue - Most Recent Quarter (Owner 2)"
    label var net_oper_rev_qtr_3            "Net Operating Revenue - Most Recent Quarter (Owner 3)"
    label var net_oper_rev_qtr_4            "Net Operating Revenue - Most Recent Quarter (Owner 4)"
    label var net_oper_rev_qtr_5            "Net Operating Revenue - Most Recent Quarter (Owner 5)"
    label var net_oper_rev_qtr_6            "Net Operating Revenue - Most Recent Quarter (Owner 6)"
    label var net_oper_rev_qtr_7            "Net Operating Revenue - Most Recent Quarter (Owner 7)"
    label var net_oper_rev_qtr_8            "Net Operating Revenue - Most Recent Quarter (Owner 8)"
    label var net_oper_profit_tax_yr_1      "Net Operating Profit After Tax - Most Recent Year (Owner 1)"
    label var net_oper_profit_tax_yr_2      "Net Operating Profit After Tax - Most Recent Year (Owner 2)"
    label var net_oper_profit_tax_yr_3      "Net Operating Profit After Tax - Most Recent Year (Owner 3)"
    label var net_oper_profit_tax_yr_4      "Net Operating Profit After Tax - Most Recent Year (Owner 4)"
    label var net_oper_profit_tax_yr_5      "Net Operating Profit After Tax - Most Recent Year (Owner 5)"
    label var net_oper_profit_tax_yr_6      "Net Operating Profit After Tax - Most Recent Year (Owner 6)"
    label var net_oper_profit_tax_yr_7      "Net Operating Profit After Tax - Most Recent Year (Owner 7)"
    label var net_oper_profit_tax_yr_8      "Net Operating Profit After Tax - Most Recent Year (Owner 8)"
    label var net_oper_profit_tax_qtr_1     "Net Operating Profit After Tax - Most Recent Quarter (Owner 1)"
    label var net_oper_profit_tax_qtr_2     "Net Operating Profit After Tax - Most Recent Quarter (Owner 2)"
    label var net_oper_profit_tax_qtr_3     "Net Operating Profit After Tax - Most Recent Quarter (Owner 3)"
    label var net_oper_profit_tax_qtr_4     "Net Operating Profit After Tax - Most Recent Quarter (Owner 4)"
    label var net_oper_profit_tax_qtr_5     "Net Operating Profit After Tax - Most Recent Quarter (Owner 5)"
    label var net_oper_profit_tax_qtr_6     "Net Operating Profit After Tax - Most Recent Quarter (Owner 6)"
    label var net_oper_profit_tax_qtr_7     "Net Operating Profit After Tax - Most Recent Quarter (Owner 7)"
    label var net_oper_profit_tax_qtr_8     "Net Operating Profit After Tax - Most Recent Quarter (Owner 8)"

    save "$temp_prop_details/property_details_5_12.dta", replace
end
program combine_property_details_5_13
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_5_ownership_details_13_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  rptd_ebitda_yr_1
    rename D  rptd_ebitda_yr_2
    rename E  rptd_ebitda_yr_3
    rename F  rptd_ebitda_yr_4
    rename G  rptd_ebitda_yr_5
    rename H  rptd_ebitda_yr_6
    rename I  rptd_ebitda_yr_7
    rename J  rptd_ebitda_yr_8
    rename K  rptd_ebitda_qtr_1
    rename L  rptd_ebitda_qtr_2
    rename M  rptd_ebitda_qtr_3
    rename N  rptd_ebitda_qtr_4
    rename O  rptd_ebitda_qtr_5
    rename P  rptd_ebitda_qtr_6
    rename Q  rptd_ebitda_qtr_7
    rename R  rptd_ebitda_qtr_8
    rename S  ebitda_yr_1
    rename T  ebitda_yr_2
    rename U  ebitda_yr_3
    rename V  ebitda_yr_4
    rename W  ebitda_yr_5
    rename X  ebitda_yr_6
    rename Y  ebitda_yr_7
    rename Z  ebitda_yr_8
    rename AA ebitda_qtr_1
    rename AB ebitda_qtr_2
    rename AC ebitda_qtr_3
    rename AD ebitda_qtr_4
    rename AE ebitda_qtr_5
    rename AF ebitda_qtr_6
    rename AG ebitda_qtr_7
    rename AH ebitda_qtr_8

    * label variables
    label var prop_name             "Name of the mine or facility"
    label var prop_id               "Unique key for the project"
    label var rptd_ebitda_yr_1      "Reported EBITDA - Most Recent Year (Owner 1)"
    label var rptd_ebitda_yr_2      "Reported EBITDA - Most Recent Year (Owner 2)"
    label var rptd_ebitda_yr_3      "Reported EBITDA - Most Recent Year (Owner 3)"
    label var rptd_ebitda_yr_4      "Reported EBITDA - Most Recent Year (Owner 4)"
    label var rptd_ebitda_yr_5      "Reported EBITDA - Most Recent Year (Owner 5)"
    label var rptd_ebitda_yr_6      "Reported EBITDA - Most Recent Year (Owner 6)"
    label var rptd_ebitda_yr_7      "Reported EBITDA - Most Recent Year (Owner 7)"
    label var rptd_ebitda_yr_8      "Reported EBITDA - Most Recent Year (Owner 8)"
    label var rptd_ebitda_qtr_1     "Reported EBITDA - Most Recent Quarter (Owner 1)"
    label var rptd_ebitda_qtr_2     "Reported EBITDA - Most Recent Quarter (Owner 2)"
    label var rptd_ebitda_qtr_3     "Reported EBITDA - Most Recent Quarter (Owner 3)"
    label var rptd_ebitda_qtr_4     "Reported EBITDA - Most Recent Quarter (Owner 4)"
    label var rptd_ebitda_qtr_5     "Reported EBITDA - Most Recent Quarter (Owner 5)"
    label var rptd_ebitda_qtr_6     "Reported EBITDA - Most Recent Quarter (Owner 6)"
    label var rptd_ebitda_qtr_7     "Reported EBITDA - Most Recent Quarter (Owner 7)"
    label var rptd_ebitda_qtr_8     "Reported EBITDA - Most Recent Quarter (Owner 8)"
    label var ebitda_yr_1           "EBITDA - Most Recent Year (Owner 1)"
    label var ebitda_yr_2           "EBITDA - Most Recent Year (Owner 2)"
    label var ebitda_yr_3           "EBITDA - Most Recent Year (Owner 3)"
    label var ebitda_yr_4           "EBITDA - Most Recent Year (Owner 4)"
    label var ebitda_yr_5           "EBITDA - Most Recent Year (Owner 5)"
    label var ebitda_yr_6           "EBITDA - Most Recent Year (Owner 6)"
    label var ebitda_yr_7           "EBITDA - Most Recent Year (Owner 7)"
    label var ebitda_yr_8           "EBITDA - Most Recent Year (Owner 8)"
    label var ebitda_qtr_1          "EBITDA - Most Recent Quarter (Owner 1)"
    label var ebitda_qtr_2          "EBITDA - Most Recent Quarter (Owner 2)"
    label var ebitda_qtr_3          "EBITDA - Most Recent Quarter (Owner 3)"
    label var ebitda_qtr_4          "EBITDA - Most Recent Quarter (Owner 4)"
    label var ebitda_qtr_5          "EBITDA - Most Recent Quarter (Owner 5)"
    label var ebitda_qtr_6          "EBITDA - Most Recent Quarter (Owner 6)"
    label var ebitda_qtr_7          "EBITDA - Most Recent Quarter (Owner 7)"
    label var ebitda_qtr_8          "EBITDA - Most Recent Quarter (Owner 8)"

    save "$temp_prop_details/property_details_5_13.dta", replace
end
program combine_property_details_5_14
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_5_ownership_details_14_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring C-Z, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  owner_ticker_1
    rename D  owner_ticker_2
    rename E  owner_ticker_3
    rename F  owner_ticker_4
    rename G  owner_ticker_5
    rename H  owner_ticker_6
    rename I  owner_ticker_7
    rename J  owner_ticker_8
    rename K  owner_exchange_1
    rename L  owner_exchange_2
    rename M  owner_exchange_3
    rename N  owner_exchange_4
    rename O  owner_exchange_5
    rename P  owner_exchange_6
    rename Q  owner_exchange_7
    rename R  owner_exchange_8
    rename S  owner_trading_symbol_exchange_1
    rename T  owner_trading_symbol_exchange_2
    rename U  owner_trading_symbol_exchange_3
    rename V  owner_trading_symbol_exchange_4
    rename W  owner_trading_symbol_exchange_5
    rename X  owner_trading_symbol_exchange_6
    rename Y  owner_trading_symbol_exchange_7
    rename Z  owner_trading_symbol_exchange_8

    * label variables
    label var prop_name                     "Name of the mine or facility"
    label var prop_id                       "Unique key for the project"
    label var owner_ticker_1                "Ticker (Primary) (Owner 1)"
    label var owner_ticker_2                "Ticker (Primary) (Owner 2)"
    label var owner_ticker_3                "Ticker (Primary) (Owner 3)"
    label var owner_ticker_4                "Ticker (Primary) (Owner 4)"
    label var owner_ticker_5                "Ticker (Primary) (Owner 5)"
    label var owner_ticker_6                "Ticker (Primary) (Owner 6)"
    label var owner_ticker_7                "Ticker (Primary) (Owner 7)"
    label var owner_ticker_8                "Ticker (Primary) (Owner 8)"
    label var owner_exchange_1              "Exchange (Primary) (Owner 1)"
    label var owner_exchange_2              "Exchange (Primary) (Owner 2)"
    label var owner_exchange_3              "Exchange (Primary) (Owner 3)"
    label var owner_exchange_4              "Exchange (Primary) (Owner 4)"
    label var owner_exchange_5              "Exchange (Primary) (Owner 5)"
    label var owner_exchange_6              "Exchange (Primary) (Owner 6)"
    label var owner_exchange_7              "Exchange (Primary) (Owner 7)"
    label var owner_exchange_8              "Exchange (Primary) (Owner 8)"
    label var owner_trading_symbol_exchange_1 "Trading Symbol & Exchange (Primary) (Owner 1)"
    label var owner_trading_symbol_exchange_2 "Trading Symbol & Exchange (Primary) (Owner 2)"
    label var owner_trading_symbol_exchange_3 "Trading Symbol & Exchange (Primary) (Owner 3)"
    label var owner_trading_symbol_exchange_4 "Trading Symbol & Exchange (Primary) (Owner 4)"
    label var owner_trading_symbol_exchange_5 "Trading Symbol & Exchange (Primary) (Owner 5)"
    label var owner_trading_symbol_exchange_6 "Trading Symbol & Exchange (Primary) (Owner 6)"
    label var owner_trading_symbol_exchange_7 "Trading Symbol & Exchange (Primary) (Owner 7)"
    label var owner_trading_symbol_exchange_8 "Trading Symbol & Exchange (Primary) (Owner 8)"

    save "$temp_prop_details/property_details_5_14.dta", replace
end
program combine_property_details_5_15
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_5_ownership_details_15_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring C-Z, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  owner_ticker_secondary_1
    rename D  owner_ticker_secondary_2
    rename E  owner_ticker_secondary_3
    rename F  owner_ticker_secondary_4
    rename G  owner_ticker_secondary_5
    rename H  owner_ticker_secondary_6
    rename I  owner_ticker_secondary_7
    rename J  owner_ticker_secondary_8
    rename K  owner_exchange_secondary_1
    rename L  owner_exchange_secondary_2
    rename M  owner_exchange_secondary_3
    rename N  owner_exchange_secondary_4
    rename O  owner_exchange_secondary_5
    rename P  owner_exchange_secondary_6
    rename Q  owner_exchange_secondary_7
    rename R  owner_exchange_secondary_8
    rename S  owner_trading_symbol_ex_second_1 // shortened to fit
    rename T  owner_trading_symbol_ex_second_2
    rename U  owner_trading_symbol_ex_second_3
    rename V  owner_trading_symbol_ex_second_4
    rename W  owner_trading_symbol_ex_second_5
    rename X  owner_trading_symbol_ex_second_6
    rename Y  owner_trading_symbol_ex_second_7
    rename Z  owner_trading_symbol_ex_second_8

    * label variables
    label var prop_name                             "Name of the mine or facility"
    label var prop_id                               "Unique key for the project"
    label var owner_ticker_secondary_1             "Ticker (Secondary) (Owner 1)"
    label var owner_ticker_secondary_2             "Ticker (Secondary) (Owner 2)"
    label var owner_ticker_secondary_3             "Ticker (Secondary) (Owner 3)"
    label var owner_ticker_secondary_4             "Ticker (Secondary) (Owner 4)"
    label var owner_ticker_secondary_5             "Ticker (Secondary) (Owner 5)"
    label var owner_ticker_secondary_6             "Ticker (Secondary) (Owner 6)"
    label var owner_ticker_secondary_7             "Ticker (Secondary) (Owner 7)"
    label var owner_ticker_secondary_8             "Ticker (Secondary) (Owner 8)"
    label var owner_exchange_secondary_1           "Exchange (Secondary) (Owner 1)"
    label var owner_exchange_secondary_2           "Exchange (Secondary) (Owner 2)"
    label var owner_exchange_secondary_3           "Exchange (Secondary) (Owner 3)"
    label var owner_exchange_secondary_4           "Exchange (Secondary) (Owner 4)"
    label var owner_exchange_secondary_5           "Exchange (Secondary) (Owner 5)"
    label var owner_exchange_secondary_6           "Exchange (Secondary) (Owner 6)"
    label var owner_exchange_secondary_7           "Exchange (Secondary) (Owner 7)"
    label var owner_exchange_secondary_8           "Exchange (Secondary) (Owner 8)"
    label var owner_trading_symbol_ex_second_1 "Trading Symbol & Exchange (Secondary) (Owner 1)"
    label var owner_trading_symbol_ex_second_2 "Trading Symbol & Exchange (Secondary) (Owner 2)"
    label var owner_trading_symbol_ex_second_3 "Trading Symbol & Exchange (Secondary) (Owner 3)"
    label var owner_trading_symbol_ex_second_4 "Trading Symbol & Exchange (Secondary) (Owner 4)"
    label var owner_trading_symbol_ex_second_5 "Trading Symbol & Exchange (Secondary) (Owner 5)"
    label var owner_trading_symbol_ex_second_6 "Trading Symbol & Exchange (Secondary) (Owner 6)"
    label var owner_trading_symbol_ex_second_7 "Trading Symbol & Exchange (Secondary) (Owner 7)"
    label var owner_trading_symbol_ex_second_8 "Trading Symbol & Exchange (Secondary) (Owner 8)"

    save "$temp_prop_details/property_details_5_15.dta", replace
end
program combine_property_details_6_1
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local years 2000/2022

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach year of numlist `years' {
        foreach region of local regions {
            local file_name "$input_property_details/property_details_6_historical_ownership_details_1_`year'_`region'.xls"
            if (fileexists("`file_name'")) {
                display "Processing: `file_name'"
                import excel "`file_name'", cellrange(A7) clear
                gen year = `year'
                tostring C-J S-AH, replace
                append using `temp_file'
                save `temp_file', replace
            }
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  historical_owner_name_1
    rename D  historical_owner_name_2
    rename E  historical_owner_name_3
    rename F  historical_owner_name_4
    rename G  historical_owner_name_5
    rename H  historical_owner_name_6
    rename I  historical_owner_name_7
    rename J  historical_owner_name_8
    rename K  historical_owner_snl_instn_key_1
    rename L  historical_owner_snl_instn_key_2
    rename M  historical_owner_snl_instn_key_3
    rename N  historical_owner_snl_instn_key_4
    rename O  historical_owner_snl_instn_key_5
    rename P  historical_owner_snl_instn_key_6
    rename Q  historical_owner_snl_instn_key_7
    rename R  historical_owner_snl_instn_key_8
    rename S  historical_owner_common_name_1
    rename T  historical_owner_common_name_2
    rename U  historical_owner_common_name_3
    rename V  historical_owner_common_name_4
    rename W  historical_owner_common_name_5
    rename X  historical_owner_common_name_6
    rename Y  historical_owner_common_name_7
    rename Z  historical_owner_common_name_8
    rename AA historical_owner_type_1
    rename AB historical_owner_type_2
    rename AC historical_owner_type_3
    rename AD historical_owner_type_4
    rename AE historical_owner_type_5
    rename AF historical_owner_type_6
    rename AG historical_owner_type_7
    rename AH historical_owner_type_8

    * label variables
    label var prop_name                     "Name of the mine or facility"
    label var prop_id                       "Unique key for the project"
    label var year                          "Year of historical ownership details"
    label var historical_owner_name_1      "Owner Name (Owner 1)"
    label var historical_owner_name_2      "Owner Name (Owner 2)"
    label var historical_owner_name_3      "Owner Name (Owner 3)"
    label var historical_owner_name_4      "Owner Name (Owner 4)"
    label var historical_owner_name_5      "Owner Name (Owner 5)"
    label var historical_owner_name_6      "Owner Name (Owner 6)"
    label var historical_owner_name_7      "Owner Name (Owner 7)"
    label var historical_owner_name_8      "Owner Name (Owner 8)"
    label var historical_owner_snl_instn_key_1 "Owner SNL Institution Key (Owner 1)"
    label var historical_owner_snl_instn_key_2 "Owner SNL Institution Key (Owner 2)"
    label var historical_owner_snl_instn_key_3 "Owner SNL Institution Key (Owner 3)"
    label var historical_owner_snl_instn_key_4 "Owner SNL Institution Key (Owner 4)"
    label var historical_owner_snl_instn_key_5 "Owner SNL Institution Key (Owner 5)"
    label var historical_owner_snl_instn_key_6 "Owner SNL Institution Key (Owner 6)"
    label var historical_owner_snl_instn_key_7 "Owner SNL Institution Key (Owner 7)"
    label var historical_owner_snl_instn_key_8 "Owner SNL Institution Key (Owner 8)"
    label var historical_owner_common_name_1 "Owner Common Name (Owner 1)"
    label var historical_owner_common_name_2 "Owner Common Name (Owner 2)"
    label var historical_owner_common_name_3 "Owner Common Name (Owner 3)"
    label var historical_owner_common_name_4 "Owner Common Name (Owner 4)"
    label var historical_owner_common_name_5 "Owner Common Name (Owner 5)"
    label var historical_owner_common_name_6 "Owner Common Name (Owner 6)"
    label var historical_owner_common_name_7 "Owner Common Name (Owner 7)"
    label var historical_owner_common_name_8 "Owner Common Name (Owner 8)"
    label var historical_owner_type_1      "Owner Type (Owner 1)"
    label var historical_owner_type_2      "Owner Type (Owner 2)"
    label var historical_owner_type_3      "Owner Type (Owner 3)"
    label var historical_owner_type_4      "Owner Type (Owner 4)"
    label var historical_owner_type_5      "Owner Type (Owner 5)"
    label var historical_owner_type_6      "Owner Type (Owner 6)"
    label var historical_owner_type_7      "Owner Type (Owner 7)"
    label var historical_owner_type_8      "Owner Type (Owner 8)"

    save "$temp_prop_details/property_details_6_1.dta", replace
end
program combine_property_details_6_2
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"
    local years 2000/2022

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach year of numlist `years' {
        foreach region of local regions {
            local file_name "$input_property_details/property_details_6_historical_ownership_details_2_`year'_`region'.xls"
            if (fileexists("`file_name'")) {
                display "Processing: `file_name'"
                import excel "`file_name'", cellrange(A7) clear
                gen year = `year'
                tostring S-AH, replace
                append using `temp_file'
                save `temp_file', replace
            }
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  historical_equity_own_pct_1
    rename D  historical_equity_own_pct_2
    rename E  historical_equity_own_pct_3
    rename F  historical_equity_own_pct_4
    rename G  historical_equity_own_pct_5
    rename H  historical_equity_own_pct_6
    rename I  historical_equity_own_pct_7
    rename J  historical_equity_own_pct_8
    rename K  historical_controlling_own_pct_1
    rename L  historical_controlling_own_pct_2
    rename M  historical_controlling_own_pct_3
    rename N  historical_controlling_own_pct_4
    rename O  historical_controlling_own_pct_5
    rename P  historical_controlling_own_pct_6
    rename Q  historical_controlling_own_pct_7
    rename R  historical_controlling_own_pct_8
    rename S  historical_owner_hq_1
    rename T  historical_owner_hq_2
    rename U  historical_owner_hq_3
    rename V  historical_owner_hq_4
    rename W  historical_owner_hq_5
    rename X  historical_owner_hq_6
    rename Y  historical_owner_hq_7
    rename Z  historical_owner_hq_8
    rename AA historical_owner_country_1
    rename AB historical_owner_country_2
    rename AC historical_owner_country_3
    rename AD historical_owner_country_4
    rename AE historical_owner_country_5
    rename AF historical_owner_country_6
    rename AG historical_owner_country_7
    rename AH historical_owner_country_8

    * label variables
    label var prop_name                     "Name of the mine or facility"
    label var prop_id                       "Unique key for the project"
    label var year                          "Year of historical ownership details"
    label var historical_equity_own_pct_1  "Historical Equity Ownership Percent (Owner 1)"
    label var historical_equity_own_pct_2  "Historical Equity Ownership Percent (Owner 2)"
    label var historical_equity_own_pct_3  "Historical Equity Ownership Percent (Owner 3)"
    label var historical_equity_own_pct_4  "Historical Equity Ownership Percent (Owner 4)"
    label var historical_equity_own_pct_5  "Historical Equity Ownership Percent (Owner 5)"
    label var historical_equity_own_pct_6  "Historical Equity Ownership Percent (Owner 6)"
    label var historical_equity_own_pct_7  "Historical Equity Ownership Percent (Owner 7)"
    label var historical_equity_own_pct_8  "Historical Equity Ownership Percent (Owner 8)"
    label var historical_controlling_own_pct_1 "Historical Controlling Ownership Percent (Owner 1)"
    label var historical_controlling_own_pct_2 "Historical Controlling Ownership Percent (Owner 2)"
    label var historical_controlling_own_pct_3 "Historical Controlling Ownership Percent (Owner 3)"
    label var historical_controlling_own_pct_4 "Historical Controlling Ownership Percent (Owner 4)"
    label var historical_controlling_own_pct_5 "Historical Controlling Ownership Percent (Owner 5)"
    label var historical_controlling_own_pct_6 "Historical Controlling Ownership Percent (Owner 6)"
    label var historical_controlling_own_pct_7 "Historical Controlling Ownership Percent (Owner 7)"
    label var historical_controlling_own_pct_8 "Historical Controlling Ownership Percent (Owner 8)"
    label var historical_owner_hq_1        "Owner HQ (Owner 1)"
    label var historical_owner_hq_2        "Owner HQ (Owner 2)"
    label var historical_owner_hq_3        "Owner HQ (Owner 3)"
    label var historical_owner_country_3   "Owner Country/Region (Owner 3)"
    label var historical_owner_country_4   "Owner Country/Region (Owner 4)"
    label var historical_owner_country_5   "Owner Country/Region (Owner 5)"
    label var historical_owner_country_6   "Owner Country/Region (Owner 6)"
    label var historical_owner_country_7   "Owner Country/Region (Owner 7)"
    label var historical_owner_country_8   "Owner Country/Region (Owner 8)"

    save "$temp_prop_details/property_details_6_2.dta", replace
end
program combine_property_details_7_1
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_7_royalty_details_1_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring C-L W-AF, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  royalty_name_1
    rename D  royalty_name_2
    rename E  royalty_name_3
    rename F  royalty_name_4
    rename G  royalty_name_5
    rename H  royalty_name_6
    rename I  royalty_name_7
    rename J  royalty_name_8
    rename K  royalty_name_9
    rename L  royalty_name_10
    rename M  royalty_snl_instn_key_1
    rename N  royalty_snl_instn_key_2
    rename O  royalty_snl_instn_key_3
    rename P  royalty_snl_instn_key_4
    rename Q  royalty_snl_instn_key_5
    rename R  royalty_snl_instn_key_6
    rename S  royalty_snl_instn_key_7
    rename T  royalty_snl_instn_key_8
    rename U  royalty_snl_instn_key_9
    rename V  royalty_snl_instn_key_10
    rename W  royalty_type_1
    rename X  royalty_type_2
    rename Y  royalty_type_3
    rename Z  royalty_type_4
    rename AA royalty_type_5
    rename AB royalty_type_6
    rename AC royalty_type_7
    rename AD royalty_type_8
    rename AE royalty_type_9
    rename AF royalty_type_10

    * label variables
    label var prop_name                 "Name of the mine or facility"
    label var prop_id                   "Unique key for the project"
    label var royalty_name_1            "Royalty Name (Royalty 1)"
    label var royalty_name_2            "Royalty Name (Royalty 2)"
    label var royalty_name_3            "Royalty Name (Royalty 3)"
    label var royalty_name_4            "Royalty Name (Royalty 4)"
    label var royalty_name_5            "Royalty Name (Royalty 5)"
    label var royalty_name_6            "Royalty Name (Royalty 6)"
    label var royalty_name_7            "Royalty Name (Royalty 7)"
    label var royalty_name_8            "Royalty Name (Royalty 8)"
    label var royalty_name_9            "Royalty Name (Royalty 9)"
    label var royalty_name_10           "Royalty Name (Royalty 10)"
    label var royalty_snl_instn_key_1   "Royalty SNL Institution Key (Royalty 1)"
    label var royalty_snl_instn_key_2   "Royalty SNL Institution Key (Royalty 2)"
    label var royalty_snl_instn_key_3   "Royalty SNL Institution Key (Royalty 3)"
    label var royalty_snl_instn_key_4   "Royalty SNL Institution Key (Royalty 4)"
    label var royalty_snl_instn_key_5   "Royalty SNL Institution Key (Royalty 5)"
    label var royalty_snl_instn_key_6   "Royalty SNL Institution Key (Royalty 6)"
    label var royalty_snl_instn_key_7   "Royalty SNL Institution Key (Royalty 7)"
    label var royalty_snl_instn_key_8   "Royalty SNL Institution Key (Royalty 8)"
    label var royalty_snl_instn_key_9   "Royalty SNL Institution Key (Royalty 9)"
    label var royalty_snl_instn_key_10  "Royalty SNL Institution Key (Royalty 10)"
    label var royalty_type_1            "Royalty Type (Royalty 1)"
    label var royalty_type_2            "Royalty Type (Royalty 2)"
    label var royalty_type_3            "Royalty Type (Royalty 3)"
    label var royalty_type_4            "Royalty Type (Royalty 4)"
    label var royalty_type_5            "Royalty Type (Royalty 5)"
    label var royalty_type_6            "Royalty Type (Royalty 6)"
    label var royalty_type_7            "Royalty Type (Royalty 7)"
    label var royalty_type_8            "Royalty Type (Royalty 8)"
    label var royalty_type_9            "Royalty Type (Royalty 9)"
    label var royalty_type_10           "Royalty Type (Royalty 10)"

    save "$temp_prop_details/property_details_7_1.dta", replace
end
program combine_property_details_7_2
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_7_royalty_details_2_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring M-AF, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  royalty_pct_1
    rename D  royalty_pct_2
    rename E  royalty_pct_3
    rename F  royalty_pct_4
    rename G  royalty_pct_5
    rename H  royalty_pct_6
    rename I  royalty_pct_7
    rename J  royalty_pct_8
    rename K  royalty_pct_9
    rename L  royalty_pct_10
    rename M  royalty_holder_hq_1
    rename N  royalty_holder_hq_2
    rename O  royalty_holder_hq_3
    rename P  royalty_holder_hq_4
    rename Q  royalty_holder_hq_5
    rename R  royalty_holder_hq_6
    rename S  royalty_holder_hq_7
    rename T  royalty_holder_hq_8
    rename U  royalty_holder_hq_9
    rename V  royalty_holder_hq_10
    rename W  royalty_holder_country_1
    rename X  royalty_holder_country_2
    rename Y  royalty_holder_country_3
    rename Z  royalty_holder_country_4
    rename AA royalty_holder_country_5
    rename AB royalty_holder_country_6
    rename AC royalty_holder_country_7
    rename AD royalty_holder_country_8
    rename AE royalty_holder_country_9
    rename AF royalty_holder_country_10

    * label variables
    label var prop_name                     "Name of the mine or facility"
    label var prop_id                       "Unique key for the project"
    label var royalty_pct_1                 "Royalty Percent (Royalty 1)"
    label var royalty_pct_2                 "Royalty Percent (Royalty 2)"
    label var royalty_pct_3                 "Royalty Percent (Royalty 3)"
    label var royalty_pct_4                 "Royalty Percent (Royalty 4)"
    label var royalty_pct_5                 "Royalty Percent (Royalty 5)"
    label var royalty_pct_6                 "Royalty Percent (Royalty 6)"
    label var royalty_pct_7                 "Royalty Percent (Royalty 7)"
    label var royalty_pct_8                 "Royalty Percent (Royalty 8)"
    label var royalty_pct_9                 "Royalty Percent (Royalty 9)"
    label var royalty_pct_10                "Royalty Percent (Royalty 10)"
    label var royalty_holder_hq_1           "Royalty Holder HQ (Royalty 1)"
    label var royalty_holder_hq_2           "Royalty Holder HQ (Royalty 2)"
    label var royalty_holder_hq_3           "Royalty Holder HQ (Royalty 3)"
    label var royalty_holder_hq_4           "Royalty Holder HQ (Royalty 4)"
    label var royalty_holder_hq_5           "Royalty Holder HQ (Royalty 5)"
    label var royalty_holder_hq_6           "Royalty Holder HQ (Royalty 6)"
    label var royalty_holder_hq_7           "Royalty Holder HQ (Royalty 7)"
    label var royalty_holder_hq_8           "Royalty Holder HQ (Royalty 8)"
    label var royalty_holder_hq_9           "Royalty Holder HQ (Royalty 9)"
    label var royalty_holder_hq_10          "Royalty Holder HQ (Royalty 10)"
    label var royalty_holder_country_1      "Royalty Holder Country/Region (Royalty 1)"
    label var royalty_holder_country_2      "Royalty Holder Country/Region (Royalty 2)"
    label var royalty_holder_country_3      "Royalty Holder Country/Region (Royalty 3)"
    label var royalty_holder_country_4      "Royalty Holder Country/Region (Royalty 4)"
    label var royalty_holder_country_5      "Royalty Holder Country/Region (Royalty 5)"
    label var royalty_holder_country_6      "Royalty Holder Country/Region (Royalty 6)"
    label var royalty_holder_country_7      "Royalty Holder Country/Region (Royalty 7)"
    label var royalty_holder_country_8      "Royalty Holder Country/Region (Royalty 8)"
    label var royalty_holder_country_9      "Royalty Holder Country/Region (Royalty 9)"
    label var royalty_holder_country_10     "Royalty Holder Country/Region (Royalty 10)"

    save "$temp_prop_details/property_details_7_2.dta", replace
end
program combine_property_details_7_3
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_7_royalty_details_3_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring C-AF, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  royalty_holder_global_region_1
    rename D  royalty_holder_global_region_2
    rename E  royalty_holder_global_region_3
    rename F  royalty_holder_global_region_4
    rename G  royalty_holder_global_region_5
    rename H  royalty_holder_global_region_6
    rename I  royalty_holder_global_region_7
    rename J  royalty_holder_global_region_8
    rename K  royalty_holder_global_region_9
    rename L  royalty_holder_global_region_10
    rename M  royalty_holder_phone_1
    rename N  royalty_holder_phone_2
    rename O  royalty_holder_phone_3
    rename P  royalty_holder_phone_4
    rename Q  royalty_holder_phone_5
    rename R  royalty_holder_phone_6
    rename S  royalty_holder_phone_7
    rename T  royalty_holder_phone_8
    rename U  royalty_holder_phone_9
    rename V  royalty_holder_phone_10
    rename W  royalty_holder_website_1
    rename X  royalty_holder_website_2
    rename Y  royalty_holder_website_3
    rename Z  royalty_holder_website_4
    rename AA royalty_holder_website_5
    rename AB royalty_holder_website_6
    rename AC royalty_holder_website_7
    rename AD royalty_holder_website_8
    rename AE royalty_holder_website_9
    rename AF royalty_holder_website_10

    * label variables
    label var prop_name                     "Name of the mine or facility"
    label var prop_id                       "Unique key for the project"
    label var royalty_holder_global_region_1 "Royalty Holder Global Region (Royalty 1)"
    label var royalty_holder_global_region_2 "Royalty Holder Global Region (Royalty 2)"
    label var royalty_holder_global_region_3 "Royalty Holder Global Region (Royalty 3)"
    label var royalty_holder_global_region_4 "Royalty Holder Global Region (Royalty 4)"
    label var royalty_holder_global_region_5 "Royalty Holder Global Region (Royalty 5)"
    label var royalty_holder_global_region_6 "Royalty Holder Global Region (Royalty 6)"
    label var royalty_holder_global_region_7 "Royalty Holder Global Region (Royalty 7)"
    label var royalty_holder_global_region_8 "Royalty Holder Global Region (Royalty 8)"
    label var royalty_holder_global_region_9 "Royalty Holder Global Region (Royalty 9)"
    label var royalty_holder_global_region_10 "Royalty Holder Global Region (Royalty 10)"
    label var royalty_holder_phone_1        "Royalty Holder Phone (Royalty 1)"
    label var royalty_holder_phone_2        "Royalty Holder Phone (Royalty 2)"
    label var royalty_holder_phone_3        "Royalty Holder Phone (Royalty 3)"
    label var royalty_holder_phone_4        "Royalty Holder Phone (Royalty 4)"
    label var royalty_holder_phone_5        "Royalty Holder Phone (Royalty 5)"
    label var royalty_holder_phone_6        "Royalty Holder Phone (Royalty 6)"
    label var royalty_holder_phone_7        "Royalty Holder Phone (Royalty 7)"
    label var royalty_holder_phone_8        "Royalty Holder Phone (Royalty 8)"
    label var royalty_holder_phone_9        "Royalty Holder Phone (Royalty 9)"
    label var royalty_holder_phone_10       "Royalty Holder Phone (Royalty 10)"
    label var royalty_holder_website_1      "Royalty Holder Website (Royalty 1)"
    label var royalty_holder_website_2      "Royalty Holder Website (Royalty 2)"
    label var royalty_holder_website_3      "Royalty Holder Website (Royalty 3)"
    label var royalty_holder_website_4      "Royalty Holder Website (Royalty 4)"
    label var royalty_holder_website_5      "Royalty Holder Website (Royalty 5)"
    label var royalty_holder_website_6      "Royalty Holder Website (Royalty 6)"
    label var royalty_holder_website_7      "Royalty Holder Website (Royalty 7)"
    label var royalty_holder_website_8      "Royalty Holder Website (Royalty 8)"
    label var royalty_holder_website_9      "Royalty Holder Website (Royalty 9)"
    label var royalty_holder_website_10     "Royalty Holder Website (Royalty 10)"

    save "$temp_prop_details/property_details_7_3.dta", replace
end
program combine_property_details_8
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_8_location_claims_comments_history_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring C-J, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    * rename variables
    rename A  prop_name
    rename B  prop_id
    rename C  location_comments
    rename D  locale
    rename E  district
    rename F  description_claim
    rename G  general_comments
    rename H  full_work_history
    rename I  enviro_comments
    rename J  subcontractors

    * label variables
    label var prop_name             "Name of the mine or facility"
    label var prop_id               "Unique key for the project"
    label var location_comments     "Property location description"
    label var locale                "General locale of a mining project"
    label var district              "A district describing location"
    label var description_claim     "Description of Claim"
    label var general_comments      "General Comments"
    label var full_work_history     "Full Work History"
    label var enviro_comments       "Environmental Comments"
    label var subcontractors        "Subcontractors"

    save "$temp_prop_details/property_details_8.dta", replace
end

program combine_property_details_9_1
    local regions "Africa LatinAmerica"
    local roles "Assaying Assessment Blasting_and_explosives Contract_mining Contract_processing Data_management Development Drilling Engineering_procurement_and_construction_management Environmental Expansion_Assessment Exploration Feasibility_study General Geophysics Geotechnical Grade_control_and_reconciliation Hydrological Independent_project_review Infrastructure_and_construction Metallurgical Mine_design_planning_and_engineering Mine_development Mining Mining_fleet Operator Optimization Prefeasibility_study Processing_design_planning_and_engineering Quality_assurance_and_quality_control Remediation Remote_location_support Resources_estimation_and_modeling Risk_management Scoping_study_and_preliminary_economic_assessment Social_impact Tailings_and_waste_management Transportation_and_shipping"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_9_contractor1_ID_name_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring AO-BZ, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name             "Name of the mine or facility"
    label var prop_id               "Unique key for the project"

    * contractor_id columns
    unab vars : C-AN
    local i = 1
    foreach oldname of local vars {
        local role : word `i' of `roles'
        local newname = "contractor_id_`=lower("`role'")'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Contractor ID (Role: `role')"

        local ++i
    }
    
    * contractor_name columns
    unab vars : AO-BZ
    local i = 1
    foreach oldname of local vars {
        local role : word `i' of `roles'
        local newname = "contractor_name_`=lower("`role'")'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Contractor name (Role: `role')"

        local ++i
    }

    save "$temp_prop_details/property_details_9_1.dta", replace
end

program combine_property_details_9_2
    local regions "Africa LatinAmerica"
    local roles "Assaying Assessment Blasting_and_explosives Contract_mining Contract_processing Data_management Development Drilling Engineering_procurement_and_construction_management Environmental Expansion_Assessment Exploration Feasibility_study General Geophysics Geotechnical Grade_control_and_reconciliation Hydrological Independent_project_review Infrastructure_and_construction Metallurgical Mine_design_planning_and_engineering Mine_development Mining Mining_fleet Operator Optimization Prefeasibility_study Processing_design_planning_and_engineering Quality_assurance_and_quality_control Remediation Remote_location_support Resources_estimation_and_modeling Risk_management Scoping_study_and_preliminary_economic_assessment Social_impact Tailings_and_waste_management Transportation_and_shipping"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_9_contractor1_HQ_verified_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring C-AN, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    * contractor_hq columns
    unab vars : C-AN
    local i = 1
    foreach oldname of local vars {
        local role : word `i' of `roles'
        local newname = "contractor_hq_`=lower("`role'")'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Contractor HQ country (`role')"

        local ++i
    }

    * contractor_last_verified columns
    unab vars : AO-BZ
    local i = 1
    foreach oldname of local vars {
        local role : word `i' of `roles'
        local newname = "contractor_verified_`=lower("`role'")'" // shortened to fit
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'

        format `shortname' %tdnn/dd/CCYY
        label var `shortname' "Contractor last verified date (`role')"
        local ++i
    }

    save "$temp_prop_details/property_details_9_2.dta", replace
end

program combine_property_details_9_3
    local regions "Africa LatinAmerica"
    local roles "Assaying Assessment Blasting_and_explosives Contract_mining Contract_processing Data_management Development Drilling Engineering_procurement_and_construction_management Environmental Expansion_Assessment Exploration Feasibility_study General Geophysics Geotechnical Grade_control_and_reconciliation Hydrological Independent_project_review Infrastructure_and_construction Metallurgical Mine_design_planning_and_engineering Mine_development Mining Mining_fleet Operator Optimization Prefeasibility_study Processing_design_planning_and_engineering Quality_assurance_and_quality_control Remediation Remote_location_support Resources_estimation_and_modeling Risk_management Scoping_study_and_preliminary_economic_assessment Social_impact Tailings_and_waste_management Transportation_and_shipping"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_9_contractor1_begin_`region'.xls"
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

    * contractor_begin_year columns
    unab vars : C-AN
    local i = 1
    foreach oldname of local vars {
        local role : word `i' of `roles'
        local newname = "begin_yr_`=lower("`role'")'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Contractor begin year (`role')"

        local ++i
    }

    * contractor_begin_month columns
    unab vars : AO-BZ
    local i = 1
    foreach oldname of local vars {
        local role : word `i' of `roles'
        local newname = "begin_mo_`=lower("`role'")'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Contractor begin month (`role')"

        local ++i
    }

    save "$temp_prop_details/property_details_9_3.dta", replace
end

program combine_property_details_9_4
    local regions "Africa LatinAmerica"
    local roles "Assaying Assessment Blasting_and_explosives Contract_mining Contract_processing Data_management Development Drilling Engineering_procurement_and_construction_management Environmental Expansion_Assessment Exploration Feasibility_study General Geophysics Geotechnical Grade_control_and_reconciliation Hydrological Independent_project_review Infrastructure_and_construction Metallurgical Mine_design_planning_and_engineering Mine_development Mining Mining_fleet Operator Optimization Prefeasibility_study Processing_design_planning_and_engineering Quality_assurance_and_quality_control Remediation Remote_location_support Resources_estimation_and_modeling Risk_management Scoping_study_and_preliminary_economic_assessment Social_impact Tailings_and_waste_management Transportation_and_shipping"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_9_contractor1_end_`region'.xls"
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

    * contractor_end_year columns
    unab vars : C-AN
    local i = 1
    foreach oldname of local vars {
        local role : word `i' of `roles'
        local newname = "end_yr_`=lower("`role'")'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Contractor end year (`role')"

        local ++i
    }

    * contractor_projected_end_month columns
    unab vars : AO-BZ
    local i = 1
    foreach oldname of local vars {
        local role : word `i' of `roles'
        local newname = "proj_end_mo_`=lower("`role'")'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Contractor projected end month (`role')"

        local ++i
    }

    save "$temp_prop_details/property_details_9_4.dta", replace
end

program combine_property_details_9_5
    local regions "Africa LatinAmerica"
    local roles "Assaying Assessment Blasting_and_explosives Contract_mining Contract_processing Data_management Development Drilling Engineering_procurement_and_construction_management Environmental Expansion_Assessment Exploration Feasibility_study General Geophysics Geotechnical Grade_control_and_reconciliation Hydrological Independent_project_review Infrastructure_and_construction Metallurgical Mine_design_planning_and_engineering Mine_development Mining Mining_fleet Operator Optimization Prefeasibility_study Processing_design_planning_and_engineering Quality_assurance_and_quality_control Remediation Remote_location_support Resources_estimation_and_modeling Risk_management Scoping_study_and_preliminary_economic_assessment Social_impact Tailings_and_waste_management Transportation_and_shipping"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_9_contractor1_projected_end_`region'.xls"
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

    * contractor_projected_end_year columns
    unab vars : C-AN
    local i = 1
    foreach oldname of local vars {
        local role : word `i' of `roles'
        local newname = "proj_end_yr_`=lower("`role'")'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Contractor projected end year (`role')"

        local ++i
    }

    * contractor_projected_end_month columns
    unab vars : AO-BZ
    local i = 1
    foreach oldname of local vars {
        local role : word `i' of `roles'
        local newname = "proj_end_mo_`=lower("`role'")'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Contractor projected end month (`role')"

        local ++i
    }

    save "$temp_prop_details/property_details_9_5.dta", replace
end

program combine_property_details_10_1
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_10_mine_econ_primary_power_source_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    * Rename variables for years 2023 to 1991
    local year = 2023
    foreach var of varlist C-AI {
        local newname = "mine_primary_power_source_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    * Add labels for renamed variables
    local year = 2023
    foreach var of varlist mine_primary_power_source_2023-mine_primary_power_source_1991 {
        label var `var' "Primary power source of a mine (`year')"
        local year = `year' - 1
    }

    save "$temp_prop_details/property_details_10_1.dta", replace
end

program combine_property_details_10_2
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_10_mine_econ_secondary_power_source_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring C-AI, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    * Rename variables for years 2023 to 1991
    local year = 2023
    foreach var of varlist C-AI {
        local newname = "mine_secondary_power_source_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    * Add labels for renamed variables
    local year = 2023
    foreach var of varlist mine_secondary_power_source_2023-mine_secondary_power_source_1991 {
        label var `var' "Secondary power source of a mine (`year')"
        local year = `year' - 1
    }

    save "$temp_prop_details/property_details_10_2.dta", replace
end

program combine_property_details_10_3
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_10_mine_econ_tertiary_power_source_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            tostring C-AI, replace
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A  prop_name
    rename B  prop_id

    * Rename variables for years 2023 to 1991
    local year = 2023
    foreach var of varlist C-AI {
        local newname = "mine_tertiary_power_source_`year'"
        rename `var' `newname'
        local year = `year' - 1
    }

    label var prop_name                "Name of the mine or facility"
    label var prop_id                  "Unique key for the project"

    * Add labels for renamed variables
    local year = 2023
    foreach var of varlist mine_tertiary_power_source_2023-mine_tertiary_power_source_1991 {
        label var `var' "Tertiary power source of a mine (`year')"
        local year = `year' - 1
    }

    save "$temp_prop_details/property_details_10_3.dta", replace
end

program combine_property_details_11_1
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_11_risk_scores_1_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            destring C-P, replace force
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A prop_name
    rename B prop_id

    label var prop_name             "Name of the mine or facility"
    label var prop_id               "Unique key for the project"

    * Rename variables for current and prior risk scores and levels
    local categories "political economic social technological environmental legal operational security"
    
    unab vars : C-R
    local i = 1

    foreach cat of local categories {
        local current : word `i' of `vars'
        local ++i
        local prior : word `i' of `vars'

        rename `current' current_score_`cat'
        label variable current_score_`cat' "Current country risk score (`cat')"

        rename `prior' prior_score_`cat'
        label variable prior_score_`cat' "Prior country risk score (`cat')"

        local ++i
    }

    unab vars : S-AD
    local i = 1

    foreach cat of local categories {
        local current : word `i' of `vars'
        local ++i
        local prior : word `i' of `vars'

        rename `current' current_level_`cat'
        label variable current_level_`cat' "Current country risk level (`cat')"

        rename `prior' prior_level_`cat'
        label variable prior_level_`cat' "Prior country risk level (`cat')"

        local ++i
    }


    save "$temp_prop_details/property_details_11_1.dta", replace
end


program combine_property_details_11_2
    local regions "AsiaPacific EuropeMiddleEast LatinAmerica USCanada Africa"

    clear
    tempname temp_file
    tempfile temp_file
    save `temp_file', emptyok

    foreach region of local regions {
        local file_name "$input_property_details/property_details_11_risk_scores_2_`region'.xls"
        if (fileexists("`file_name'")) {
            display "Processing: `file_name'"
            import excel "`file_name'", cellrange(A7) clear
            append using `temp_file'
            save `temp_file', replace
        }
    }

    rename A prop_name
    rename B prop_id

    label var prop_name "Name of the mine or facility"
    label var prop_id   "Unique key for the project"

    local categories "political economic social technological environmental legal operational security"

    * risk outlook
    unab vars : C-I
    local i = 1
    foreach oldname of local vars {
        local cat : word `i' of `categories'
        local newname = "risk_outlook_`=lower("`cat'")'"
        local shortname = substr("`newname'", 1, 32)

        rename `oldname' `shortname'
        label var `shortname' "Projection of country risk (`cat')"

        local ++i
    }

    * risk summary
    rename J current_risk_summary
    rename K prior_risk_summary

    label var current_risk_summary "Current country risk summary of qualitative factors"
    label var prior_risk_summary "Prior country risk summary of qualitative factors"

    * score as of date
    unab vars : L-Y
    local i = 1
    foreach cat of local categories {
        local current : word `i' of `vars'
        local ++i
        local prior : word `i' of `vars'

        rename `current' current_score_date_`cat'
        label variable current_score_date_`cat' "Date the current score was last updated (`cat')"

        rename `prior' prior_score_date_`cat'
        label variable prior_score_date_`cat' "Date the prior score was last updated (`cat')"

        local ++i
    }

    * summary as of date
    rename Z current_summary_date
    rename AA prior_summary_date

    label var current_summary_date "Date the current summary was last updated"
    label var prior_summary_date   "Date the prior summary was last updated"

    save "$temp_prop_details/property_details_11_2.dta", replace

end
