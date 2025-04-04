**** construct property level cross-section data ****
**** notes ****
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


* roadmap
program main
    clean_property_details_1
    clean_property_details_2
    clean_property_details_3_1
    clean_property_details_3_2
    clean_property_details_4
end

**** property details ****
program clean_property_details_1
    cd "$input_metals_mining/properties_property_details"

    local property_details_1 "property_details_1_general_info_commodities_location_AsiaPacific.xls property_details_1_general_info_commodities_location_EuropeMiddleEast.xls property_details_1_general_info_commodities_location_LatinAmerica.xls property_details_1_general_info_commodities_location_USCanada.xls"

    import excel "property_details_1_general_info_commodities_location_Africa.xls", cellrange(A7) clear
    save "$dir_temp/property_details_1.dta", replace

    foreach file of local property_details_1 {
        display "Processing: `file'"
        import excel "`file'", cellrange(A7) clear
        append using "$dir_temp/property_details_1.dta"
        save "$dir_temp/property_details_1.dta", replace
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

    save "$dir_temp/property_details_1.dta", replace
end

program clean_property_details_2

    local property_details_2 "property_details_2_coal_AsiaPacific.xls property_details_2_coal_EuropeMiddleEast.xls property_details_2_coal_LatinAmerica.xls property_details_2_coal_USCanada.xls"

    import excel "property_details_2_coal_Africa.xls", cellrange(A7) clear
    keep A B C D E F G H I J K
    save "$dir_temp/property_details_2.dta", replace

    foreach file of local property_details_2 {
        display "Processing: `file'"
        import excel "`file'", cellrange(A7) clear
        keep A B C D E F G H I J K
        append using "$dir_temp/property_details_2.dta"
        save "$dir_temp/property_details_2.dta", replace
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

    save "$dir_temp/property_details_2.dta", replace
end

program clean_property_details_3_1

    local property_details_3_1 "property_details_3_operator_1_AsiaPacific.xls property_details_3_operator_1_EuropeMiddleEast.xls property_details_3_operator_1_LatinAmerica.xls property_details_3_operator_1_USCanada.xls"

    import excel "property_details_3_operator_1_Africa.xls", cellrange(A7) clear
    save "$dir_temp/property_details_3_1.dta", replace

    foreach file of local property_details_3_1 {
        display "Processing: `file'"
        import excel "`file'", cellrange(A7) clear
        append using "$dir_temp/property_details_3_1.dta"
        save "$dir_temp/property_details_3_1.dta", replace
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
    
    save "$dir_temp/property_details_3_1.dta", replace
end

program clean_property_details_3_2

    local property_details_3_2 "property_details_3_operator_2_AsiaPacific.xls property_details_3_operator_2_EuropeMiddleEast.xls property_details_3_operator_2_LatinAmerica.xls property_details_3_operator_2_USCanada.xls"

    import excel "property_details_3_operator_2_Africa.xls", cellrange(A7) clear
    save "$dir_temp/property_details_3_2.dta", replace

    foreach file of local property_details_3_2 {
        display "Processing: `file'"
        import excel "`file'", cellrange(A7) clear
        append using "$dir_temp/property_details_3_2.dta"
        save "$dir_temp/property_details_3_2.dta", replace
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

    save "$dir_temp/property_details_3_2.dta", replace
end



