**** construct property level cross-section data ****
**** notes ****
* some string variables should be converted to numeric
* 
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

* intermediates
global temp_transactions "$dir_temp/temp_transactions"


************************************************************************
cd "$input_metals_mining/transactions"

program main
    combine_transactions_1
    combine_transactions_2
end

program combine_transactions_1
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
                local file general_transaction_details_1_general_info_`region'.xlsx

                // Check if file exists; if not, continue to next iteration
                capture confirm file "`file'"
                if _rc != 0 {
                    display "File `file' does not exist, skipping."
                    continue
                }
                display "Processing: `file'"

                // Read file1 main data (from row 7 down)
                import excel "`file'", cellrange(A7) clear
                //tostring A C-J, replace
                // rename variables
                rename A target_name
                rename B mi_transaction_id
                rename C buyer_target_name
                rename D snl_deal_key
                rename E snl_offering_key
                rename F ciq_transaction_id
                rename G target_id
                rename H buyers_investors
                rename I sellers
                rename J transaction_type
                rename K funding_type
                rename L ma_feature_type
                rename M buyback_features
                rename N offerings_feature_type
                rename O mi_target_primary_industry
                rename P target_industry_group
                rename Q second_level_primary_industry
                rename R iq_target_primary_industry
                rename S transaction_geography
                rename T ann_date
                rename U announced_agreement_date
                rename V def_agrmt_date
                rename W record_date
                rename X expect_competion_date_begin
                rename Y expect_competion_date_end
                rename Z closed_date
                rename AA cancelled_date
                rename AB suspended_date
                rename AC expired_plan_date
                rename AD status
                rename AE pe_involvement
                rename AF deal_summary
                rename AG security_desc
                rename AH currency_code
                rename AI transaction_value
                rename AJ desc_consideration
                rename AK sponsor_backed

                // Label vars
                label variable target_name "Name of the target or issuer for the transaction"
                label variable mi_transaction_id "Unique key to identify transactions"
                label variable buyer_target_name "Name of buyer and name of target"
                label variable snl_deal_key "Publishable key of the M&A dataset to assist in pulling relevant information."
                label variable snl_offering_key "Publishable key of the Capital Issues dataset to assist in pulling relevant information."
                label variable ciq_transaction_id "Publishable key for CIQ Transaction"
                label variable target_id "Key of the target or issuer for the transaction"
                label variable buyers_investors "Complete name of the institution, as specified in its charter"
                label variable sellers "Complete name of the institution, as specified in its charter"
                label variable transaction_type "Concatenation of a given transaction entity type and its transaction entity type parent"
                label variable funding_type "A funding type indicates the structure and liquidation preference of a security or other capital-raising instrument"
                label variable ma_feature_type "Feature describing the transaction structure"
                label variable buyback_features "Feature describing the transaction structure"
                label variable offerings_feature_type "Feature describing the transaction structure"
                label variable mi_target_primary_industry "Primary industry of the transaction target. Includes company and asset transactions."
                label variable target_industry_group "A grouping of companies based on common operating characteristics"
                label variable second_level_primary_industry "A grouping of companies based on common operating characteristics"
                label variable iq_target_primary_industry "A grouping of companies based on common operating characteristics in CIQ"
                label variable transaction_geography "Global region of the transaction target. Includes company and asset transactions. In multiple targets acquisition or unknown target location, it is the region of the seller or buyer."
                label variable ann_date "Date on which the transaction was announced"
                label variable announced_agreement_date "The date a transaction agreement was made public. If a deal is discovered after deal completion, the completion date is used as the announcement date"
                label variable def_agrmt_date "The date the deal event occurred"
                label variable record_date "The date, set by the distributing company, by which a distributing company shareholder must own shares in order to be eligible to receive the shares of the target company"
                label variable expect_competion_date_begin "First date on which the deal is expected to close"
                label variable expect_competion_date_end "Last date on which the deal is expected to close"
                label variable closed_date "Date on which the transaction was completed"
                label variable cancelled_date "Date on which the transaction was terminated"
                label variable suspended_date "The date on which the transaction was suspended"
                label variable expired_plan_date "The date on which the transaction was expired"
                label variable status "Milestones in a deal's progress"
                label variable pe_involvement "Indicates that a given deal, offering, funding or transaction has private equity involvement"
                label variable deal_summary "Summary of the transaction"
                label variable security_desc "A stand-alone description of the security, text to identify the different securities issued by a company, such as might be seen in the company's financial statements or cover of SEC filings"
                label variable currency_code "Alphabetic code used to identify currencies pursuant to ISO-4217"
                label variable transaction_value "Value of the transaction"
                label variable desc_consideration "Text description of a Deal's consideration"
                label variable sponsor_backed "Indicates the transaction is backed by a financial sponsor"

                // Append to the accumulating dataset
                append using `temp_all'
                save `temp_all', replace
            }

    save "$temp_transactions/transactions_1.dta", replace
end

program combine_transactions_1
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
                local file general_transaction_details_1_general_info_`region'.xlsx

                // Check if file exists; if not, continue to next iteration
                capture confirm file "`file'"
                if _rc != 0 {
                    display "File `file' does not exist, skipping."
                    continue
                }
                display "Processing: `file'"

                // Read file1 main data (from row 7 down)
                import excel "`file'", cellrange(A7) clear
                // rename variables
                rename A sptr_target_name
                rename B sptr_mi_transaction_id
                rename C sptr_buyer_target_name
                rename D sptr_snl_deal_key
                rename E sptr_snl_offering_key
                rename F sptr_ciq_transaction_id
                rename G sptr_target_id
                rename H sptr_buyers_investors
                rename I sptr_sellers
                rename J sptr_transaction_type
                rename K sptr_funding_type
                rename L sptr_ma_feature_type
                rename M sptr_buyback_features
                rename N sptr_offerings_feature_type
                rename O sptr_mi_target_primary_industry
                rename P sptr_target_industry_group
                rename Q sptr_2nd_level_primary_industry
                rename R sptr_iq_target_primary_industry
                rename S sptr_transaction_geography
                rename T sptr_ann_date
                rename U sptr_announced_agreement_date
                rename V sptr_def_agrmt_date
                rename W sptr_record_date
                rename X sptr_expect_competion_date_begin
                rename Y sptr_expect_competion_date_end
                rename Z sptr_closed_date
                rename AA sptr_cancelled_date
                rename AB sptr_suspended_date
                rename AC sptr_expired_plan_date
                rename AD sptr_status
                rename AE sptr_pe_involvement
                rename AF sptr_deal_summary
                rename AG sptr_security_desc
                rename AH sptr_currency_code
                rename AI sptr_transaction_value
                rename AJ sptr_desc_consideration
                rename AK sptr_sponsor_backed

                // Label vars
                label variable sptr_target_name "Name of the target or issuer for the transaction"
                label variable sptr_mi_transaction_id "Unique key to identify transactions"
                label variable sptr_buyer_target_name "Name of buyer and name of target"
                label variable sptr_snl_deal_key "Publishable key of the M&A dataset to assist in pulling relevant information."
                label variable sptr_snl_offering_key "Publishable key of the Capital Issues dataset to assist in pulling relevant information."
                label variable sptr_ciq_transaction_id "Publishable key for CIQ Transaction"
                label variable sptr_target_id "Key of the target or issuer for the transaction"
                label variable sptr_buyers_investors "Complete name of the institution, as specified in its charter"
                label variable sptr_sellers "Complete name of the institution, as specified in its charter"
                label variable sptr_transaction_type "Concatenation of a given transaction entity type and its transaction entity type parent"
                label variable sptr_funding_type "A funding type indicates the structure and liquidation preference of a security or other capital-raising instrument"
                label variable sptr_ma_feature_type "Feature describing the transaction structure"
                label variable sptr_buyback_features "Feature describing the transaction structure"
                label variable sptr_offerings_feature_type "Feature describing the transaction structure"
                label variable sptr_mi_target_primary_industry "Primary industry of the transaction target. Includes company and asset transactions."
                label variable sptr_target_industry_group "A grouping of companies based on common operating characteristics"
                label variable sptr_2nd_level_primary_industry "A grouping of companies based on common operating characteristics"
                label variable sptr_iq_target_primary_industry "A grouping of companies based on common operating characteristics in CIQ"
                label variable sptr_transaction_geography "Global region of the transaction target. Includes company and asset transactions. In multiple targets acquisition or unknown target location, it is the region of the seller or buyer."
                label variable sptr_ann_date "Date on which the transaction was announced"
                label variable sptr_announced_agreement_date "The date a transaction agreement was made public. If a deal is discovered after deal completion, the completion date is used as the announcement date"
                label variable sptr_def_agrmt_date "The date the deal event occurred"
                label variable sptr_record_date "The date, set by the distributing company, by which a distributing company shareholder must own shares in order to be eligible to receive the shares of the target company"
                label variable sptr_expect_competion_date_begin "First date on which the deal is expected to close"
                label variable sptr_expect_competion_date_end "Last date on which the deal is expected to close"
                label variable sptr_closed_date "Date on which the transaction was completed"
                label variable sptr_cancelled_date "Date on which the transaction was terminated"
                label variable sptr_suspended_date "The date on which the transaction was suspended"
                label variable sptr_expired_plan_date "The date on which the transaction was expired"
                label variable sptr_status "Milestones in a deal's progress"
                label variable sptr_pe_involvement "Indicates that a given deal, offering, funding or transaction has private equity involvement"
                label variable sptr_deal_summary "Summary of the transaction"
                label variable sptr_security_desc "A stand-alone description of the security, text to identify the different securities issued by a company, such as might be seen in the company's financial statements or cover of SEC filings"
                label variable sptr_currency_code "Alphabetic code used to identify currencies pursuant to ISO-4217"
                label variable sptr_transaction_value "Value of the transaction"
                label variable sptr_desc_consideration "Text description of a Deal's consideration"
                label variable sptr_sponsor_backed "Indicates the transaction is backed by a financial sponsor"

                // Append to the accumulating dataset
                append using `temp_all'
                save `temp_all', replace
            }

    save "$temp_transactions/transactions_1.dta", replace
end

program combine_transactions_2
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
                local file general_transaction_details_2_company_info_`region'.xlsx

                // Check if file exists; if not, continue to next iteration
                capture confirm file "`file'"
                if _rc != 0 {
                    display "File `file' does not exist, skipping."
                    continue
                }
                display "Processing: `file'"

                // Read file1 main data (from row 7 down)
                import excel "`file'", cellrange(A7) clear
                drop K
                if "`region'" != "LatinAmerica" {
                    drop AE // K is same as A (sptr_target_name), AE is region
                }
                //tostring A C-J, replace
                // rename variables
                rename A sptr_target_name
                rename B sptr_mi_transaction_id
                rename C sptr_buyer_name
                rename D sptr_buyer_id
                rename E sptr_buyer_ticker
                rename F sptr_buyer_exchange
                rename G sptr_acquirer_name
                rename H sptr_acquirer_ticker
                rename I sptr_acquirer_exchange
                rename J sptr_acquirer_country
                //rename K sptr_target_name
                rename L sptr_target_id
                rename M sptr_target_ticker
                rename N sptr_target_exchange
                rename O sptr_target_entity_type
                rename P sptr_target_state
                rename Q sptr_target_province
                rename R sptr_target_country
                rename S sptr_target_current_finl
                rename T sptr_target_announcement_finl
                rename U sptr_target_business_description
                rename V sptr_target_long_business_descri
                rename W sptr_seller_name
                rename X sptr_seller_id
                rename Y sptr_seller_ticker
                rename Z sptr_seller_exchange
                rename AA sptr_seller_city
                rename AB sptr_seller_state
                rename AC sptr_seller_us_region
                rename AD sptr_seller_country
                //rename AE transaction_geography

                // Label vars
                label variable sptr_target_name "Name of the target or issuer for the transaction"
                label variable sptr_mi_transaction_id "Unique key to identify transactions"
                label variable sptr_buyer_name "Name of the Buyer or Investor for the transaction"
                label variable sptr_buyer_id "Key of the buyer or investor for the transaction"
                label variable sptr_buyer_ticker "Name of the pricing entity"
                label variable sptr_buyer_exchange "Abbreviated name of the securities exchange or OTC Tier"
                label variable sptr_acquirer_name "Name of the acquirer"
                label variable sptr_acquirer_ticker "Ticker/trading symbol of the acquirer"
                label variable sptr_acquirer_exchange "Abbreviated name of the acquirer exchange"
                label variable sptr_acquirer_country "Common English name of acquirer country"
                label variable sptr_target_id "Key of the target or issuer for the transaction"
                label variable sptr_target_ticker "Ticker/trading symbol of the target or issuer"
                label variable sptr_target_exchange "Name of the target/issuer exchange"
                label variable sptr_target_entity_type "Deal Entity Domain"
                label variable sptr_target_state "State postal code (U.S. FIPS 5-2)"
                label variable sptr_target_province "Province/major political subdivision (non-U.S.)"
                label variable sptr_target_country "Common English name of target country"
                label variable sptr_target_current_finl "Target company has current financial data"
                label variable sptr_target_announcement_finl "Target has transaction announcement financials"
                label variable sptr_target_business_description "Target/Issuer business description"
                label variable sptr_target_long_business_descri "Target/Issuer long business description"
                label variable sptr_seller_name "Name of the Seller for the transaction"
                label variable sptr_seller_id "Seller institution key"
                label variable sptr_seller_ticker "Name of the pricing entity (seller)"
                label variable sptr_seller_exchange "Abbreviated name of the seller exchange"
                label variable sptr_seller_city "Seller city"
                label variable sptr_seller_state "Seller state postal code (U.S. FIPS 5-2)"
                label variable sptr_seller_us_region "Seller U.S. region"
                label variable sptr_seller_country "Common English name of seller country"
                //label variable transaction_geography "Global region of the transaction target. Includes company and asset transactions. In multiple targets acquisition or unknown target location, it is the region of the seller or buyer"
                // Append to the accumulating dataset
                append using `temp_all'
                save `temp_all', replace
            }

    save "$temp_transactions/transactions_2.dta", replace
end

program combine_transactions_3
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
                local file general_transaction_details_3_company_financials_buyer_`region'.xlsx

                // Check if file exists; if not, continue to next iteration
                capture confirm file "`file'"
                if _rc != 0 {
                    display "File `file' does not exist, skipping."
                    continue
                }
                display "Processing: `file'"

                // Read file1 main data (from row 7 down)
                import excel "`file'", cellrange(A7) clear
                //tostring A C-J, replace
                // rename variables
                rename A sptr_target_name
                rename B sptr_mi_transaction_id
                rename C sptr_buyer_ciq_period_end
                rename D sptr_buyer_ciq_currency_financ
                rename E sptr_buyer_ciq_total_rev
                rename F sptr_buyer_ciq_ebitda
                rename G sptr_buyer_ciq_ebit
                rename H sptr_buyer_ciq_net_income
                rename I sptr_buyer_ciq_earnings
                rename J sptr_buyer_ciq_eps
                rename K sptr_buyer_ciq_total_assets
                rename L sptr_buyer_ciq_total_debt
                rename M sptr_buyer_ciq_net_debt
                rename N sptr_buyer_ciq_total_equity
                rename O sptr_buyer_ciq_total_preferred
                rename P sptr_buyer_ciq_cash_sti
                rename Q sptr_buyer_ciq_minority_interest
                rename R sptr_buyer_period_end
                rename S sptr_buyer_currency_financials
                rename T sptr_buyer_total_rev
                rename U sptr_buyer_ebitda
                rename V sptr_buyer_ebit
                rename W sptr_buyer_ni
                rename X sptr_buyer_eps
                rename Y sptr_buyer_total_assets
                rename Z sptr_buyer_total_debt
                rename AA sptr_buyer_total_equity

                // Label vars
                label variable sptr_target_name "Name of the target or issuer for the transaction"
                label variable sptr_mi_transaction_id "Unique key to identify transactions"
                label variable sptr_buyer_ciq_period_end "Date on which the fiscal period ended for the buyer in CIQ transaction"
                label variable sptr_buyer_ciq_currency_financ "Alphabetic code used to identify currencies pursuant to ISO-4217 (CIQ)"
                label variable sptr_buyer_ciq_total_rev "Total revenue of the buyer in the transaction (CIQ)"
                label variable sptr_buyer_ciq_ebitda "EBITDA incl. EQ income from affiliates for the buyer in the transaction (CIQ)"
                label variable sptr_buyer_ciq_ebit "EBIT incl. EQ income from affiliates for the buyer in the transaction (CIQ)"
                label variable sptr_buyer_ciq_net_income "Net income to common excl. extra items for the buyer in the transaction (CIQ)"
                label variable sptr_buyer_ciq_earnings "Earnings from continuing operations before tax for the buyer in the transaction (CIQ)"
                label variable sptr_buyer_ciq_eps "Diluted EPS excl. extra items for the buyer in the transaction (CIQ)"
                label variable sptr_buyer_ciq_total_assets "Total assets for the buyer in the transaction (CIQ)"
                label variable sptr_buyer_ciq_total_debt "Total debt for the buyer in the transaction (CIQ)"
                label variable sptr_buyer_ciq_net_debt "Net debt for the buyer in the transaction (CIQ)"
                label variable sptr_buyer_ciq_total_equity "Total common equity for the buyer in the transaction (CIQ)"
                label variable sptr_buyer_ciq_total_preferred "Total preferred equity for the buyer in the transaction (CIQ)"
                label variable sptr_buyer_ciq_cash_sti "Cash and short term investments for the buyer in the transaction (CIQ)"
                label variable sptr_buyer_ciq_minority_interest "Total minority interest for the buyer in the transaction (CIQ)"
                label variable sptr_buyer_period_end "Date on which the fiscal period ended for the buyer in the transaction (SNL)"
                label variable sptr_buyer_currency_financials "Alphabetic code used to identify currencies pursuant to ISO-4217 (SNL)"
                label variable sptr_buyer_total_rev "Total revenue for the buyer in the transaction (SNL)"
                label variable sptr_buyer_ebitda "EBITDA for the buyer in the transaction (SNL)"
                label variable sptr_buyer_ebit "EBIT for the buyer in the transaction (SNL)"
                label variable sptr_buyer_ni "Net income for the buyer in the transaction (SNL)"
                label variable sptr_buyer_eps "Diluted EPS after extra for the buyer in the transaction (SNL)"
                label variable sptr_buyer_total_assets "Total assets for the buyer in the transaction (SNL)"
                label variable sptr_buyer_total_debt "Total debt for the buyer in the transaction (SNL)"
                label variable sptr_buyer_total_equity "Total equity for the buyer in the transaction (SNL)"
                // Append to the accumulating dataset
                append using `temp_all'
                save `temp_all', replace
            }

    drop AB // same as region
    save "$temp_transactions/transactions_3.dta", replace
end

program combine_transactions_4
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
                local file general_transaction_details_3_company_financials_target_issuer_`region'.xlsx

                // Check if file exists; if not, continue to next iteration
                capture confirm file "`file'"
                if _rc != 0 {
                    display "File `file' does not exist, skipping."
                    continue
                }
                display "Processing: `file'"

                // Read file1 main data (from row 7 down)
                import excel "`file'", cellrange(A7) clear
                //tostring A C-J, replace
                // rename variables
                rename A sptr_target_name
                rename B sptr_mi_transaction_id
                rename C sptr_target_ciq_period_end
                rename D sptr_target_ciq_currency_financ
                rename E sptr_target_ciq_total_rev
                rename F sptr_target_ciq_ebitda
                rename G sptr_target_ciq_ebit
                rename H sptr_target_ciq_net_income
                rename I sptr_target_ciq_earnings
                rename J sptr_target_ciq_eps
                rename K sptr_target_ciq_total_assets
                rename L sptr_target_ciq_total_debt
                rename M sptr_target_ciq_net_debt
                rename N sptr_target_ciq_total_equity
                rename O sptr_target_ciq_total_preferred
                rename P sptr_target_ciq_cash_sti
                rename Q sptr_target_ciq_minority_interes
                rename R sptr_target_marketcap
                rename S sptr_target_tev
                rename T sptr_target_current_tev
                rename U sptr_target_period_end
                rename V sptr_target_currency_financials
                rename W sptr_target_total_rev
                rename X sptr_target_ebitda
                rename Y sptr_target_ebit
                rename Z sptr_target_ni
                rename AA sptr_target_eps
                rename AB sptr_target_total_assets
                rename AC sptr_target_total_debt
                rename AD sptr_target_total_equity

                // Label vars
                label variable sptr_target_name "Name of the target or issuer for the transaction"
                label variable sptr_mi_transaction_id "Unique key to identify transactions"
                label variable sptr_target_ciq_period_end "Date on which the fiscal period ended for the target in CIQ transaction"
                label variable sptr_target_ciq_currency_financ "Alphabetic code used to identify currencies pursuant to ISO-4217"
                label variable sptr_target_ciq_total_rev "Total revenue of the target in the transaction"
                label variable sptr_target_ciq_ebitda "EBITDA incl. EQ income from affiliates for the target in the transaction"
                label variable sptr_target_ciq_ebit "EBIT incl. EQ income from affiliates for the target in the transaction"
                label variable sptr_target_ciq_net_income "Net income to common excl. extra items for the target in the transaction"
                label variable sptr_target_ciq_earnings "Earnings from continuing operations before tax for the target in the transaction"
                label variable sptr_target_ciq_eps "Diluted EPS excl. extra items for the target in the transaction"
                label variable sptr_target_ciq_total_assets "Total assets for the target in the transaction"
                label variable sptr_target_ciq_total_debt "Total debt for the target in the transaction"
                label variable sptr_target_ciq_net_debt "Net debt for the target in the transaction"
                label variable sptr_target_ciq_total_equity "Total common equity for the target in the transaction"
                label variable sptr_target_ciq_total_preferred "Total preferred equity for the target in the transaction"
                label variable sptr_target_ciq_cash_sti "Cash and short term investments for the target in the transaction"
                label variable sptr_target_ciq_minority_interes "Total minority interest for the target in the transaction"
                label variable sptr_target_marketcap "Target's Market Capitalization at transaction date"
                label variable sptr_target_tev "Target's Total Enterprise Value at transaction date"
                label variable sptr_target_current_tev "Target's current Total Enterprise Value"
                //variables U-AV were not listed in the word document, so no labels defined
                
                append using `temp_all'
                save `temp_all', replace
            }

    drop AE // same as region
    save "$temp_transactions/transactions_4.dta", replace
end

program combine_transactions_5
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
                local file M_A_deals_terms_values_market_intelligence_1_`region'.xlsx

                // Check if file exists; if not, continue to next iteration
                capture confirm file "`file'"
                if _rc != 0 {
                    display "File `file' does not exist, skipping."
                    continue
                }
                display "Processing: `file'"

                // Read file1 main data (from row 7 down)
                import excel "`file'", cellrange(A7) clear
                //tostring A C-J, replace
                // rename variables
                rename A sptr_target_name
                rename B sptr_mi_transaction_id
                rename C sptr_deal_value
                rename D sptr_deal_value_rptd
                rename E sptr_deal_value_per_share
                rename F sptr_deal_value_per_share_rptd
                rename G sptr_deal_transaction_value
                rename H sptr_deal_transaction_value_rptd
                rename I sptr_total_consid_sh
                rename J sptr_implied_eq
                rename K sptr_implied_ev
                rename L sptr_gross_transaction_value
                rename M sptr_net_debt_assumed
                rename N sptr_gross_debt_assumed
                rename O sptr_cash_consideration
                rename P sptr_equity_consideration
                rename Q sptr_preferred_consideration
                rename R sptr_debt_consideration
                rename S sptr_derivatives_consideration
                rename T sptr_assets_consideration
                rename U sptr_unclassified_consideration
                rename V sptr_cash_percent
                rename W sptr_equity_percent
                rename X sptr_preferred_percent
                rename Y sptr_debt_percent
                rename Z sptr_derivatives_percent

                // Label vars
                label variable sptr_target_name "Name of the target or issuer for the transaction"
                label variable sptr_mi_transaction_id "Unique key to identify transactions"
                label variable sptr_deal_value "Total consideration accrued to the sellers. Includes only the price paid for equity, not assumption of any obligations of the entity sold. For Spin-off/ Split-offs, this field indicates distribution value of the transaction."
                label variable sptr_deal_value_rptd "Total consideration accrued to the sellers. Includes only the price paid for equity, not assumption of any obligations of the entity sold. As reported."
                label variable sptr_deal_value_per_share "Total consideration paid to the sellers for the equity of the company, on a per-share basis"
                label variable sptr_deal_value_per_share_rptd "Total consideration paid to the sellers for the equity of the company, on a per-share basis, as reported by the participants"
                label variable sptr_deal_transaction_value "Deal value paid for equity, plus the value of assumed current liabilities, net of current assets"
                label variable sptr_deal_transaction_value_rptd "Deal value paid for equity, plus the value of any assumed long-term liabilities, net of any current assets assumed, as reported by the participants"
                label variable sptr_total_consid_sh "The sum of all merger considerations received by common shareholders of the target company"
                label variable sptr_implied_eq "Implied value of the target’s common equity if 100% of its outstanding equity were to be purchased"
                label variable sptr_implied_ev "Implied enterprise value of the target if 100% of its outstanding equity were to be purchased"
                label variable sptr_gross_transaction_value "Gross transaction value paid by the buyer for the merger target, without considering the net effects of any excess cash or short-term securities on the target’s balance sheet"
                label variable sptr_net_debt_assumed "Total net debt assumed less the value of convertible securities which were converted and included in deal valuation, plus minority interest"
                label variable sptr_gross_debt_assumed "Total debt assumed less the value of convertible securities which were converted and included in deal valuation, plus minority interest"
                label variable sptr_cash_consideration "Aggregate value for cash consideration paid by buyer for target securities"
                label variable sptr_equity_consideration "Aggregate value for common equity consideration paid by buyer for target securities"
                label variable sptr_preferred_consideration "Aggregate value for preferred equity consideration paid by buyer for target securities"
                label variable sptr_debt_consideration "Aggregate value for debt consideration paid by buyer for target securities"
                label variable sptr_derivatives_consideration "Aggregate value for equity derivatives consideration paid by buyer for target securities"
                label variable sptr_assets_consideration "Aggregate value for assets consideration paid by buyer for target securities"
                label variable sptr_unclassified_consideration "Aggregate value for unknown consideration paid by buyer for target securities"
                label variable sptr_cash_percent "Value of cash consideration paid by buyer to the shareholders as a percent of total consideration paid to shareholders"
                label variable sptr_equity_percent "Value of stock consideration paid by buyer to the shareholders as a percent of total consideration paid to shareholders"
                label variable sptr_preferred_percent "Value of preferred consideration paid by buyer to the shareholders as a percent of total consideration paid to shareholders"
                label variable sptr_debt_percent "Value of debt consideration paid by buyer to the shareholders as a percent of total consideration paid to shareholders"
                label variable sptr_derivatives_percent "Value of equity derivative consideration paid by buyer to the shareholders as a percent of total consideration paid to shareholders"
                //variables AA onwards were not listed in the word document, so not defined
                
                append using `temp_all'
                save `temp_all', replace
            }

    drop AA-AY
    save "$temp_transactions/transactions_5.dta", replace
end

program combine_transactions_6
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
                local file M_A_deals_terms_values_market_intelligence_2_`region'.xlsx

                // Check if file exists; if not, continue to next iteration
                capture confirm file "`file'"
                if _rc != 0 {
                    display "File `file' does not exist, skipping."
                    continue
                }
                display "Processing: `file'"

                // Read file1 main data (from row 7 down)
                import excel "`file'", cellrange(A7) clear
                //tostring A C-J, replace
                // rename variables
                rename A sptr_target_name
                label variable sptr_target_name "Name of the target or issuer for the transaction"
                rename B sptr_mi_transaction_id
                label variable sptr_mi_transaction_id "Unique key to identify transactions"
                rename C sptr_eq_cons_ann
                label variable sptr_eq_cons_ann "Aggregate value for target's common equity acquired by the buyer (Announcement)"
                rename D sptr_eq_cons_comp
                label variable sptr_eq_cons_comp "Aggregate value for target's common equity acquired by the buyer (Completion)"
                rename E sptr_pref_cons_ann
                label variable sptr_pref_cons_ann "Aggregate value for target's preferred equity acquired by the buyer (Announcement)"
                rename F sptr_pref_cons_comp
                label variable sptr_pref_cons_comp "Aggregate value for target's preferred equity acquired by the buyer (Completion)"
                rename G sptr_debt_cons_ann
                label variable sptr_debt_cons_ann "Aggregate value for target's equity debt acquired by the buyer (Announcement)"
                rename H sptr_debt_cons_comp
                label variable sptr_debt_cons_comp "Aggregate value for target's equity debt acquired by the buyer (Completion)"
                rename I sptr_deriv_cons_ann
                label variable sptr_deriv_cons_ann "Aggregate value for target's equity derivatives acquired by the buyer (Announcement)"
                rename J sptr_deriv_cons_comp
                label variable sptr_deriv_cons_comp "Aggregate value for target's equity derivatives acquired by the buyer (Completion)"
                rename K sptr_asset_cons_ann
                label variable sptr_asset_cons_ann "Aggregate value for target's assets acquired by the buyer (Announcement)"
                rename L sptr_asset_cons_comp
                label variable sptr_asset_cons_comp "Aggregate value for target's assets acquired by the buyer (Completion)"
                rename M sptr_unclass_cons_ann
                label variable sptr_unclass_cons_ann "Aggregate value for target's unclassified acquired by the buyer (Announcement)"
                rename N sptr_unclass_cons_comp
                label variable sptr_unclass_cons_comp "Aggregate value for target's unclassified acquired by the buyer (Completion)"
                rename O sptr_affil_cons_ann
                label variable sptr_affil_cons_ann "Aggregate value for securities of third company related to the buyer being issued for target securities (Announcement)"
                rename P sptr_affil_cons_comp
                label variable sptr_affil_cons_comp "Aggregate value for securities of third company related to the buyer being issued for target securities (Completion)"
                rename Q sptr_cap_contrib_cons_ann
                label variable sptr_cap_contrib_cons_ann "Aggregate value for capital invested by the buyer in the newly issued shares of the target (Announcement)"
                rename R sptr_cap_contrib_cons_comp
                label variable sptr_cap_contrib_cons_comp "Aggregate value for capital invested by the buyer in the newly issued shares of the target (Completion)"
                rename S sptr_capex_cons_ann
                label variable sptr_capex_cons_ann "Aggregate value for expenditure incurred by buyer on the asset to upgrade and maintain it (Announcement)"
                rename T sptr_capex_cons_comp
                label variable sptr_capex_cons_comp "Aggregate value for expenditure incurred by buyer on the asset to upgrade and maintain it (Completion)"
                rename U sptr_conting_cons_ann
                label variable sptr_conting_cons_ann "Aggregate value for consideration to be paid by buyer is dependent on happening or non-happening of certain event (Announcement)"
                rename V sptr_conting_cons_comp
                label variable sptr_conting_cons_comp "Aggregate value for consideration to be paid by buyer is dependent on happening or non-happening of certain event (Completion)"
                rename W sptr_debt_repaid_cons_ann
                label variable sptr_debt_repaid_cons_ann "Aggregate value for consideration paid by buyer for target non-convertible securities (debt & preferred equity) (Announcement)"
                rename X sptr_debt_repaid_cons_comp
                label variable sptr_debt_repaid_cons_comp "Aggregate value for consideration paid by buyer for target non-convertible securities (debt & preferred equity) (Completion)"
                rename Y sptr_div_seller_cons_ann
                label variable sptr_div_seller_cons_ann "Aggregate value for dividend value paid by target to its shareholders (Announcement)"
                rename Z sptr_div_seller_cons_comp
                label variable sptr_div_seller_cons_comp "Aggregate value for dividend value paid by target to its shareholders (Completion)"
                rename AA sptr_noncont_cons_ann
                label variable sptr_noncont_cons_ann "Aggregate value for consideration paid more than 90 days after the deal completion (Announcement)"
                rename AB sptr_noncont_cons_comp
                label variable sptr_noncont_cons_comp "Aggregate value for consideration paid more than 90 days after the deal completion (Completion)"
                rename AC sptr_royalty_cons_ann
                label variable sptr_royalty_cons_ann "Aggregate value for royalty issued by buyer as a consideration for target (Announcement)"
                rename AD sptr_royalty_cons_comp
                label variable sptr_royalty_cons_comp "Aggregate value for royalty issued by buyer as a consideration for target (Completion)"
                rename AE sptr_pct_eq_acq_ann
                label variable sptr_pct_eq_acq_ann "Percent of equity acquired in a transaction. For Spin-off/ Split-offs, this field indicates the percentage stack distributed. (Announcement)"
                rename AF sptr_pct_eq_acq_comp
                label variable sptr_pct_eq_acq_comp "Percent of equity acquired in a transaction. For Spin-off/ Split-offs, this field indicates the percentage stack distributed. (Completion)"
                rename AG sptr_pct_owned_ann
                label variable sptr_pct_owned_ann "Percent of target equity owned by the buyer following the completion of the transaction (Announcement)"
                rename AH sptr_pct_owned_comp
                label variable sptr_pct_owned_comp "Percent of target equity owned by the buyer following the completion of the transaction (Completion)"
                rename AI sptr_deal_status
                label variable sptr_deal_status "Deal Event Type. Milestones reached in the timeline of a Deal, e.g., Announcement, Completion"
                rename AJ reported_currency_code
                label variable reported_currency_code "Alphabetic code used to identify currencies pursuant to ISO-4217"
                rename AK sptr_valuation_date_ann
                label variable sptr_valuation_date_ann "Date of the deal valuation. Valuation details, including stock prices and financial information, are taken as of this date. (Announcement)"
                rename AL sptr_valuation_date_comp
                label variable sptr_valuation_date_comp "Date of the deal valuation. Valuation details, including stock prices and financial information, are taken as of this date. (Completion)"
                rename AM sptr_chg_control_ann
                label variable sptr_chg_control_ann "Indicates that the transaction will result in a change in ownership for the target entity (Announcement)"
                rename AN sptr_chg_control_comp
                label variable sptr_chg_control_comp "Indicates that the transaction will result in a change in ownership for the target entity (Completion)"
                rename AO sptr_minority_int_acq
                label variable sptr_minority_int_acq "Indicates the acquisition is for less than 50 percent of the target's equity"
                rename AP sptr_deal_attitude
                label variable sptr_deal_attitude "Indicates the nature or attitude of the M&A transaction. It is the attitude or recommendation of the target company’s management or Board of Directors towards the transaction."
                rename AQ sptr_deal_approach
                label variable sptr_deal_approach "Describes how a transaction is initiated. A deal can be either target/seller initiated or buyer initiated and accordingly, deal approach can be solicited or unsolicited"
                rename AR sptr_deal_conditions
                label variable sptr_deal_conditions "The type of condition for a given transaction"
                rename AS sptr_deal_responses
                label variable sptr_deal_responses "The type of response for a given transaction"
                rename AT sptr_acct_method
                label variable sptr_acct_method "Method by which purchase is accounted"
                rename AU sptr_geo_mkt_expansion
                label variable sptr_geo_mkt_expansion "A classification of how the buyer will expand its operations geographically as a result of the transaction"
                rename AV sptr_sell_term_fee
                label variable sptr_sell_term_fee "The maximum amount payable by the seller to the buyer under certain circumstances, usually including termination of the agreement by the seller, to enter into a deal with a third party or otherwise"
                append using `temp_all'
                save `temp_all', replace
            }

    drop AW
    save "$temp_transactions/transactions_6.dta", replace
end

program combine_transactions_7
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
                local file M_A_deals_terms_values_SNL_1_`region'.xlsx

                // Check if file exists; if not, continue to next iteration
                capture confirm file "`file'"
                if _rc != 0 {
                    display "File `file' does not exist, skipping."
                    continue
                }
                display "Processing: `file'"

                // Read file1 main data (from row 7 down)
                import excel "`file'", cellrange(A7) clear
                //tostring A C-J, replace
                // rename variables
                rename A sptr_target_name
                label variable sptr_target_name "Name of the target or issuer for the transaction"
                rename B sptr_mi_transaction_id
                label variable sptr_mi_transaction_id "Unique key to identify transactions"
                rename C sptr_total_debt_announce
                label variable sptr_total_debt_announce "The aggregate unpaid principal balance owed under financial obligations to other parties, required to be paid by a specified date or on demand (Announcement)"
                rename D sptr_total_debt_complete
                label variable sptr_total_debt_complete "The aggregate unpaid principal balance owed under financial obligations to other parties, required to be paid by a specified date or on demand (Completion)"
                rename E sptr_st_curr_lt_debt_announce
                label variable sptr_st_curr_lt_debt_announce "Principal amount of debt payable within the next calendar year, including the current portion of long-term debt (Announcement)"
                rename F sptr_st_curr_lt_debt_complete
                label variable sptr_st_curr_lt_debt_complete "Principal amount of debt payable within the next calendar year, including the current portion of long-term debt (Completion)"
                rename G sptr_noncurr_lt_debt_announce
                label variable sptr_noncurr_lt_debt_announce "Principal debt due more than one calendar year after the period end (Announcement)"
                rename H sptr_noncurr_lt_debt_complete
                label variable sptr_noncurr_lt_debt_complete "Principal debt due more than one calendar year after the period end (Completion)"
                rename I sptr_cash_eq_announce
                label variable sptr_cash_eq_announce "Cash and cash equivalents such as money orders, coins, and checks. For banks, this includes central bank balances. (Announcement)"
                rename J sptr_cash_eq_complete
                label variable sptr_cash_eq_complete "Cash and cash equivalents such as money orders, coins, and checks. For banks, this includes central bank balances. (Completion)"
                rename K sptr_curr_invest_announce
                label variable sptr_curr_invest_announce "The portion of investment securities classified as current assets (Announcement)"
                rename L sptr_curr_invest_complete
                label variable sptr_curr_invest_complete "The portion of investment securities classified as current assets (Completion)"
                rename M sptr_curr_invent_announce
                label variable sptr_curr_invent_announce "Current portion of materials and finished goods held for future use, sale, or lease to another party, or to be used in the operations of the company. This may include non-current inventories for Home Builder and North American Real Estate companies. (Announcement)"
                rename N sptr_curr_invent_complete
                label variable sptr_curr_invent_complete "Current portion of materials and finished goods held for future use, sale, or lease to another party, or to be used in the operations of the company. This may include non-current inventories for Home Builder and North American Real Estate companies. (Completion)"
                rename O sptr_obl_cap_leases_announce
                label variable sptr_obl_cap_leases_announce "Obligations payable under capital leases or operating leases, unless reported combined, then capital leases and operating leases (Announcement)"
                rename P sptr_obl_cap_leases_complete
                label variable sptr_obl_cap_leases_complete "Obligations payable under capital leases or operating leases, unless reported combined, then capital leases and operating leases (Completion)"
                rename Q sptr_postret_benef_announce
                label variable sptr_postret_benef_announce "Estimated future cost of postretirement benefits pursuant to FAS 106 or other pension benefits not included in comprehensive income (Announcement)"
                rename R sptr_postret_benef_complete
                label variable sptr_postret_benef_complete "Estimated future cost of postretirement benefits pursuant to FAS 106 or other pension benefits not included in comprehensive income (Completion)"
                rename S sptr_nonconv_pref_red_announce
                label variable sptr_nonconv_pref_red_announce "Value of redemption of non-convertible preferred equity (Announcement)"
                rename T sptr_nonconv_pref_red_complete
                label variable sptr_nonconv_pref_red_complete "Value of redemption of non-convertible preferred equity (Completion)"
                rename U sptr_exch_ratio_c2c_announce
                label variable sptr_exch_ratio_c2c_announce "Number of shares of the buyer's common stock to be exchanged for each share of seller's common stock (Announcement)"
                rename V sptr_exch_ratio_c2c_complete
                label variable sptr_exch_ratio_c2c_complete "Number of shares of the buyer's common stock to be exchanged for each share of seller's common stock (Completion)"
                rename W sptr_exch_ratio_c2p_announce
                label variable sptr_exch_ratio_c2p_announce "Number of shares of the buyer's common stock to be exchanged for each share of seller's preferred stock (Announcement)"
                rename X sptr_exch_ratio_c2p_complete
                label variable sptr_exch_ratio_c2p_complete "Number of shares of the buyer's common stock to be exchanged for each share of seller's preferred stock (Completion)"
                rename Y sptr_exch_ratio_p2c_announce
                label variable sptr_exch_ratio_p2c_announce "Number of shares of the buyer's preferred stock to be exchanged for each share of seller's common stock (Announcement)"
                rename Z sptr_exch_ratio_p2c_complete
                label variable sptr_exch_ratio_p2c_complete "Number of shares of the buyer's preferred stock to be exchanged for each share of seller's common stock (Completion)"
                rename AA sptr_exch_ratio_p2p_announce
                label variable sptr_exch_ratio_p2p_announce "Number of shares of the buyer's preferred stock to be exchanged for each share of seller's preferred stock (Announcement)"
                rename AB sptr_exch_ratio_p2p_complete
                label variable sptr_exch_ratio_p2p_complete "Number of shares of the buyer's preferred stock to be exchanged for each share of seller's preferred stock (Completion)"
                rename AC sptr_in_money_opt_out_announce
                label variable sptr_in_money_opt_out_announce "Total in-the-money options to be converted as part of the transaction (Announcement)"
                rename AD sptr_in_money_opt_out_complete
                label variable sptr_in_money_opt_out_complete "Total in-the-money options to be converted as part of the transaction (Completion)"
                rename AE sptr_deal_val_shares_announce
                label variable sptr_deal_val_shares_announce "Shares outstanding for the target, used in the derivation of the deal value. For Real Estate deals this includes both common and OP units outstanding. (Announcement)"
                rename AF sptr_deal_val_shares_complete
                label variable sptr_deal_val_shares_complete "Shares outstanding for the target, used in the derivation of the deal value. For Real Estate deals this includes both common and OP units outstanding. (Completion)"
                rename AG sptr_cons_cash_announce
                label variable sptr_cons_cash_announce "Value of the consideration incurred by the buyers (Announcement)"
                rename AH sptr_cons_cash_complete
                label variable sptr_cons_cash_complete "Value of the consideration incurred by the buyers (Completion)"
                rename AI sptr_cons_common_stk_announce
                label variable sptr_cons_common_stk_announce "Value of the consideration incurred by the buyers (Announcement)"
                rename AJ sptr_cons_common_stk_complete
                label variable sptr_cons_common_stk_complete "Value of the consideration incurred by the buyers (Completion)"
                rename AK sptr_gross_proceeds
                label variable sptr_gross_proceeds "Aggregate price paid to the sellers in the transaction for the equity of the target, as of the event in question. Where available, this is calculated as the number of diluted shares outstanding, excluding any shares already owned by the buyer or otherwise unsold in the deal, multiplied by the deal value per share, less the aggregate strike price of any in-the-money common-stock options, plus the consideration paid for the target's preferred equity. Deal value excludes debt assumption, employee retention pools, and the assumption of any other obligations of the target."
                rename AL sptr_common_sh_issued_announce
                label variable sptr_common_sh_issued_announce "The number of securities issued as consideration to the seller (Announcement)"
                rename AM sptr_common_sh_issued_complete
                label variable sptr_common_sh_issued_complete "The number of securities issued as consideration to the seller (Completion)"
                rename AN sptr_cons_royalty_issued_ann
                label variable sptr_cons_royalty_issued_ann "Value of the consideration incurred by the buyers (Announcement)"
                rename AO sptr_cons_royalty_issued_comp
                label variable sptr_cons_royalty_issued_comp "Value of the consideration incurred by the buyers (Completion)"
                rename AP sptr_cons_capex_announce
                label variable sptr_cons_capex_announce "Value of the consideration incurred by the buyers (Announcement)"
                rename AQ sptr_cons_capex_complete
                label variable sptr_cons_capex_complete "Value of the consideration incurred by the buyers (Completion)"
                rename AR sptr_cons_pref_stk_announce
                label variable sptr_cons_pref_stk_announce "Value of the consideration incurred by the buyers (Announcement)"
                rename AS sptr_cons_pref_stk_complete
                label variable sptr_cons_pref_stk_complete "Value of the consideration incurred by the buyers (Completion)"
                rename AT sptr_cons_common_op_units_ann
                label variable sptr_cons_common_op_units_ann "Value of the consideration incurred by the buyers (Announcement)"
                rename AU sptr_cons_common_op_units_comp
                label variable sptr_cons_common_op_units_comp "Value of the consideration incurred by the buyers (Completion)"
                rename AV sptr_cons_pref_op_units_announce
                label variable sptr_cons_pref_op_units_announce "Value of the consideration incurred by the buyers (Announcement)"
                rename AW sptr_cons_pref_op_units_complete
                label variable sptr_cons_pref_op_units_complete "Value of the consideration incurred by the buyers (Completion)"
                rename AX sptr_pref_sh_issued_announce
                label variable sptr_pref_sh_issued_announce "The number of securities issued as consideration to the seller (Announcement)"
                rename AY sptr_pref_sh_issued_complete
                label variable sptr_pref_sh_issued_complete "The number of securities issued as consideration to the seller (Completion)"

                append using `temp_all'
                save `temp_all', replace
            }

    drop AZ
    save "$temp_transactions/transactions_7.dta", replace
end

program combine_transactions_8
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
                local file M_A_deals_terms_values_SNL_2_`region'.xlsx

                // Check if file exists; if not, continue to next iteration
                capture confirm file "`file'"
                if _rc != 0 {
                    display "File `file' does not exist, skipping."
                    continue
                }
                display "Processing: `file'"

                // Read file1 main data (from row 7 down)
                import excel "`file'", cellrange(A7) clear
                tostring AU, replace
                // rename variables
                rename A sptr_target_name
                label variable sptr_target_name "Name of the target or issuer for the transaction"
                rename B sptr_mi_transaction_id
                label variable sptr_mi_transaction_id "Unique key to identify transactions"

                rename C sptr_cons_debt_issued_announce
                label variable sptr_cons_debt_issued_announce "Value of the consideration incurred by the buyers (Announcement)"
                rename D sptr_cons_debt_issued_complete
                label variable sptr_cons_debt_issued_complete "Value of the consideration incurred by the buyers (Completion)"

                rename E sptr_cons_opt_announce
                label variable sptr_cons_opt_announce "Value of the consideration incurred by the buyers (Announcement)"
                rename F sptr_cons_opt_complete
                label variable sptr_cons_opt_complete "Value of the consideration incurred by the buyers (Completion)"

                rename G sptr_cons_unclass_announce
                label variable sptr_cons_unclass_announce "Value of the consideration incurred by the buyers (Announcement)"
                rename H sptr_cons_unclass_complete
                label variable sptr_cons_unclass_complete "Value of the consideration incurred by the buyers (Completion)"

                rename I sptr_cons_debt_assumed_announce
                label variable sptr_cons_debt_assumed_announce "Value of the consideration incurred by the buyers (Announcement)"
                rename J sptr_cons_debt_assumed_complete
                label variable sptr_cons_debt_assumed_complete "Value of the consideration incurred by the buyers (Completion)"

                rename K sptr_cons_earnout_announce
                label variable sptr_cons_earnout_announce "Value of the consideration incurred by the buyers (Announcement)"
                rename L sptr_cons_earnout_complete
                label variable sptr_cons_earnout_complete "Value of the consideration incurred by the buyers (Completion)"

                rename M sptr_cons_cap_infus_announce
                label variable sptr_cons_cap_infus_announce "Value of the consideration incurred by the buyers (Announcement)"
                rename N sptr_cons_cap_infus_complete
                label variable sptr_cons_cap_infus_complete "Value of the consideration incurred by the buyers (Completion)"

                rename O sptr_cons_div_tosel_announce
                label variable sptr_cons_div_tosel_announce "Value of the consideration incurred by the buyers (Announcement)"
                rename P sptr_cons_div_tosel_complete
                label variable sptr_cons_div_tosel_complete "Value of the consideration incurred by the buyers (Completion)"

                rename Q sptr_common_sh_issued_announce
                label variable sptr_common_sh_issued_announce "The number of securities issued as consideration to the seller (Announcement)"
                rename R sptr_common_sh_issued_complete
                label variable sptr_common_sh_issued_complete "The number of securities issued as consideration to the seller (Completion)"

                rename S sptr_opt_issued_announce
                label variable sptr_opt_issued_announce "The number of securities issued as consideration to the seller (Announcement)"
                rename T sptr_opt_issued_complete
                label variable sptr_opt_issued_complete "The number of securities issued as consideration to the seller (Completion)"

                rename U sptr_strkprc_in_money_opt_ann
                label variable sptr_strkprc_in_money_opt_ann "Weighted average strike price of options in the money that will be converted as part of the transaction (Announcement)"
                rename V sptr_strkprc_in_money_opt_comp
                label variable sptr_strkprc_in_money_opt_comp "Weighted average strike price of options in the money that will be converted as part of the transaction (Completion)"

                rename W sptr_conv_pref_eq_announce
                label variable sptr_conv_pref_eq_announce "Preferred equity convertible into common equity at maturity, or at the discretion of the issuer or owner (Announcement)"
                rename X sptr_conv_pref_eq_complete
                label variable sptr_conv_pref_eq_complete "Preferred equity convertible into common equity at maturity, or at the discretion of the issuer or owner (Completion)"

                rename Y sptr_sh_from_conv_pref_announce
                label variable sptr_sh_from_conv_pref_announce "Number of new shares issued by the buyer in exchange for preferred equity of the target (Announcement)"
                rename Z sptr_sh_from_conv_pref_complete
                label variable sptr_sh_from_conv_pref_complete "Number of new shares issued by the buyer in exchange for preferred equity of the target (Completion)"

                rename AA sptr_conv_debt_announce
                label variable sptr_conv_debt_announce "Debt convertible to equity of the issuer, either at maturity or at the option of the debtholder (Announcement)"
                rename AB sptr_conv_debt_complete
                label variable sptr_conv_debt_complete "Debt convertible to equity of the issuer, either at maturity or at the option of the debtholder (Completion)"

                rename AC sptr_sh_from_conv_debt_announce
                label variable sptr_sh_from_conv_debt_announce "Number of new shares issued by the buyer in exchange for debt of the target (Announcement)"
                rename AD sptr_sh_from_conv_debt_complete
                label variable sptr_sh_from_conv_debt_complete "Number of new shares issued by the buyer in exchange for debt of the target (Completion)"

                rename AE sptr_type_assets_cash
                label variable sptr_type_assets_cash "Indicates that cash is one of the types of assets transferred to the buyer as part of the deal"

                rename AF sptr_type_assets_loans
                label variable sptr_type_assets_loans "Indicates that loans are one of the types of assets transferred to the buyer as part of the deal"

                rename AG sptr_type_assets_mbs
                label variable sptr_type_assets_mbs "Indicates that MBS are one of the types of assets transferred to the buyer as part of the deal"

                rename AH sptr_core_deposits_lessjumbo
                label variable sptr_core_deposits_lessjumbo "Deposits, less all deposit accounts with balances over $100,000 and foreign deposits"
                
                rename AI sptr_coredep_intang_announce
                label variable sptr_coredep_intang_announce "Amount of core deposit intangibles created as a result of the transaction (Announcement)"
                rename AJ sptr_coredep_intang_complete
                label variable sptr_coredep_intang_complete "Amount of core deposit intangibles created as a result of the transaction (Completion)"

                rename AK sptr_coredep_amort_announce
                label variable sptr_coredep_amort_announce "Number of years over which the buyer will amortize the core deposit intangibles generated as a result of the transaction (Announcement)"
                rename AL sptr_coredep_amort_complete
                label variable sptr_coredep_amort_complete "Number of years over which the buyer will amortize the core deposit intangibles generated as a result of the transaction (Completion)"

                rename AM sptr_amort_meth_announce
                label variable sptr_amort_meth_announce "A method of amortization (Announcement)"
                rename AN sptr_amort_meth_complete
                label variable sptr_amort_meth_complete "A method of amortization (Completion)"

                rename AO sptr_oth_sec_liabilities
                label variable sptr_oth_sec_liabilities "Other secured liabilities assumed by the acquiring institution (Announcement)"
        
                rename AP sptr_dep_transfer
                label variable sptr_dep_transfer "Total deposits from customers. For US banks, this is the total deposits from customers and banks. (Announcement)"
               
                rename AQ sptr_dep_assumed
                label variable sptr_dep_assumed "Deposits transferred as part of the regulatory resolution. These deposits are generally all deposits less uninsured deposits less brokered deposits. (Announcement)"
                
                rename AR sptr_total_assets
                label variable sptr_total_assets "All assets owned by the company as of the date indicated, as carried on the balance sheet and defined under the indicated accounting principles"

                rename AS sptr_premium_paid
                label variable sptr_premium_paid "Franchise premium paid. Equal to the value of consideration paid for the common equity, net of the tangible common equity acquired. For branch acquisitions, the value paid in excess of deposits acquired."

                rename AT sptr_assets_sold
                label variable sptr_assets_sold "Of the total assets of a resolved institution, the amount that is sold to the buyer"

                rename AU sptr_royalty_type_acquired
                label variable sptr_royalty_type_acquired "Type of royalty"

                rename AV sptr_royalty_pct_acquired
                label variable sptr_royalty_pct_acquired "Royalty interest in target acquired in a transaction"

                append using `temp_all'
                save `temp_all', replace
            }

    //drop AZ
    save "$temp_transactions/transactions_8.dta", replace
end

program combine_transactions_9
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
                local file M_A_deals_ratios_multiples_SPIQ_metals_SNL_`region'.xlsx

                // Check if file exists; if not, continue to next iteration
                capture confirm file "`file'"
                if _rc != 0 {
                    display "File `file' does not exist, skipping."
                    continue
                }
                display "Processing: `file'"

                // Read file1 main data (from row 7 down)
                import excel "`file'", cellrange(A7) clear
                //tostring AU, replace
                // rename variables
                rename A sptr_target_name
                label variable sptr_target_name "Name of the target or issuer for the transaction"
                rename B sptr_mi_transaction_id
                label variable sptr_mi_transaction_id "Unique key to identify transactions"

                rename C sptr_ev_ebitda_ann
                label variable sptr_ev_ebitda_ann "Target's implied total enterprise value as a multiple of last-twelve-month EBITDA (Announcement)"

                rename D sptr_ev_ebitda_cmp
                label variable sptr_ev_ebitda_cmp "Target's implied total enterprise value as a multiple of last-twelve-month EBITDA (Completion)"

                rename E sptr_ev_rev_ann
                label variable sptr_ev_rev_ann "Target's implied total enterprise value as a multiple of last-twelve-month revenue (Announcement)"

                rename F sptr_ev_rev_cmp
                label variable sptr_ev_rev_cmp "Target's implied total enterprise value as a multiple of last-twelve-month revenue (Completion)"

                rename G sptr_ev_ebit_ann
                label variable sptr_ev_ebit_ann "Target's implied total enterprise value as a multiple of last-twelve-month EBIT (Announcement)"

                rename H sptr_ev_ebit_cmp
                label variable sptr_ev_ebit_cmp "Target's implied total enterprise value as a multiple of last-twelve-month EBIT (Completion)"

                rename I sptr_eq_ni_ltm_ann
                label variable sptr_eq_ni_ltm_ann "Target's implied equity value as a multiple of last-twelve-month net income (Announcement)"

                rename J sptr_eq_ni_ltm_cmp
                label variable sptr_eq_ni_ltm_cmp "Target's implied equity value as a multiple of last-twelve-month net income (Completion)"

                rename K sptr_eq_bv_ann
                label variable sptr_eq_bv_ann "Target's implied equity value as a multiple of book value (Announcement)"

                rename L sptr_eq_bv_cmp
                label variable sptr_eq_bv_cmp "Target's implied equity value as a multiple of book value (Completion)"

                rename M sptr_ev_rev_fwd_ann
                label variable sptr_ev_rev_fwd_ann "Implied enterprise value as a multiple of the next twelve month revenue estimate of the company (Announcement)"

                rename N sptr_ev_rev_fwd_cmp
                label variable sptr_ev_rev_fwd_cmp "Implied enterprise value as a multiple of the next twelve month revenue estimate of the company (Completion)"

                rename O sptr_ev_ebitda_fwd_ann
                label variable sptr_ev_ebitda_fwd_ann "Implied enterprise value as a multiple of the next twelve month earnings before interest, taxes, depreciation and amortization estimate of the company (Announcement)"

                rename P sptr_ev_ebitda_fwd_cmp
                label variable sptr_ev_ebitda_fwd_cmp "Implied enterprise value as a multiple of the next twelve month earnings before interest, taxes, depreciation and amortization estimate of the company (Completion)"

                rename Q sptr_ev_ebit_fwd_ann
                label variable sptr_ev_ebit_fwd_ann "Implied enterprise value as a multiple of the next twelve month earnings before interest and taxes estimate of the company (Announcement)"

                rename R sptr_ev_ebit_fwd_cmp
                label variable sptr_ev_ebit_fwd_cmp "Implied enterprise value as a multiple of the next twelve month earnings before interest and taxes estimate of the company (Completion)"

                rename S sptr_offer_pe_fwd_ann
                label variable sptr_offer_pe_fwd_ann "Offer per share as a multiple of the earnings per share estimate of the company (Announcement)"

                rename T sptr_offer_pe_fwd_cmp
                label variable sptr_offer_pe_fwd_cmp "Offer per share as a multiple of the earnings per share estimate of the company (Completion)"

                rename U sptr_offer_pbv_fwd_ann
                label variable sptr_offer_pbv_fwd_ann "Offer per share as a multiple of the next fiscal year's consensus estimated book value per share (Announcement)"

                rename V sptr_offer_pbv_fwd_cmp
                label variable sptr_offer_pbv_fwd_cmp "Offer per share as a multiple of the next fiscal year's consensus estimated book value per share (Completion)"

                rename W sptr_tv_reserves_ann
                label variable sptr_tv_reserves_ann "Transaction value as a percent of the value of the equivalent reserve metals acquired (Announcement)"

                rename X sptr_tv_reserves_cmp
                label variable sptr_tv_reserves_cmp "Transaction value as a percent of the value of the equivalent reserve metals acquired (Completion)"

                rename Y sptr_tv_resres_ann
                label variable sptr_tv_resres_ann "Transaction value as a percent of the value of the equivalent reserve and resource metals acquired (Announcement)"

                rename Z sptr_tv_resres_cmp
                label variable sptr_tv_resres_cmp "Transaction value as a percent of the value of the equivalent reserve and resource metals acquired (Completion)"

                
                append using `temp_all'
                save `temp_all', replace
            }

    drop AA
    save "$temp_transactions/transactions_9.dta", replace
end

program combine_transactions_10
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
                local file M_A_metals_mining_deals_acquired_info_dates_`region'.xlsx

                // Check if file exists; if not, continue to next iteration
                capture confirm file "`file'"
                if _rc != 0 {
                    display "File `file' does not exist, skipping."
                    continue
                }
                display "Processing: `file'"

                // Read file1 main data (from row 7 down)
                import excel "`file'", cellrange(A7) clear
                //tostring AU, replace
                // rename variables
                rename A sptr_target_name
                label variable sptr_target_name "Name of the target or issuer for the transaction"
                rename B sptr_mi_transaction_id
                label variable sptr_mi_transaction_id "Unique key to identify transactions"

                rename C  sptr_deal_prim_commodity
                label variable sptr_deal_prim_commodity "A type of valuable material that is extracted from the earth during the course of mining operations"

                rename D  sptr_earn_in
                label variable sptr_earn_in "Indicates that a company will agree to make an investment over a period of time in order to gain a portion of ownership. This may, but typically does not, happen at the same time that the company actually purchases an ownership stake outright."

                rename E  sptr_metal_streaming
                label variable sptr_metal_streaming "Indicates the purchase of all or part of the metals produced from mines"

                rename F  sptr_deal_commodities
                label variable sptr_deal_commodities "A type of valuable material that is extracted from the earth during the course of mining operations"

                rename G  sptr_prim_resv_acq_tonne
                label variable sptr_prim_resv_acq_tonne "The quantity of mineable product identified as reserves acquired"

                rename H  sptr_prim_resv_eqv_tonne
                label variable sptr_prim_resv_eqv_tonne "The quantity of all mineral products identified as reserves acquired, converted to the specified mineral"

                rename I  sptr_prim_resv_acq_oz
                label variable sptr_prim_resv_acq_oz "The quantity of mineable product identified as reserves acquired"

                rename J  sptr_prim_resv_eqv_oz
                label variable sptr_prim_resv_eqv_oz "The quantity of all mineral products identified as reserves acquired, converted to the specified mineral"

                rename K  sptr_prim_resv_acq_ct
                label variable sptr_prim_resv_acq_ct "The quantity of mineable product identified as reserves acquired"

                rename L  sptr_prim_resv_acq_lb
                label variable sptr_prim_resv_acq_lb "The quantity of mineable product identified as reserves acquired"

                rename M  sptr_prim_resv_eqv_lb
                label variable sptr_prim_resv_eqv_lb "The quantity of all mineral products identified as reserves acquired, converted to the specified mineral"

                rename N  sptr_prim_resres_acq_ton
                label variable sptr_prim_resres_acq_ton "The quantity of mineable product identified as reserves and resources acquired"

                rename O  sptr_prim_resres_eqv_ton
                label variable sptr_prim_resres_eqv_ton "The quantity of all mineral products identified as reserves and resources acquired, converted to the specified mineral"

                rename P  sptr_prim_resres_acq_oz
                label variable sptr_prim_resres_acq_oz "The quantity of mineable product identified as reserves and resources acquired"

                rename Q  sptr_prim_resres_eqv_oz
                label variable sptr_prim_resres_eqv_oz "The quantity of all mineral products identified as reserves and resources acquired, converted to the specified mineral"

                rename R  sptr_prim_resres_acq_ct
                label variable sptr_prim_resres_acq_ct "The quantity of mineable product identified as reserves and resources acquired"

                rename S  sptr_prim_resres_acq_lb
                label variable sptr_prim_resres_acq_lb "The quantity of mineable product identified as reserves and resources acquired"

                rename T  sptr_prim_resres_eqv_lb
                label variable sptr_prim_resres_eqv_lb "The quantity of all mineral products identified as reserves and resources acquired, converted to the specified mineral"

                rename U  sptr_total_resv_val
                label variable sptr_total_resv_val "The value of all reserve metals owned"

                rename V  sptr_total_resres_val
                label variable sptr_total_resres_val "The value of all reserve and resource metals owned"

                rename W  sptr_prop_acquired
                label variable sptr_prop_acquired "The name of the mine, or, if a stand-alone processing facility, the name of that facility"

                rename X  sptr_acq_prop_ids
                label variable sptr_acq_prop_ids "Key of mining project"

                rename Y  sptr_acq_prop_region
                label variable sptr_acq_prop_region "Name of the global region"

                rename Z  sptr_acq_prop_country
                label variable sptr_acq_prop_country "Common English name of a country"

                rename AA sptr_acq_prop_state
                label variable sptr_acq_prop_state "Common name of a country division"

                rename AB sptr_acq_dev_stages
                label variable sptr_acq_dev_stages "Development stages of a mining reserve; states in which a reserve goes through from exploration to production"

                rename AC sptr_acq_prim_commodity
                label variable sptr_acq_prim_commodity "A type of valuable material that is extracted from the earth during the course of mining operations"

                rename AD sptr_price_paid_ton_resv
                label variable sptr_price_paid_ton_resv "The price paid for the metal acquired in the transaction"

                rename AE sptr_price_paid_ton_eqv
                label variable sptr_price_paid_ton_eqv "Price paid for the equivalent metal acquired in the transaction"

                rename AF sptr_price_paid_ton_rr
                label variable sptr_price_paid_ton_rr "The price paid for the reserves and resouces acquired in the transaction"

                rename AG sptr_price_paid_ton_rr_eqv
                label variable sptr_price_paid_ton_rr_eqv "Price paid for the equivalent reserves and resources acquired in the transaction"

                rename AH sptr_price_paid_oz_resv
                label variable sptr_price_paid_oz_resv "The price paid for the metal acquired in the transaction"

                rename AI sptr_price_paid_oz_eqv
                label variable sptr_price_paid_oz_eqv "Price paid for the equivalent metal acquired in the transaction"

                rename AJ sptr_price_paid_oz_rr
                label variable sptr_price_paid_oz_rr "The price paid for the reserves and resouces acquired in the transaction"

                rename AK sptr_price_paid_oz_rr_eqv
                label variable sptr_price_paid_oz_rr_eqv "Price paid for the equivalent reserves and resources acquired in the transaction"

                rename AL sptr_price_paid_ct_resv
                label variable sptr_price_paid_ct_resv "The price paid for the metal acquired in the transaction"

                rename AM sptr_price_paid_ct_rr
                label variable sptr_price_paid_ct_rr "The price paid for the reserves and resouces acquired in the transaction"

                rename AN sptr_price_paid_lb_resv
                label variable sptr_price_paid_lb_resv "The price paid for the metal acquired in the transaction"

                rename AO sptr_price_paid_lb_eqv
                label variable sptr_price_paid_lb_eqv "Price paid for the equivalent metal acquired in the transaction"

                rename AP sptr_price_paid_lb_rr
                label variable sptr_price_paid_lb_rr "The price paid for the reserves and resouces acquired in the transaction"

                rename AQ sptr_price_paid_lb_rr_eqv
                label variable sptr_price_paid_lb_rr_eqv "Price paid for the equivalent reserves and resources acquired in the transaction"

                rename AR sptr_val_to_total_resv
                label variable sptr_val_to_total_resv "Deal value as a percent of the reserves acquired"

                rename AS sptr_val_to_total_resres
                label variable sptr_val_to_total_resres "Deal value as a percent of the value of the reserves and resources acquired"

                rename AT sptr_ann_date_ma
                label variable sptr_ann_date_ma "Date on which M&A Transaction was announced"

                rename AU sptr_closed_date_ma
                label variable sptr_closed_date_ma "Date on which M&A Transaction was completed"

                rename AV sptr_cancelled_date_ma
                label variable sptr_cancelled_date_ma "Date on which M&A Transaction was terminated"

                rename AW sptr_end_marker_dummy
                label variable sptr_end_marker_dummy "End marker or dummy variable (placeholder)"

                
                append using `temp_all'
                save `temp_all', replace
            }

    drop AX
    save "$temp_transactions/transactions_10.dta", replace
end

program combine_transactions_11
    clear all
    set more off

    local regions "Africa EmergingAsiaPacific China EmergingAsiaPacificOthers EuropeMiddleEast LatinAmerica"
    tempfile temp_all
    save `temp_all', emptyok

    foreach region of local regions {
                local file M_A_shareholder_value_`region'.xlsx

                // Check if file exists; if not, continue to next iteration
                capture confirm file "`file'"
                if _rc != 0 {
                    display "File `file' does not exist, skipping."
                    continue
                }
                display "Processing: `file'"

                // Read file1 main data (from row 7 down)
                import excel "`file'", cellrange(A7) clear
                //tostring AU, replace
                // rename variables
                rename A sptr_target_name
                label variable sptr_target_name "Name of the target or issuer for the transaction"
                rename B sptr_mi_transaction_id
                label variable sptr_mi_transaction_id "Unique key to identify transactions"

                rename C sptr_tbv_dilution
                label variable sptr_tbv_dilution "Dilution of tangible book value (TBV) that occurs as a result of a transaction. Negative value indicates accretion. Reported as of announcement date."

                rename D sptr_tbv_ex_aoci_dil
                label variable sptr_tbv_ex_aoci_dil "Dilution of tangible book value (TBV), excluding accumulated other comprehensive income (AOCI), that occurs as a result of a transaction. Negative value indicates accretion. Reported as of announcement date."

                rename E sptr_tbv_earnback
                label variable sptr_tbv_earnback "Years it takes to earn back the deal's tangible book value (TBV) dilution through incremental earnings. Reported as of announcement date."

                rename F sptr_tbv_ex_aoci_earn
                label variable sptr_tbv_ex_aoci_earn "Years it takes to earn back the deal's tangible book value (TBV) dilution, excluding accumulated other comprehensive income (AOCI), through incremental earnings. Reported as of announcement date."

                
                append using `temp_all'
                save `temp_all', replace
            }

    drop G
    save "$temp_transactions/transactions_11.dta", replace
end

    