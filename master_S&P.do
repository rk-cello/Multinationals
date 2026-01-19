*** Last modifier: Reina Kishida 20250810
* Although we define "globals" in each code, define them in each code. 
clear all
set more off 

*** REQUIRED PACKAGES
* Type "ssc install estout, replace" for the 1st time
********************************************************

cd ../clean
*run combine_claims.do
*run combine_drill_results.do
*run combine_financings.do
*run combine_mine_econ_modeled.do
*run combine_most_recent_transactions.do
*run combine_production.do
*run combine_property_details.do
*run combine_reserves_resources.do
*run combine_tech_geo.do
*run combine_top_drill_results.do
*run combine_transactions.do
*run time_invariant.do
*run time_variant.do 

cd ../build

  
cd ../analysis
