# Multinationals
Some cross-section data constructed from metals_mining data are not at the property level, and thus excluded from the property_level output folder.
These include...
* claims: claim owner level data linked to properties
* drill results: drill results level data linked to properties
* capital costs: capital cost level data linked to properties
* trasactions: transaction level
and are stored in ../data/raw_cleaned/S&P_cleaned

prop_id does NOT uniquely identify the observations in property_level_crosssection_data.dta
prop_name prop_id does
(same prop_id for different prop_name, reason unidentified...)
