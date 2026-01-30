# Data Center Siting Relative to Water Resources and Electricity Infrastructure in Texas Using Geospatial Methods

## Quick Links

## Overview 
Across water-stressed regions, there has been a significant rise in the siting of large and hyperscale data centers due to the comparative advantages in land costs and economic development incentives these areas sometimes offer. The accelerated demand for electricity generation and computing driven by corporate interests in artificial intelligence has heightened concerns about fresh water usage and energy consumption. As a result, these expansion projects have raised critical questions about the disparate environmental and social implications of data center siting patterns.

## Research Questions
1)  How do Texas data center water consumption patterns (WUE) vary spatially across ERCOT wind and solar generation zones,  and do facilities in water-stressed regions experience higher environmental burdens than their water-secure counterparts?
2)  Where do low water stress and strong grid coverage areas converge, if any, and how do underlying electricity generation fuel mix advantages align with data center siting patterns? 
3) To what extent are environmental costs from data center water consumption disproportionately concentrated in counties with elevated socioeconomic vulnerability indicators?

## Data 
* <ins>Data Center Locations and Attributes:</ins> Retrieved 382 of 392 data center location from [datacentermap.com](https://www.datacentermap.com/usa/texas/) using custom built python scraper. 
* <ins>Electricity Generating Power Plant Locations:</ins> Retrieved latitude and longitude coordinates using 'plantid' and 'plantName' as index to estimate geographic impact of  Fuel Mix (EIA 930) data within specific counties/regions and how this may correspond with data center siting once aggregated/preprocessed.
* <ins>Fuel Energy Mix Data:</ins> Sourced from Form EIA-923 Monthly Generation by Energy Source, filtered by Balancing Authority/Form Respondent and selected: ERCOT (Electric Reliability Council of Texas), EPE (El Paso Electric Company), SWPP (Southwest Power Pool for Southeastern Texas bordering Louisiana), and (TEX). 
* <ins>Weather Data:</ins> Retrieved hourly temperature, humidity, and precipitation, wet bulb temperature at coordinates retrieved for datacenter locations from July 1, 2022 through July 31, 2025 (37 month period).
* <ins>Transmission Line Data:</ins> Retrieved from [EIA Atlas ArcGIS retired data](https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Electric_Power_Transmission_Lines/FeatureServer/0/query).
* <ins>ERCOT Zone Data:</ins> Incorporated in order to join and standardize data from disparate sources across a common geographic unit (counties), and then perform spatial operations in order to perform analysis across distinct regulatory/regional entities in Texas.
  
After fetching data from Open-Meteo and the U.S.Energy Information Administration‘s public API services with Python’s `requests` library, the tidy datasets were then uploaded to PostgreSQL database for PostGIS spatial operations. Find more here: [Data Ingestion Workflow Notebook](https://github.com/shalini-k-das/tx-datacenters-geospatial-analysis/blob/main/notebooks/01_data_ingestion_pipeline.ipynb)

## Methods
In order to determine spatial dependence of key predictor variables (i.e. wet bulb temperatures, datacenter power capacity, or electricity generation totals by fuel type) and identify geographic clusters with similar infrastructural and socioeconomic profiles, I will be using using `geopandas`, `PySAL`, `esda`, and `sklearn` to perform:
* Moran’s I
* Local Indicators of Spatial Association (LISA)
* Geographically Weighted Regression (GWR)
* Spatial Hierarchical Clustering 

## Figures
*[Launch Interactive Map](https://shalini-k-das.github.io/tx-datacenters-geospatial-analysis/figures/tx_infrastructure_interactive.html)*

## Results and Outcomes

## Future Directions

## References
* Arzumanyan, M., Calzado, E. R., Lin, N., Bahadur, V., Das, J., Ko, T. L., & Koesterke, L. (2025). Geospatial suitability analysis for data center placement: A case study in Texas, USA. Sustainable Cities and Society, 131, 106687. doi:10.1016/j.scs.2025.106687
* Gupta, P. S., Hossen, M. R., Li, P., Ren, S., & Islam, M. A. (2024). A Dataset for Research on Water Sustainability. Proceedings of the 15th ACM International Conference on Future and Sustainable Energy Systems, 442–446. Presented at the Singapore, Singapore. doi:10.1145/3632775.3661962
* Jegham, N., Abdelatti, M., Elmoubarki, L., & Hendawi, A. (2025). How Hungry is AI? Benchmarking Energy, Water, and Carbon Footprint of LLM Inference. arXiv [Cs.CY]. Retrieved from
http://arxiv.org/abs/2505.09598
* Shumba, N., Tshekiso, O., Li, P., Fanti, G., & Ren, S. (2025). A Water Efficiency Dataset for African Data Centers. Proceedings of the 2025 ACM SIGCAS/SIGCHI Conference on Computing and Sustainable
Societies, 453–460. doi:10.1145/3715335.3735483
* Wang, L., Chen, D., Yao, M., & She, G. (2025). Spatial distribution and influencing factors of data centers in China: An empirical analysis based on the geodetector model. Energy
and Buildings, 336, 115588. doi:10.1016/j.enbuild.2025.115588
