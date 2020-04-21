# MediCollab

Insight Data Engineering</br>
New York 2020A</br>

## Introduction

[MediCollab](http://www.dataglobe.me) is a tool to explore Medicare provider networks and identify the best providers in a given area. 

#### How to Use
Simply enter the specialty you are interested in along with the zip code you want to search, and MediCollab will return a network graphic of those specialists and their connections to other specialists, along with a list of specialists in that zip code ranked by their network influence. The results include information on the average out of pocket cost for each provider and their quality scores.

Under the hood, every Medicare doctor is stored along with information on their relationships with other Medicare providers, which comprises a graph database. This allows you to run graph algorithms giving further insight into how each doctor fits into the broader network. Relationships between doctors are based on having patients in common.

![Screenshot of MediCollab Results](https://github.com/lfchesebrough/MediCollab/blob/master/graphscreenshot.png)

## Background

Health care in America is badly in need of reform and innovation due to ever rising costs uncorrelated with quality of care. MediCollab proposes a solution based on two key insights into the causes of health care's cost and quality connundrum. First, consumers are generally unable to make educated choices when seeking health services due to the absence of cost and quality information. Second, poor care coordination between providers leads to both higher costs and variable health outcomes. MediCollab aggregates information published by Medicare, the largest free and public information repository on health care in America, to provide key information on cost and quality of Medicare providers. A graph database is the perfect way to store this information because not only are we concerned with the details on individual doctors but with the network of providers that will work together to provide care. Relationships between providers play into the final product in three ways: 1) search results are ranked by their Page Rank algorithm score, a measure of network centrality/influence,  2) the average out of pocket cost of a doctor's connections is calculated and displayed, and 3) a visual graphic is provided to show doctors who see the same patients, with the idea that doctors with strong relationships will be more effective at coordinating care.

#### Medicare

Medicare, a federal health insurnace program provided to all Americans over age 65, is accepted by 93% of health care providers. In a fee for service system, the most common form of Medicare payment, when doctors see a patient they record each service provided and submit a claim for payment to Medicare. Medicare pays them based on agreed rates, generally lower than what private insurance companies pay.

## Pipeline
![Pipeline for MediCollab](https://github.com/lfchesebrough/MediCollab/blob/master/pipeline.png)

## Data Sources

The data for this project comes from two main sources: [DocGraph Hop Teaming](https://careset.com/docgraph-hop-teaming/) published by CareSet Systems and a few different [data sets published by Medicare](https://data.medicare.gov/).

The DocGraph data covers 2014-2017 and is based on raw Medicare claims data (which is expensive and requires a lot of paperwork to acquire) and includes every pair of Medicare providers that have at least 11 patients in common in a given year, showing how many patients they have in common and the average wait time between doctors.

The first Medicare data set is [Medicare Provider Utilization and Payments](https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Physician-and-Other-Supplier) which summarizes each provider's total billing for a year including how many patients they saw, how many different services they provide and how much they were paid by Medicare. The second is [Medicare Physician Compare](https://www.medicare.gov/physiciancompare/) which includes data on provider quality metrics, I primarily used this to get the MIPS (Medicare Incentive Payment System) score for each provider, a standardized metric that factors in clinical process improvements and patient population health outcomes.




 
