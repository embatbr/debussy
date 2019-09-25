# Debussy

*Centralized Google Composer library for Dotz Big Data*

Debussy is a open source library to centralize code for Google Composer (Airflow) here in Dotz.


## Objective

This repository is a library of common code for Airflow/Google Composer, instead of repeating plugins across multiple projects.


## Installing

We are expecting to be able to install this library using PyPI. However, for now, we only use it in our internal systems. Therefore, the installation is made by following these steps, roughly:

- Clone this repository into a directory, sibling to your target;
- In your target project, create a directory *composer* containing the DAGs folder;
- Execute the  *deploy.sh* passing the target project as argument.


## Organization

In the project's root directory there is a directory named *debussy*. That is the library. Inside it there are the following Python modules and packages:

- **operators**:
    - **basic**: contains basic operators, mainly to record metadata (or intended to be);
    - **bigquery**: operators to deal with BigQuery, obviously;
    - **extractors**: operators to extract data (currently using only Dataflow);
    - **notification**: so far, notifies in Slack.
- **subdags**: functions to create specified subdags, given the proper parameters.
