# Data-Challenge

This repo represents my solution to the [Data Engineering Challenge](https://drive.google.com/drive/folders/1rKXXaTugmIvzbGov-pNMiHGWxlELFENc) organized by [Nordeus](https://nordeus.com/)


## Table of Contents

1. [Initial Setup](#initial-setup)
2. [How To Run It](#how-to-run-it)
3. [Data Processing Pipeline](#data-processing-pipeline)


## Initial Setup
There are two options for setting up the work-environment:
* Using Anaconda 
    * Open Anaconda Prompt and navigate to the directory of this repo by using: ```cd PATH_TO_THIS_REPO```
    * Execute ``` conda env create -f environment.yml ``` This will set up an environment with all necessary dependencies. 
     * Activate previously created environment by executing: ``` conda activate nordeus_data_challenge```
* Using system-wide Python
    * Open Bash/Command Prompt/Power Shell and navigate to the directory of this repo by using: ```cd PATH_TO_THIS_REPO```
    * Run ```pip install requirements.txt```

Your work environment should be properly set up now.

## How To Run It
1. Navigate to the directory of this repo by using: ```cd PATH_TO_THIS_REPO```
2. Run ```python process_data.py DATASET_PATH```. This script loads the dataset and executes data cleaning pipeline explained [here](#data-processing-pipeline).
```
usage: process_data.py [-h] --dataset_path DATASET_PATH

Loads and cleans the dataset. Saves processed data to a database.

optional arguments:
  -h, --help            show this help message and exit

Required Arguments:
  --dataset_path DATASET_PATH
                        Path where the '.jsonl' dataset file is stored.
```
3. Run ```python main.py LEAGUE_ID```. This script retrieves and displays the desired league scoreboard.
```
usage: main.py [-h] --league_id LEAGUE_ID

Retrieves scoreboard for the desired league

optional arguments:
  -h, --help            show this help message and exit

Required arguments:
  --league_id LEAGUE_ID
                        Id of a league for which we want to get the
                        scoreboard.
```

## Data Processing Pipeline

After cleaning, the statistics related to club performance are stored in a database.

* In order to correctly retrieve the league scoreboard it was necessary to preprocess the dataset
* Dataset cleaning steps are explained in the diagram below
* Functions which make up the cleaning/processing part of the API are available [here](utils.py)
* Functions for database manipulation are available [here](database_utils.py)

<p align="center">
  <img src=".\\data_processing_pipeline.png" />
</p>

## Licence
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
