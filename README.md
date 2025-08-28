# SICSS@UCLA 2025 Reddit Data

This repository contains a python script for obtaining data from subreddits for our SICSS@UCLA 2025 Project

---

## Features

- Extracts posts, comments, and metadata from specified subreddits.
- Outputs data into CSV files.
- Allows customization of subreddits, date ranges, and comment depth.

### Configuration

At the bottom of the script (`reddit_data_extraction_script.py`), you can customize:


```python
SUBREDDITS = [
    "politics", 
    "politicaldiscussion", 
    "immigration"
]
```

Set the following variables to the desired date ranges in the specified format.

**NOTE:** The farther back in time you wish to pull data from will result in less data being extracted due to API historical data limitations.

```python
# Date ranges ("YYYY-MM-DD")
GLOBAL_START = "2025-01-01" 
GLOBAL_END   = "2025-07-01"  
OUTPUT_DIR = "csv_data"
POSTS_PER_SUBREDDIT = 1000
COMMENTS_PER_POST = 3  # Number of comments to extract per post (1-10 recommended)
```

## How to run script

In order to run the script, please follow the instructions listed below


### Installation

1. Clone the repository to your computer.

    ```bash
    git clone git@github.com:amoralesflor01/SICSS_Reddit_Data.git
    ```

2. Navigate to the project directory.
 
    ```bash
    cd SICSS_Reddit_Data/
    ```

3. Run the script.

    ```bash
    python reddit_data_extraction_script.py
    ```

Once the script completes, your resulting CSV file or files will be in the folder `csv_data`.
