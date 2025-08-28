# SICSS@UCLA 2025 Reddit Data

This repository contains a python script for obtaining data from subreddits for our SICSS@UCLA 2025 Project

---

### Extraction of posts, comments, and post metadata from subreddits of interest.

You can modify the following lines of code to decide which subreddits you want to pull data from and output the data into CSV files.

**NOTE:** You can find these lines if code at the very end of the script!

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

<!-- ### Prerequisites

- Python 3.7+ installed on your system.
- Flask 2.0.1 and OpenAI Python SDK installed.
- Set up your OpenAI API key. -->

### Installation

<!-- 1. Install the required Python packages:
    ```bash
    pip install -r requirements.txt
    ```
This command will install all the necessary Python packages and dependencies required for your chatbot application. -->

1. Clone the repository to your computer.

    ```bash
    git clone git@github.com:amoralesflor01/SICSS_Reddit_Data.git
    ```

1. Navigate to the project directory.
 
    ```bash
    cd SICSS_Reddit_Data/
    ```

1. Run the script.

    ```bash
    python reddit_data_extraction_script.py
    ```

Once the script completes, your resulting CSV file or files will be in the folder `csv_data`.
