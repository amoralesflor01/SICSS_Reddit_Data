# SICSS@UCLA 2025 Reddit Data

This repository contains a python script for obtaining data from subreddits for our SICSS@UCLA 2025 Project

---

## Features

- Extracts posts, comments, and metadata from specified subreddits.
- Outputs subreddit data into individual CSV files.
- Allows customization of subreddits, date ranges, and comment depth.

## Required software

1. **Git**

    
    [Download Git here](https://git-scm.com/downloads)
    

1. **Python**

    [Download Python here](https://www.python.org/downloads/)

## How to run script

In order to run the script, please open a terminal on your computer then follow the instructions listed below. The commands are for **UNIX** systems so they will be different if you are a Windows user. If you are running on Windows, please use **Powershell** and I will specify where different commands should be used although most commands will be similar.

### Installation

1. Clone the repository to a preferred directory on your computer (i.e. Documents).

    ```bash
    git clone git@github.com:amoralesflor01/SICSS_Reddit_Data.git
    ```
    Or use the command below if you have not set up your SSH keys on GitHub.

    ```bash
    git clone https://github.com/amoralesflor01/SICSS_Reddit_Data.git
    ```

1. Navigate to the project directory.
 
    ```bash
    cd SICSS_Reddit_Data
    ```

1. Create a Python environment to avoid dependency conflicts on your base environment.

    ```bash
    python -m venv reddit_scraper_env
    ```

    Then activate the virtual python environment.

    **Linux/Mac**
    ```bash
    source reddit_scraper_env/bin/activate
    ```
    **Windows Powershell**
    ```powershell
    .\reddit_scraper_env\Scripts\Activate.ps1
    ```

    ***Note for Windows users***  
    If you recieve the following error *"running scripts is disabled on this system"* when trying to activate your environment, try running this command once in the same PowerShell terminal:

    ```powershell
    Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
    ```
    * When prompted, type `Y` then `Enter` to allow the change  

    Next install the necessary dependencies.

    ```bash
    pip install -r requirements.txt
    ```

1. Create your `config.json` file to add your credentials.

    **Linux/Mac**
    ```bash
    touch config.json
    ```
    
    **Windows Powershell**
    ```powershell
    New-Item -Name config.json -ItemType File
    ```

    Then follow the instructions below.

    ### Configuration

    This script requires a `config.json` file to store your API credentials and other sensitive information. 

    **Important:** Do not upload your `config.json` file to GitHub or share it publicly. It contains private keys and passwords.

    - Add the following text inside the file by copying and pasting then replace with your real credentials:

    ```json
    {
    "client_id": "your_client_id_here",
    "client_secret": "your_client_secret_here",
    "username": "your_username_here",
    "password": "your_password_here",
    "user_agent": "example_app/0.1 by your_username"
    }
    ```
    Then save by pressing `ctrl + s` for Linux and PC users, or `command + s` for Mac users.

    ### Example (with fake data)

    ```json
    {
    "client_id": "ABC123XYZ",
    "client_secret": "s3cr3tFAK3k3y",
    "username": "cool_user42",
    "password": "notARealPassword!",
    "user_agent": "myRedditBot/1.0 by cool_user42"
    }
    ```
    If you are unsure on how to create/obtain your credetials please visit the following [YouTube tutorial](https://www.youtube.com/watch?v=x9boO9x3TDA)

1. Edit the python script

    At the bottom of the script (`reddit_data_extraction_script.py`), you can customize which subreddits you want to extract data from.


    ```python
    # Subreddit names must be lowercase
    SUBREDDITS = [
        "politics", 
        "politicaldiscussion", 
        "immigration"
    ]
    ```

    Set the following variables to the desired date ranges in the specified format.

    - **NOTE:** The farther back in time you wish to pull data from will result in less data being extracted due to API historical data limitations.

    ```python
    # Date ranges ("YYYY-MM-DD")
    GLOBAL_START = "2025-01-01" 
    GLOBAL_END   = "2025-07-01"  
    OUTPUT_DIR = "csv_data"
    POSTS_PER_SUBREDDIT = 1000
    COMMENTS_PER_POST = 3  # Number of comments to extract per post (1-10 recommended)
    ```
    - Be sure to save your edits by using `ctrl + s` for Linux and PC users, or `command + s` for Mac users.

1. Run the script (with environment still activated):

    ```bash
    python reddit_data_extraction_script.py
    ```

    Once the script completes, deactivate the environment.

    ```bash
    deactivate
    ```

    **Note:** For future runs, simply reactivate the python environment (***See step 3***), then run the script, then deactivate again.  

### Expected Output:

If directions were followed, you should see the following results:
- One CSV file for each subreddit specified in the SUBREDDITS list.
- Files will be in the `csv_data` folder (This will be created at runtime).

Example files:
- `politics_data_2025-01-01_to_2025-07-01.csv`
- `politicaldiscussion_data_2025-01-01_to_2025-07-01.csv`
- `immigration_data_2025-01-01_to_2025-07-01.csv`

ALEXIS

SEPTEMBER