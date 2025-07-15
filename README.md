# async-kaltura-backup
Python script to batch download Kaltura entries (source and child entries) in parallel from a csv (like one that can be obtained with (Kaltura Library Export).

## Prerequisites
* A csv of Kaltura entries you wish to download with an "Entry ID" column
* Python ~3.9
* pip

## Installation/Setup
From inside the cloned repo or downloaded and extracted zip file, create a python virtual environment and install the necessary packages.
```
python -m venv .
source bin/activate
pip install -r requirements.txt
```
## Usage
* Edit the empty variables in async-kaltura-backup.py (lines 10-14) to reflect your Kaltura account.
* If you expect the script to run for more than 24 hours, change the timeout setting on line 37
	* Downloads from Kaltura can be pretty slow 750 GB took over 24 hours in my experience.
* Note: the root directory should already exist, other directories will be made
* then finally...
```
python async-kaltura-backup.py 
```

## Notes
* This does not use the KalturaApiClient python library. I found it easier to use simple http requests.
* This was my first time using the two asynchronous libraries and async in python, please forgive (or correct) the oblique, un-idiomatic syntax.
* The only errors I encountered when developing was timeout errors from aiohttp, which is set very high now (one hour) to accommodate > 2 hr .mov files and Klatura's speeds. 
