# async-kaltura-backup
Python script to batch download Kaltura entries (source video and child assets) that have not been watched in 4 years.

## Prerequisites
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

At this point the script will collect entries that haven't been watched in four years from the current date. However this caps out at 10,000 entries. To keep the script simple you can just run a second time after running the delete script. After the search it will begin collecting assets and downloading into the root directory into subdirectories named from the entry ids.

## Deleting Entries
The async download script generates a csv that you can use to delete the archived entries. Simply change the file name to reflect the most recent report and run the script. There is a virtually undocumented limit of 35 deletes per 10 seconds. This would have been confusing to work out asynchronously (imo), so it merely puts blocked deletes at the back of the queue. This may cause a problem right at the end, I can't tell if the last few fail to download or just fail to be counted.

## Notes
* This does not use the KalturaApiClient python library. I found it easier to use simple http requests.
* This was my first time using the two asynchronous libraries and async in python, please forgive (or correct) the oblique, un-idiomatic syntax.
* The only errors I encountered when developing was timeout errors from aiohttp, which is set very high now (one hour) to accommodate > 2 hr .mov files and Klatura's speeds.
* It's not bullet proof, bad data from the API can still cause hanging or missing an occasional file. But, it can be run repeatedly.

## Planned
* Clean up the output and logging
* autmoatically prep the delete script with file name
* bullet proof data validation (including file sizes) 
