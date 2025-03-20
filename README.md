# WAMerge
#### WhatsApp Message Store Merger  

A Python script, requiring sqlite3

---

I had to reset my main phone, and in the meantine I used my old phone.  I stupidly "logged in" to WA thinking it would inherit everything from my linked devices.  Instead it disconnects all the linked devices, hiding their history, and giving you no history on the newly-logged in device.  F. That.  
In the meantime I am sending and receiving messages without history on the old device.

I got my normal phone back up and whatsapp logged in and the pre-reset-history was there but NOTHING from the time on my other device.  WTF

Luckily I backed up the database before reset, and the database on the old phone.  How to merge these?  I found mention of tools that did not work (any more, at least).  So I loaded up the database into DB Browser to check out the schema and figure out the table links until I could query conversations as they appeared on the devices.

What makes this difficult is everything links together by an auto-increment primary key.  i.e. a new database starts every entry of data with an ID of 1 and counts up with each entry.  This does not mesh easily with 2 datasets becasue they both started at 1 and two IDs cannot co-exist.  So you need to re-number the IDs from one side to continue where the other left off but also correctly re-align the links to everything esle. 

Done.

Consider this a working utility as of March 2025.

### What you need
Python with sqlite3  
Unencrypted copies of your WhatsApp msgstore.db files  
  * msgstore.db
  * msgstore.db-shm  (if exists)
  * msgstore.db-wal  (if exists)

On Android if you are rooted this is in /data/data/com.whatsapp/databases

Keep the larger of the 2 databases named as-is (`msgstore.db`)
Rename the smaller database you want merged into the above as `msgstore.small` (and related files similarly as `msgstore.small-shm`, `msgstore.small-wal` if they exist)

Just run the script:
  `python WAMerge.py`

This will copy users, groups, call history, chat [messages, links, media/albums, locations, mentions, replies, reactions]  
By default you will get a new file `msgstore.db.merged` without the 2 shm/wal files!  When you copy it back to your phone make sure to delete the msgstore.db* files from the phone before copying the new db, and renaming it back to `msgstore.db`.

What I haven't bothered with are chat polls, commerce/payment related stuff.
I will work on those later as they were not important to me at this time.
