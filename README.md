# WAMerge
WhatsApp Message Store Merger
I had to reset my main phone, and in the meantine I used my old phone.  I stupidly "logged in" to WA thinking it would inherit everything from my linked devices.  Instead it disconnecs all the linked devices, hiding their history, and giving you no histors on the newly-logged in device.  F. That.
In the meantime I am sending and receiving messages without history on the old device.

I got my normal phone back up and whatsapp logged in and the history pre-reset was there but NOTHING from the time on my other device.  WTF

Luckily I backed up the database before reset, and the database on the old phone.  How to merge these?  I found Metion of tools that did no work (any more, at least).  So I loaded up the database into DB Browser to check out the schema and figure out the table links until I could query conersations as they appeared on the devices.

Consider this a working utility as of March 2025.
1
A Python script, requiring sqlite3

Unencrypted copies of your WhatsApp msgstore.db files
  * msgstore.db
  * msgstore.db-shm  (if exists)
  * msgstore.db-wal  (if exists)

On Android if you are rooted this is in /data/data/com.whatsapp/databases

Keep the larger of the 2 databases named as-is (`msgstore.db`)
Rename the smaller database you want merged into the above as `msgstore.small` (and related files similarly as `msgstore.small-shm`, `msgstore.small-wal`)

Just run the script:
  `python WAMerge.py`

This will copy users, groups, call history, chat [messages, links, media/albums, locations, mentions, replies, reactions]

What i haven't bothered with are chat polls, commerce/payment related stuff, read receipts.
I will work on those later as they were not important to me at this time.
