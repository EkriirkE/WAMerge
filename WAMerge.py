#!/usr/bin/env python
#--------========########========--------
#	WhatsApp Message Store Merger
#	2025-03-01	Erik Johnson - EkriirkE
#
#	Keep your larger `msgstore.db` named as-is
#	Name the second (smaller) msgstore you want merged into the larger one above as `msgstore.small`
#	Run:
#	python WAMerge.py
#
#--------========########========--------

import sqlite3

db=sqlite3.connect("msgstore.db")
db.execute("ATTACH 'msgstore.small' AS b")
db.row_factory=sqlite3.Row
#db.autocommit=True

cur=db.cursor()
#for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall():print(r)

a_jid=[]
for row in db.execute("SELECT * FROM jid").fetchall():a_jid+=[{x:row[x] for x in row.keys()}]
a_chat=[]
for row in db.execute("SELECT * FROM chat").fetchall():a_chat+=[{x:row[x] for x in row.keys()}]


b_jid=[]
for row in db.execute("SELECT * FROM b.jid").fetchall():b_jid+=[{x:row[x] for x in row.keys()}]
b_chat=[]
for row in db.execute("SELECT * FROM b.chat").fetchall():b_chat+=[{x:row[x] for x in row.keys()}]

a_msg=[]
b_msg=[]
def getBAmsg(b_msg_id):
	global a_msg,b_msg
	if not (bm:=next((x for x in b_msg if x["_id"]==b_msg_id),None)):
		if not (bm:=cur.execute("SELECT * FROM b.message WHERE _id=?",(b_msg_id,)).fetchall()):
			print(f"Could not resolve B.message._id={b_msg_id}")
			return None
		bm=bm[0]
		r={x:bm[x] for x in bm.keys()}
		b_msg+=[r]
	if am:=next((x for x in a_msg if x["key_id"]==bm["key_id"]),None):
		return am
	if not (am:=cur.execute("SELECT * FROM message WHERE key_id=?",(bm["key_id"],)).fetchall()):
		print(f"Could not resolve A.message.key_id={bm['key_id']}")
		return None
	am=am[0]
	r={x:am[x] for x in am.keys()}
	a_msg+=[r]
	return r

#Instead of inheriting the columns from source DB as the schemas may differ. So get what actually exists in the dest DB
schemas={}
def tablecols(table,sans=[]):
	if t:=schemas.get(table):return t
	res=db.execute(f"SELECT name FROM pragma_table_info('{table}')").fetchall()
	t=[x["name"] for x in res if x["name"] not in sans]
	schemas.update({table:t})
	return t

if 1:
	print("Messages...")
	for row in db.execute("SELECT * FROM b.message WHERE message_type NOT IN (7,11) AND key_id NOT IN (SELECT key_id FROM message) ORDER BY timestamp ASC").fetchall():
		#b_msg+=[{x:row[x] for x in row.keys()}]
		if not (bc:=next((x for x in b_chat if x["_id"]==row["chat_row_id"]),None)):
			print(f"Could not resolve B.chat._id={row['chat_row_id']}")
			continue
		if not (bj:=next((x for x in b_jid if x["_id"]==bc["jid_row_id"]),None)):
			print(f"Could not resolve B.jid._id={bc['jid_row_id']}")
			continue
		if not (bsj:=next((x for x in b_jid if x["_id"]==row["sender_jid_row_id"]),None)) and row["sender_jid_row_id"]:
			print(f"Could not resolve sender B.jid._id={row['sender_jid_row_id']}")
			continue
		if not (aj:=next((x for x in a_jid if x["user"]==bj["user"]),None)):
			print(f"Could not resolve A.jid.user={bj['user']}")
			continue
		if not (ac:=next((x for x in a_chat if x["jid_row_id"]==aj["_id"]),None)):
			print(f"Could not resolve A.chat._id={aj['_id']}")
			continue

		print(f"Copy {row['key_id']} ({row['chat_row_id']}:{row['_id']})",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message",sans=("_id","sort_id"))}
		r["chat_row_id"]=ac["_id"]
		if bsj:r["sender_jid_row_id"]=bsj["_id"]
		#print(f"INSERT INTO message ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		cur.execute(f"INSERT INTO message ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)
		db.execute("UPDATE message SET sort_id=? WHERE _id=?",(cur.lastrowid,cur.lastrowid))
		db.execute("UPDATE chat SET display_message_row_id=?,last_message_row_id=? WHERE _id=?",(cur.lastrowid,cur.lastrowid,ac["_id"]))

if 1:
	print("Message secrets...")
	for row in db.execute("SELECT * FROM b.message_secret WHERE message_row_id NOT IN (SELECT message_row_id FROM message_secret)").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue

		print(f"Copy {row['message_row_id']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message_secret")}
		cur.execute(f"INSERT INTO message_secret ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

if 1:
	print("Message albums...")
	for row in db.execute("SELECT ma.*,m.key_id FROM b.message_album ma INNER JOIN b.message m ON m._id=ma.message_row_id WHERE ma.message_row_id NOT IN (SELECT message_row_id FROM message_album)").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue

		print(f"Copy {row['message_row_id']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message_album")}
		r["message_row_id"]=am["_id"]
		cur.execute(f"INSERT INTO message_album ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

if 1:
	print("Message media...")
	for row in db.execute("SELECT ma.*,m.key_id,m.chat_row_id FROM b.message_media ma INNER JOIN b.message m ON m._id=ma.message_row_id WHERE ma.message_row_id NOT IN (SELECT message_row_id FROM message_media)").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue

		print(f"Copy {row['message_row_id']} ({row['mime_type']})",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message_media")}
		r["message_row_id"]=am["_id"]
		r["chat_row_id"]=am["_id"]
		cur.execute(f"INSERT INTO message_media ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

if 1:
	print("Message mentions...")
	for row in db.execute("SELECT * FROM b.message_mentions WHERE message_row_id NOT IN (SELECT message_row_id FROM message_mentions)").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue
		if not (bc:=next((x for x in b_chat if x["_id"]==row["chat_row_id"]),None)):
			print(f"Could not resolve B.chat._id={row['chat_row_id']}")
			continue
		if not (bj:=next((x for x in b_jid if x["_id"]==bc["jid_row_id"]),None)):
			print(f"Could not resolve B.jid._id={bc['jid_row_id']}")
			continue
		if not (aj:=next((x for x in a_jid if x["user"]==bj["user"]),None)):
			print(f"Could not resolve A.jid.user={bj['user']}")
			continue

		print(f"Copy {row['message_row_id']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message_mentions",sans=("_id",))}
		r["message_row_id"]=am["_id"]
		r["jit_row_id"]=aj["_id"]
		cur.execute(f"INSERT INTO message_mentions ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

if 1:
	print("Message links...")
	for row in db.execute("SELECT * FROM b.message_link WHERE message_row_id NOT IN (SELECT message_row_id FROM message_link)").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue

		print(f"Copy {row['message_row_id']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message_link",sans=("_id",))}
		r["message_row_id"]=am["_id"]
		r["chat_row_id"]=am["chat_row_id"]
		cur.execute(f"INSERT INTO message_link ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

if 1:
	print("Message locations...")
	for row in db.execute("SELECT * FROM b.message_location WHERE message_row_id NOT IN (SELECT message_row_id FROM message_location)").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue

		print(f"Copy {row['message_row_id']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message_location")}
		r["message_row_id"]=am["_id"]
		r["chat_row_id"]=am["chat_row_id"]
		cur.execute(f"INSERT INTO message_location ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

if 1:
	print("Message quoted...")
	for row in db.execute("SELECT * FROM b.message_quoted WHERE message_row_id NOT IN (SELECT message_row_id FROM message_quoted)").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue
		if not (bc:=next((x for x in b_chat if x["_id"]==row["chat_row_id"]),None)):
			print(f"Could not resolve B.chat._id={row['chat_row_id']}")
			continue
		if not (bj:=next((x for x in b_jid if x["_id"]==bc["jid_row_id"]),None)):
			print(f"Could not resolve B.jid._id={bc['jid_row_id']}")
			continue
		if not (bsj:=next((x for x in b_jid if x["_id"]==row["sender_jid_row_id"]),None)) and row["sender_jid_row_id"]:
			print(f"Could not resolve sender B.jid._id={row['sender_jid_row_id']}")
			continue
		if not (aj:=next((x for x in a_jid if x["user"]==bj["user"]),None)):
			print(f"Could not resolve A.jid.user={bj['user']}")
			continue

		if not (bpc:=next((x for x in b_chat if x["_id"]==row["parent_message_chat_row_id"]),None)):
			print(f"Could not resolve B.chat._id={row['chat_row_id']}")
			continue
		if not (bpj:=next((x for x in b_jid if x["_id"]==bpc["jid_row_id"]),None)):
			print(f"Could not resolve B.jid._id={bpc['jid_row_id']}")
			continue
		if not (apj:=next((x for x in a_jid if x["user"]==bpj["user"]),None)):
			print(f"Could not resolve A.jid.user={bpj['user']}")
			continue
		if not (apc:=next((x for x in a_chat if x["jid_row_id"]==apj["_id"]),None)):
			print(f"Could not resolve A.chat._id={apj['_id']}")
			continue

		print(f"Copy {row['key_id']} ({row['chat_row_id']}:{row['message_row_id']})",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message_quoted",sans=("_id"))}
		r["message_row_id"]=am["_id"]
		r["chat_row_id"]=am["chat_row_id"]
		r["parent_message_chat_row_id"]=apc["_id"]
		if bsj:r["sender_jid_row_id"]=bsj["_id"]
		cur.execute(f"INSERT INTO message_quoted ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

	for row in db.execute("SELECT * FROM b.message_association WHERE parent_message_row_id||':'||child_message_row_id NOT IN (SELECT parent_message_row_id||':'||child_message_row_id FROM message_association) ORDER BY parent_message_row_id ASC").fetchall():
		if not (apm:=getBAmsg(row["parent_message_row_id"])):continue
		if not (acm:=getBAmsg(row["child_message_row_id"])):continue

		print(f"Copy {row['parent_message_row_id']}:{row['child_message_row_id']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message_association",sans=("_id",))}
		cur.execute(f"INSERT INTO message_association ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

if 1:
	print("Message addons...")
	for row in db.execute("SELECT * FROM b.message_add_on WHERE parent_message_row_id||':'||chat_row_id NOT IN (SELECT parent_message_row_id||':'||chat_row_id FROM message_add_on) ORDER BY timestamp ASC").fetchall():
		if not (am:=getBAmsg(row["parent_message_row_id"])):continue
		if not (bc:=next((x for x in b_chat if x["_id"]==row["chat_row_id"]),None)):
			print(f"Could not resolve B.chat._id={row['chat_row_id']}")
			continue
		if not (bj:=next((x for x in b_jid if x["_id"]==bc["jid_row_id"]),None)):
			print(f"Could not resolve B.jid._id={bc['jid_row_id']}")
			continue
		if not (bsj:=next((x for x in b_jid if x["_id"]==row["sender_jid_row_id"]),None)) and row["sender_jid_row_id"]:
			print(f"Could not resolve sender B.jid._id={row['sender_jid_row_id']}")
			continue
		if not (aj:=next((x for x in a_jid if x["user"]==bj["user"]),None)):
			print(f"Could not resolve A.jid.user={bj['user']}")
			continue
		if not (ac:=next((x for x in a_chat if x["jid_row_id"]==aj["_id"]),None)):
			print(f"Could not resolve A.chat._id={aj['_id']}")
			continue

		print(f"Copy {row['parent_message_row_id']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message_add_on",sans=("_id",))}
		r["parent_message_row_id"]=am["_id"]
		r["chat_row_id"]=ac["_id"]
		r["sender_jid_row_id"]=bsj["_id"]
		cur.execute(f"INSERT INTO message_add_on ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		aid=cur.lastrowid
		print(aid)
		
		for srow in cur.execute("SELECT * FROM message_add_on_reaction WHERE message_add_on_row_id=?",(row["_id"],)).fetchall():

			print(f"Copy {srow['message_add_on_row_id']}",end="...",flush=True)

			r={x:srow[x] for x in srow.keys() if x in tablecols("message_add_on_reaction")}
			r["message_add_on_row_id"]=aid
			cur.execute(f"INSERT INTO message_add_on_reaction ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
			print(cur.lastrowid)


if 1:
	print("Call log...")
	for row in db.execute("SELECT * FROM b.call_log WHERE call_id NOT IN (SELECT call_id FROM call_log) ORDER BY timestamp ASC").fetchall():
		if not (bj:=next((x for x in b_jid if x["_id"]==row["jid_row_id"]),None)):
			print(f"Could not resolve B.jid._id={bc['jid_row_id']}")
			continue
		if not (aj:=next((x for x in a_jid if x["user"]==bj["user"]),None)):
			print(f"Could not resolve A.jid.user={bj['user']}")
			continue

		print(f"Copy {row['call_id']} ({row['jid_row_id']}:{row['_id']})",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("call_log",sans=("_id",))}
		r["jid_row_id"]=aj["_id"]
		cur.execute(f"INSERT INTO call_log ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)


	for row in db.execute("SELECT * FROM b.missed_call_logs WHERE message_row_id NOT IN (SELECT message_row_id FROM missed_call_logs) ORDER BY timestamp ASC").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue

		print(f"Copy {row['message_row_id']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("missed_call_logs")}
		cur.execute(f"INSERT INTO missed_call_logs ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

cur.close()
db.commit()
db.close()
print("\n\nDone!")
