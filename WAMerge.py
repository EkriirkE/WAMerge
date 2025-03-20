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

print("Preparing to merge WA databases:")
if 1:
	db=sqlite3.connect("msgstore.db")
	db.execute("ATTACH 'msgstore.small' AS b")
db.row_factory=sqlite3.Row
#db.autocommit=True
cur=db.cursor()

#Prefetch users and groups from A and B
a_jid=[]
for row in db.execute("SELECT * FROM jid").fetchall():a_jid+=[{x:row[x] for x in row.keys()}]
a_chat=[]
for row in db.execute("SELECT * FROM chat").fetchall():a_chat+=[{x:row[x] for x in row.keys()}]

b_jid=[]
for row in db.execute("SELECT * FROM b.jid ORDER BY _id").fetchall():b_jid+=[{x:row[x] for x in row.keys()}]
b_chat=[]
for row in db.execute("SELECT * FROM b.chat ORDER BY _id").fetchall():b_chat+=[{x:row[x] for x in row.keys()}]

#Match B message to A message via common key_id
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

#Match B jid to A jid via common user
def getBAjid(b_j_id):
	if not b_j_id:return (None,None)
	if b_j_id==-1:return ({"_id":-1},{"_id":-1})
	aj=bj=None
	if not (bj:=next((x for x in b_jid if x["_id"]==b_j_id),None)):
		print(f"Could not resolve B.jid._id={b_j_id}")
	elif not (aj:=next((x for x in a_jid if x["user"]==bj["user"]),None)):
		print(f"Could not resolve A.jid.user={bj['user']}")
	return (bj,aj)

#Match B chat to A chat via common jid user
def getBAchat(b_chat_id):
	if not b_chat_id:return (None,None)
	bj=aj=ac=None
	if not (bc:=next((x for x in b_chat if x["_id"]==b_chat_id),None)):
		print(f"Could not resolve B.chat._id={b_chat_id}")
	else:
		bj,aj=getBAjid(bc["jid_row_id"])
	if aj and not (ac:=next((x for x in a_chat if x["jid_row_id"]==aj["_id"]),None)):
		print(f"Could not resolve A.chat.jid_row_id={aj['_id']}")
	return (bc,bj,aj,ac)

#Instead of inheriting the columns from source DB as the schemas may differ. So get what actually exists in the dest DB
#Ignore the primary key (_id typically) as this will auto-generate
schemas={}
def tablecols(table,sans=[],pk=False):
	if t:=schemas.get(table):return t
	res=db.execute(f"SELECT name,pk FROM pragma_table_info('{table}')").fetchall()
	t=[x["name"] for x in res if x["name"] not in sans and (pk or not x["pk"])]
	schemas.update({table:t})
	return t

if 1:
	print("People & groups...")
	for row in b_jid:
		if any(x for x in a_jid if x["user"]==row["user"]):continue

		print(f"Copy {row['_id']} ({row['user']})",end="...",flush=True)

		r={x:row[x] for x in row if x in tablecols("jid")}
		cur.execute(f"INSERT INTO jid ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)
		r["_id"]=cur.lastrowid
		a_jid+=[r]
	
	for row in b_chat:
		bj,aj=getBAjid(row["jid_row_id"])
		if not (bj and aj):continue
		if any(x for x in a_chat if x["jid_row_id"]==aj["_id"]):continue

		print(f"Copy {row['_id']} ({row['subject'] or aj['user']})",end="...",flush=True)

		r={x:row[x] for x in row if x in tablecols("chat")}
		r["jid_row_id"]=aj["_id"]
		cur.execute(f"INSERT INTO chat ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)
		r["_id"]=cur.lastrowid
		a_chat+=[r]
		
if 1:
	print("Messages...")
	for row in db.execute("SELECT * FROM b.message WHERE message_type NOT IN (7,11) AND key_id NOT IN (SELECT key_id FROM message) ORDER BY timestamp ASC").fetchall():
		#b_msg+=[{x:row[x] for x in row.keys()}]
		bc,bj,aj,ac=getBAchat(row["chat_row_id"])
		if not (bc and bj and aj and ac):continue
		bsj,asj=getBAjid(row["sender_jid_row_id"])
		if row["sender_jid_row_id"] and not (bsj and asj):continue

		print(f"Copy {row['key_id']} ({row['chat_row_id']}:{row['_id']})",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message",sans=("sort_id",))}
		r["chat_row_id"]=ac["_id"]
		if asj:r["sender_jid_row_id"]=asj["_id"]
		#print(f"INSERT INTO message ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		cur.execute(f"INSERT INTO message ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)
		db.execute("UPDATE message SET sort_id=? WHERE _id=?",(cur.lastrowid,cur.lastrowid))
		db.execute("UPDATE chat SET display_message_row_id=?,last_message_row_id=? WHERE _id=?",(cur.lastrowid,cur.lastrowid,ac["_id"]))
	#UPDATE chat SET display_message_row_id=(SELECT MAX(message._id) FROM message WHERE message.chat_row_id=chat.

if 1:
	print("Message secrets...")
	for row in db.execute("SELECT ms.* FROM b.message_secret ms INNER JOIN b.message m ON m._id=ms.message_row_id WHERE key_id NOT IN (SELECT key_id FROM message_secret xms INNER JOIN message xm ON xm._id=xms.message_row_id)").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue

		print(f"Copy {row['message_row_id']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message_secret")}
		cur.execute(f"INSERT INTO message_secret ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

if 1:
	print("Message albums...")
	for row in db.execute("SELECT ma.*,m.key_id FROM b.message_album ma INNER JOIN b.message m ON m._id=ma.message_row_id WHERE m.key_id NOT IN (SELECT key_id FROM message_album xma INNER JOIN message xm ON xm._id=xma.message_row_id)").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue

		print(f"Copy {row['message_row_id']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message_album")}
		r["message_row_id"]=am["_id"]
		cur.execute(f"INSERT INTO message_album ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

if 1:
	print("Message media...")
	for row in db.execute("SELECT ma.*,m.key_id,m.chat_row_id FROM b.message_media ma INNER JOIN b.message m ON m._id=ma.message_row_id WHERE m.key_id NOT IN (SELECT key_id FROM message_media xma INNER JOIN message xm ON xm._id=xma.message_row_id)").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue

		print(f"Copy {row['message_row_id']} ({row['mime_type']})",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message_media")}
		r["message_row_id"]=am["_id"]
		r["chat_row_id"]=am["chat_row_id"]
		cur.execute(f"INSERT INTO message_media ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

	for row in db.execute("SELECT mt.* FROM b.message_thumbnail mt INNER JOIN b.message m ON m._id=mt.message_row_id WHERE m.key_id NOT IN (SELECT key_id FROM message_thumbnail xmt INNER JOIN message xm ON xm._id=xmt.message_row_id)").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue

		r={x:row[x] for x in row.keys() if x in tablecols("message_thumbnail")}
		r["message_row_id"]=am["_id"]
		cur.execute(f"INSERT INTO message_thumbnail ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		#print(cur.lastrowid)

	for row in db.execute("SELECT mv.* FROM b.message_view_once_media mv INNER JOIN b.message m ON m._id=mv.message_row_id WHERE m.key_id NOT IN (SELECT key_id FROM message_view_once_media xmv INNER JOIN message xm ON xm._id=xmv.message_row_id)").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue

		r={x:row[x] for x in row.keys() if x in tablecols("message_view_once_media")}
		r["message_row_id"]=am["_id"]
		cur.execute(f"INSERT INTO message_view_once_media ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		#print(cur.lastrowid)

if 1:
	print("Message mentions...")
	for row in db.execute("SELECT mm.* FROM b.message_mentions mm INNER JOIN b.message m ON m._id=mm.message_row_id WHERE m.key_id NOT IN (SELECT key_id FROM message_mentions xmm INNER JOIN message xm ON xm._id=xmm.message_row_id)").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue
		bj,aj=getBAjid(row["jid_row_id"])
		if not (bj and aj):continue

		print(f"Copy {row['message_row_id']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message_mentions")}
		r["message_row_id"]=am["_id"]
		r["jid_row_id"]=aj["_id"]
		cur.execute(f"INSERT INTO message_mentions ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

if 1:
	print("Message links...")
	for row in db.execute("SELECT ml.* FROM b.message_link ml INNER JOIN b.message m ON m._id=ml.message_row_id WHERE m.key_id NOT IN (SELECT key_id FROM message_link xml INNER JOIN message xm ON xm._id=xml.message_row_id)").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue

		print(f"Copy {row['message_row_id']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message_link")}
		r["message_row_id"]=am["_id"]
		r["chat_row_id"]=am["chat_row_id"]
		cur.execute(f"INSERT INTO message_link ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

if 1:
	print("Message locations...")
	for row in db.execute("SELECT ml.* FROM b.message_location ml INNER JOIN b.message m ON m._id=ml.message_row_id WHERE m.key_id NOT IN (SELECT key_id FROM message_location xml INNER JOIN message xm ON xm._id=xml.message_row_id)").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue

		print(f"Copy {row['message_row_id']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message_location")}
		r["message_row_id"]=am["_id"]
		r["chat_row_id"]=am["chat_row_id"]
		cur.execute(f"INSERT INTO message_location ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

if 1:
	print("Message quoted...")
	for row in db.execute("SELECT * FROM b.message_quoted WHERE key_id NOT IN (SELECT key_id FROM message_quoted)").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue
		bc,bj,aj,ac=getBAchat(row["chat_row_id"])
		if not (bc and bj and aj):continue
		bpc,bpj,apj,apc=getBAchat(row["parent_message_chat_row_id"])
		if not (bpc and bpj and apj and apc):continue
		bsj,asj=getBAjid(row["sender_jid_row_id"])
		if row["sender_jid_row_id"] and not (bsj and asj):continue

		print(f"Copy {row['key_id']} ({row['chat_row_id']}:{row['message_row_id']})",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message_quoted")}
		r["message_row_id"]=am["_id"]
		r["chat_row_id"]=am["chat_row_id"]
		r["parent_message_chat_row_id"]=apc["_id"]
		if asj:r["sender_jid_row_id"]=asj["_id"]
		cur.execute(f"INSERT INTO message_quoted ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

	for row in db.execute("SELECT ma.* FROM b.message_association ma INNER JOIN b.message m ON m._id=ma.child_message_row_id WHERE m.key_id NOT IN (SELECT key_id FROM message_association xma INNER JOIN message xm ON xm._id=xma.child_message_row_id) ORDER BY parent_message_row_id ASC").fetchall():
		if not (apm:=getBAmsg(row["parent_message_row_id"])):continue
		if not (acm:=getBAmsg(row["child_message_row_id"])):continue

		print(f"Copy {row['parent_message_row_id']}:{row['child_message_row_id']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message_association")}
		r["parent_message_row_id"]=apm["_id"]
		r["child_message_row_id"]=acm["_id"]
		cur.execute(f"INSERT INTO message_association ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

if 1:
	print("Message addons...")
	for row in db.execute("SELECT * FROM b.message_add_on WHERE key_id NOT IN (SELECT key_id FROM message_add_on) ORDER BY timestamp ASC").fetchall():
		if not (am:=getBAmsg(row["parent_message_row_id"])):continue
		bc,bj,aj,ac=getBAchat(row["chat_row_id"])
		if not (bc and bj and aj and ac):continue
		bsj,asj=getBAjid(row["sender_jid_row_id"])
		if not (bsj and asj):continue

		print(f"Copy {row['parent_message_row_id']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("message_add_on")}
		r["parent_message_row_id"]=am["_id"]
		r["chat_row_id"]=ac["_id"]
		r["sender_jid_row_id"]=asj["_id"]
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
	print("Receipts..")
	for row in db.execute("SELECT * FROM b.receipts WHERE key_id||remote_resource NOT IN (SELECT key_id||remote_resource FROM receipts) ORDER BY receipt_device_timestamp ASC").fetchall():

		print(f"Copy {row['key_id']}:{row['remote_resource']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("receipts")}
		cur.execute(f"INSERT INTO receipts ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

	for row in db.execute("SELECT ru.* FROM b.receipt_user ru INNER JOIN b.message m ON m._id=ru.message_row_id WHERE m.key_id||ru.receipt_timestamp NOT IN (SELECT xm.key_id||xru.receipt_timestamp FROM receipt_user xru INNER JOIN message xm ON xm._id=xru.message_row_id) ORDER BY receipt_timestamp ASC").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue
		brj,arj=getBAjid(row["receipt_user_jid_row_id"])
		if not (brj and arj):continue

		print(f"Copy {am['_id']}:{arj['_id']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("receipt_user")}
		r["message_row_id"]=am["_id"]
		r["receipt_user_jid_row_id"]=arj["_id"]
		cur.execute(f"INSERT INTO receipt_user ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

	for row in db.execute("SELECT rd.* FROM b.receipt_device rd INNER JOIN b.message m ON m._id=rd.message_row_id WHERE m.key_id||rd.receipt_device_timestamp NOT IN (SELECT xm.key_id||xrd.receipt_device_timestamp FROM receipt_device xrd INNER JOIN message xm ON xm._id=xrd.message_row_id) ORDER BY receipt_device_timestamp ASC").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue
		brj,arj=getBAjid(row["receipt_user_jid_row_id"])
		if not (brj and arj):continue

		print(f"Copy {am['_id']}:{arj['_id']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("receipt_device")}
		r["message_row_id"]=am["_id"]
		r["receipt_user_jid_row_id"]=arj["_id"]
		cur.execute(f"INSERT INTO receipt_device ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

	for row in db.execute("SELECT * FROM b.receipt_orphaned WHERE key_id||timestamp NOT IN (SELECT key_id||timestamp FROM receipt_orphaned) ORDER BY timestamp ASC").fetchall():
		bc,bj,aj,ac=getBAchat(row["chat_row_id"])
		if not ac:continue
		bdj,adj=getBAjid(row["receipt_device_jid_row_id"])
		if not (bdj and adj):continue
		brj,arj=getBAjid(row["receipt_recipient_jid_row_id"])
		print(f"Copy {row['key_id']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("receipt_orphaned")}
		r["chat_row_id"]=ac["_id"]
		r["receipt_device_jid_row_id"]=adj["_id"]
		r["receipt_recipient_jid_row_id"]=arj["_id"] if arj else None
		cur.execute(f"INSERT INTO receipt_orphaned ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

if 1:
	print("Call log...")
	for row in db.execute("SELECT * FROM b.call_log WHERE call_id||timestamp NOT IN (SELECT call_id||timestamp FROM call_log) ORDER BY timestamp ASC").fetchall():
		bj,aj=getBAjid(row["jid_row_id"])
		if not (bj and aj):continue

		print(f"Copy {row['call_id']} ({row['jid_row_id']}:{row['_id']})",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("call_log")}
		r["jid_row_id"]=aj["_id"]
		cur.execute(f"INSERT INTO call_log ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

	for row in db.execute("SELECT mc.* FROM b.missed_call_logs mc INNER JOIN b.message m ON m._id=mc.message_row_id WHERE m.key_id NOT IN (SELECT xm.key_id FROM missed_call_logs xmc INNER JOIN message xm ON xm._id=xmc.message_row_id) ORDER BY timestamp ASC").fetchall():
		if not (am:=getBAmsg(row["message_row_id"])):continue

		print(f"Copy {row['message_row_id']}",end="...",flush=True)

		r={x:row[x] for x in row.keys() if x in tablecols("missed_call_logs")}
		cur.execute(f"INSERT INTO missed_call_logs ({','.join((x for x in r))}) VALUES ({','.join('?'*len(r))})",tuple(r[x] for x in r))
		print(cur.lastrowid)

cur.close()
db.commit()
db.close()
print("\n\nDone!")
