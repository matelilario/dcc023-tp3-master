#!/usr/bin/env python

###########################################
#               Redes - TP3               #
#            message_utils.py             #
#                                         # 
# Autor: Jonatas Cavalcante               #
# Matricula: 2014004301                   #
###########################################

import struct


ID_MSG_TYPE = 4
KEYREQ_MSG_TYPE = 5
TOPOREQ_MSG_TYPE = 6
KEYFLOOD_MSG_TYPE = 7
TOPOFLOOD_MSG_TYPE = 8
RESP_MSG_TYPE = 9
LOCALHOST = '127.0.0.1'


def create_id_msg(port):
	type = struct.pack("!H", ID_MSG_TYPE)
	porta = struct.pack("!H", port)

	msg = type + porta
	print (msg)


	return msg


def create_keyreq_msg(nseq, key):
	type = struct.pack("!H", KEYREQ_MSG_TYPE)
	nseq = struct.pack("!I", nseq)
	size = struct.pack("@H", len(key))

	msg = type + nseq + size + key.encode('ascii')

	return msg


def create_toporeq_msg(nseq):
	type = struct.pack("!H", TOPOREQ_MSG_TYPE)
	nseq = struct.pack("!I", nseq)

	msg = type + nseq

	return msg


def create_flood_message(msg_type, ttl, nseq, src_port, info):
	
	if msg_type == KEYFLOOD_MSG_TYPE:
		type = struct.pack("!H", KEYFLOOD_MSG_TYPE)
	else:
		type = struct.pack("!H", TOPOFLOOD_MSG_TYPE)

	ttl = struct.pack("!H", ttl)
	nseq = struct.pack("!I", nseq)

	src_ip = LOCALHOST.split(".")

	for i in range(0,4):
		ip += struct.pack("!b", int(src_ip[i]))

	print (ip)
	src_port = struct.pack("!H", src_port)
	size = struct.pack("@H", len(info))

	msg = type + ttl + nseq + ip + src_port + size + info.encode('ascii')

	return msg


def create_resp_msg(nseq, value):
	type = struct.pack("!H", RESP_MSG_TYPE)
	nseq = struct.pack("!I", nseq)
	size = struct.pack("@H", len(value))

	msg = type + nseq + size + value.encode('ascii')

	return msg


def receive_servent_msg(con, addr, nseq):
	(msg_type,) = struct.unpack("!H", con.recv(2))
	(msg_nseq,) = struct.unpack("!I", con.recv(4))

	if msg_type != 9 or msg_nseq != nseq:
		print("Mensagem incorreta recebida de " + addr)
		return
	else:
		(msg_size,) = struct.unpack("@H", con.recv(2))
		print ('msgsize:',msg_size)
		msg_value = con.recv(msg_size)
		print(msg_value.decode('ascii') + " " + addr)
		return


def get_keyreq_msg_data(con):
	(nseq,) = struct.unpack("!I", con.recv(4))
	(size,) = struct.unpack("@H", con.recv(2))
	
	key = con.recv(size)

	return nseq, key


def get_toporeq_msg_data(con):
	(nseq,) = struct.unpack("!I", con.recv(4))

	return nseq


def get_flood_msg_data(con):
	(ttl,) = struct.unpack("!H", con.recv(2))
	(nseq,) = struct.unpack("!I", con.recv(4))

	for i in range(0,4):
		src_ip += struct.unpack("!b", con.recv(1))[0]
		if i < 3:
			src_ip += '.'

	src_ip = con.recv(4)
	(src_port,) = struct.unpack("!H", con.recv(2))
	(size,) = struct.unpack("@H", con.recv(2))
	info = con.recv(size)

	return ttl, nseq, src_ip, src_port, info.decode('ascii')
