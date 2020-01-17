#!/usr/bin/python3
import psycopg2
import pika


#maybe https://github.com/RhodiumToad/ip4r/blob/master/README.ip4r will be needed
# GIST index
#'seq', 'log_type', 'timestamp', 'peer_ip', 'bmp_router_port', 'event_type', 'afi', 'safi', 'ip_prefix', 'bgp_nexthop', 'as_path', 'ecomms', 'origin', 'local_pref', 'rd', 'label', 'bmp_router', 'bmp_msg_type'
"""
CREATE TABLE bmpbb (
  "seq" int not null,
  "log_type" text not null,
  "timestamp" timestamp without time zone NOT NULL,
  "peer_ip" inet not null,
  "bmp_router_port" int not null,
  "event_type" text not null,
  "afi" int not null,
  "safi" int not null,
  "ip_prefix" cidr not null,
  "bgp_nexthop" inet not null,
  "as_path" text not null,
  "comms" text not null,
  "ecomms" text not null,
  "origin" text not null,
  "local_pref" int not null,
  "rd" text not null,
  "label" int not null,
  "bmp_router" inet not null,
  "bmp_msg_type" text not null 
);

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
SELECT create_hypertable('bmpbb', 'timestamp');
ALTER TABLE bmpbb ADD PRIMARY KEY (timestamp, host);
grant select on table bmpbb to export;
create user bmpbb with encrypted password 'redacted';
grant all privileges on bmpbb to bmpbb;
"""
conn=psycopg2.connect(host="localhost",user="bmpbb",port=5432,password="redacted",dbname="bmpbb")
c=conn.cursor()

def toDB(ch, method, properties, body):
    params = eval(body)
    cols = params.keys()
    cols_str = ','.join(cols) 
    vals = [ params[k] for k in cols ]
    vals_str = ','.join( ['%s' for i in range(len(vals))] )
    try:
       query = """INSERT INTO bmpbb ({}) VALUES ({})""".format(cols_str, vals_str)
       c.execute(query, vals)
       conn.commit()
    except psycopg2.Error as e:
       print("Error inserting parameters to DB")
       print(e)
       print(params)
       conn.rollback()

#AMQP workflow - connect, create exchange, create queue, bind them together
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='pmacct', type='direct')
channel.queue_declare(queue='bmp_log')
channel.queue_bind(exchange='pmacct', routing_key='bmp_log', queue='bmp_log')
channel.basic_consume(toDB, queue='bmp_log', no_ack=True)
channel.start_consuming()

conn.close()
connection.close()
