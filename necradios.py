#!/usr/bin/env python3
from easysnmp import Session
import time
import psycopg2
from psycopg2.extensions import AsIs
import urllib.request
import multiprocessing
from datetime import datetime
"""
CREATE TABLE necradios (
  "timestamp" timestamp with time zone not null,
  "hostname" text not null,
  "host_ip" inet not null,
  "ifindex" bigint not null,
  "ifname" text default NULL,
  "rxoctets" bigint default NULL,
  "txoctets" bigint default NULL,
  "discards" bigint default NULL,
  "txpower" real default NULL,
  "rxlevel" real default NULL,
  "neighborhost text default NULL,
  "neighbormodem text default NULL
);

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
SELECT create_hypertable('necradios', 'timestamp');
ALTER TABLE necradios ADD PRIMARY KEY (timestamp, hostname, ifindex);
grant select on table necradios to export;
create user necradios with encrypted password 'redacted';
grant all privileges on necradios to necradios;
"""
globalnow = time.strftime('%Y-%m-%d %H:%M:%S')

def hosts():
 try:
    f1=urllib.request.urlopen('ftp://user:pass@1.2.3.4/config/data.dat')
    f2=urllib.request.urlopen('ftp://user:pass@1.2.3.5/config/data.dat')
    f3=urllib.request.urlopen('ftp://user:pass@1.2.3.6/config/data.dat')
    ips = list()
    for file in zip(f1,f2,f3):
       for line in file:
          line = str(line)
          if '=NE' in line:
             ip = line.split(',')[1]
             ipdata = ('.'.join(str(int(part)) for part in ip.split('.')))
             ips.append(ipdata)
    out = set(ips)
    #there should be more than 1000 nec radios
    if len(out) > 1000:
        with open('nechosts.cache', 'w') as f:
           for item in out:
              f.write("%s\n" % item)
    with open('nechosts.cache','r') as f:
        out = f.read().splitlines()
    return(out)
 except Exception as e:
    print("Something is wrong, using host file cache! ",e)
    with open('nechosts.cache','r') as f:
        out = f.read().splitlines()
    return(out)

hosts = hosts() 

def toDBif(vals: dict,now,host_ip,hostname):
    conn=psycopg2.connect(host="localhost",user="necradios",port=5432,password="redacted",dbname="necradios")
    c=conn.cursor()
    for k,v in vals.items():
        params = [now,hostname,host_ip,k,v]
        try:
           c.execute("""INSERT INTO necradios (timestamp,hostname, host_ip,ifindex,ifname) VALUES (%s, %s, %s, %s, %s)""", params)
           conn.commit()
        except psycopg2.Error as e:
           conn.rollback()
           print("Error inserting parameters to DB for " + hostname,host_ip,e)
    conn.close()

def toDB(vals: dict,counter,now,host_ip):
    conn=psycopg2.connect(host="localhost",user="necradios",port=5432,password="redacted",dbname="necradios")
    c=conn.cursor()
    for k,v in vals.items():
        try:
           c.execute("""UPDATE necradios set %s = %s where timestamp = %s and host_ip = %s and ifindex = %s""",(AsIs(counter),v,now,host_ip,k))
           conn.commit()
        except psycopg2.Error as e:
           conn.rollback()
           print("Error inserting parameters to DB for "+ hostname,host_ip, e)
    conn.close()

def getData(host_ip):
    now = globalnow
    session = Session(hostname=host_ip, community='public', version=2, retries=1, timeout=1)
    hostname = session.get('1.3.6.1.4.1.119.2.3.69.5.1.1.1.3.1').value
    ifnames = session.bulkwalk('1.3.6.1.2.1.31.1.1.1.1')
    ifdict = {rec.oid_index:rec.value for rec in ifnames}
    toDBif(ifdict,now,host_ip,hostname)
    rxoctetsdata = session.bulkwalk('1.3.6.1.4.1.119.2.3.69.501.11.1.2.2.1.3')
    rxoctets = {rec.oid.split('.')[-1]:rec.value for rec in rxoctetsdata}
    toDB(rxoctets,'rxoctets',now,host_ip)
    txoctetsdata = session.bulkwalk('1.3.6.1.4.1.119.2.3.69.501.11.1.2.2.1.5')
    txoctets = {rec.oid.split('.')[-1]:rec.value for rec in txoctetsdata}
    toDB(txoctets,'txoctets',now,host_ip)
    discardsdata = session.bulkwalk('1.3.6.1.4.1.119.2.3.69.501.11.1.2.2.1.67')
    discards = {rec.oid.split('.')[-1]:rec.value for rec in discardsdata}
    toDB(discards,'discards',now,host_ip)
    txpowerdata = session.bulkwalk('1.3.6.1.4.1.119.2.3.69.501.8.1.1.4')
    txpower = {rec.oid.split('.')[-1]:rec.value for rec in txpowerdata}
    toDB(txpower,'txpower',now,host_ip)
    rxleveldata = session.bulkwalk('1.3.6.1.4.1.119.2.3.69.501.8.1.1.6')
    rxlevel = {rec.oid.split('.')[-1]:rec.value for rec in rxleveldata}
    toDB(rxlevel,'rxlevel',now,host_ip)
    neighborhostdata = session.bulkwalk('1.0.8802.1.1.2.1.4.1.1.9')
    neighborhost = {rec.oid.split('.')[-2]:rec.value for rec in neighborhostdata}
    toDB(neighborhost,'neighborhost',now,host_ip)
    neighbormodemdata = session.bulkwalk('1.0.8802.1.1.2.1.4.1.1.8')
    neighbormodem = {rec.oid.split('.')[-2]:rec.value for rec in neighbormodemdata}
    toDB(neighbormodem,'neighbormodem',now,host_ip)
    print(hostname + ' processed')

def main():
    start_time = datetime.now()
    pool = multiprocessing.Pool(processes=10)
    for host in hosts:
       pool.apply_async(getData, args=(host,))
    pool.close()
    pool.join()
    print("\nElapsed time: " + str(datetime.now() - start_time))

if __name__ == "__main__":
    main()
