#!/usr/bin/python3
import xmlrpc.client
from unidecode import unidecode
from python_freeipa import Client
from python_freeipa import exceptions
import datetime

weekago = (datetime.datetime.now() - datetime.timedelta(days = 7)).strftime('%Y-%m-%d %H:%M:%S')

ourl = 'https://hostname'
domain = 'domain'
db = 'db'
username = 'user'
password = 'redacted'

client = Client('ipahost', version='2.230')
client.login('autoprovisioner', 'redacted')

common = xmlrpc.client.ServerProxy('https://{}/xmlrpc/2/common'.format(domain))
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(ourl))

circs = models.execute_kw(db, uid, password, 'o2.circuit', 'search_read',[[['write_date', '>', weekago]]],{'fields': ['write_date','write_uid','name','partner','site_b','status','tech_attributes_id'],'limit':0})

def ipaMod(params):
   user = params[0].lstrip().rstrip()
   phonenum = params[1].lstrip().rstrip()
   orgunit = params[2].lstrip().rstrip()
   streetaddr = params[3].lstrip().rstrip()
   out = client.user_mod(user,telephonenumber=phonenum,ou=orgunit,street=streetaddr)
   return(out)

for circ in circs:
  cid = circ['name']
  cust = unidecode(circ['partner'][1]).replace(',','')
  siteb = unidecode(circ['site_b'][1]).replace(',','')
  wdate = circ['write_date']
  wuid = circ['write_uid'][0]
  wmail = models.execute_kw(db, uid, password, 'res.users', 'search_read',[[['id', '=', wuid]]],{'fields': ['email'],'limit':0})[0]['email']
  tids = circ['tech_attributes_id']
  status = circ['status']
  attrs = models.execute_kw(db, uid, password, 'o2.circuit.attributes', 'read', [tids])
  out = {}
  for attr in attrs:
    out[attr['name']] = attr['value']
  try:
    if out['User Name (login)'] != False:
      try:
       params=[out['User Name (login)'],cid,cust,siteb]
       ipaout = ipaMod(params)
       print(ipaout)
      except (exceptions.NotFound,exceptions.BadRequest) as e:
        print(e)

  except KeyError as e:
    pass

