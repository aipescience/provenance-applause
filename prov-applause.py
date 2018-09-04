import time
import sys
import json
import pandas
import prov
import uws.UWS.client as client
import uws.UWS.errors as uwserror
from prov.model import ProvDocument
from prov.dot import prov_to_dot

with open('plate-archive.org.json') as data_file:
        username, password = json.load(data_file).values()


# check for double entries
archives = []
processes = []
institutes = []

def get_client():    
    url = 'https://www.plate-archive.org/uws/query'
    cli = client.Client(url, username, password)
    return cli

def submit_query(client, query, queue):
    parameters = {'query': query, 'queue': queue}
    job = client.new_job(parameters)
    time.sleep(2)
    run = client.run_job(job.job_id)

    return run

def get_data(client, run, username, password, wait='30',
             filename='res.csv'):
    time.sleep(1)
    job = client.get_job(run.job_id, wait=wait, phase='QUEUED')

    if job.phase[0] == 'COMPLETED':
        fileurl = str(job.results[0].reference)
        client.connection.download_file(fileurl, username, password,
                                        file_name=filename)
        data = pandas.read_csv(filename)
        
        # delete the jobs afterwords
        try:
            success = client.delete_job(job.job_id)
        except uwserror.UWSError:
            print('Cannot delete the job')

        print 'Job is %s' % (job.phase[0])
        return data
    else:
        print 'Job is %s' % (job.phase[0])


def declare_namespaces(prov_doc):
    prov_doc.add_namespace('applause', 'https://www.plate-archive.org/')
    prov_doc.add_namespace('archive', 'https://www.plate-archive.org/applause/documentation/dr2/tables/archive/')
    prov_doc.add_namespace('institute', 'https://www.plate-archive.org/institutes')
    prov_doc.add_namespace('plate', 'https://www.plate-archive.org/applause/documentation/dr2/tables/plate/')
    prov_doc.add_namespace('scan', 'https://www.plate-archive.org/applause/documentation/dr2/tables//')
    prov_doc.add_namespace('pyplate', 'https://www.plate-archive.org/applause/project/pyplate/')
    prov_doc.add_namespace('process', 'https://www.plate-archive.org/applause/documentation/dr2/tables/process/')
    prov_doc.add_namespace('logbook', 'https://www.plate-archive.org/applause/documentation/dr2/tables/logbook/')
    prov_doc.add_namespace('logpage', 'https://www.plate-archive.org/applause/documentation/dr2/tables/logpage/')
    prov_doc.add_namespace('source', 'https://www.plate-archive.org/applause/documentation/dr2/tables/source/')
    prov_doc.add_namespace('lightcurve', 'https://www.plate-archive.org/applause/documentation/dr2/tables/lightcurve/')


# get all the archives and institutes
def get_all_archives(prov_doc):
    query = 'select * from APPLAUSE_DR2.archive'
    cli = get_client()
    run = submit_query(cli, query, queue='long')
    archives = get_data(cli, run, username, password)

    for i in list(set(archives['institute'])):
        i = unicode(i, errors='replace')
        print(i)
        prov_doc.agent('institute:'+i)

    for (name, id, i) in zip(archives['archive_name'], archives['archive_id'], archives['institute']):
        e = d1.entity('archive:'+name,{'prov:label':name,'prov:id':id, 'prov:type':'Collection'})
        i = unicode(i, errors='replace')
        d1.wasAttributedTo(e, 'institute:'+i)
    return 'archives'


def get_archive(archive_id, prov_doc):
    if archive_id in archives:
        archive_ident = 'archive:'+ archive_id  
    else:
        query = 'select * from APPLAUSE_DR2.archive where archive_id = ' + archive_id
        cli = get_client()
        run = submit_query(cli, query, queue='long')
        archive = get_data(cli, run, username, password)
        print(archive)
        # get institute name
        i = archive['institute'][0]
        i = unicode(i, errors='replace')
        prov_doc.agent('institute:'+i)
        archive_ident = 'archive:'+ archive_id
        e = prov_doc.entity(archive_ident,{'prov:label':archive['archive_name'][0],'prov:id':id, 'prov:type':'Collection'})
        prov_doc.wasAttributedTo(e, 'institute:'+i)
        archives.append(archive_id)
    return archive_ident



def get_plate(plate_id, prov_doc):
    query = 'select * from APPLAUSE_DR2.plate where plate_id = '+ plate_id
    cli = get_client()
    run = submit_query(cli, query, queue='long')
    plate = get_data(cli, run, username, password)
    print(plate)
    a = plate['archive_id'][0]
    archive_name = get_archive(str(a),prov_doc)
    print(archive_name)
    ident = 'plate:' + str(plate['plate_id'][0])
    print(ident)
    prov_doc.entity(ident, {'prov:label':plate['plate_num'][0],'prov:id':plate_id, 'prov:type':'Plate'})
    prov_doc.hadMember(str(archive_name), ident)
    return ident


def get_scan(scan_id, prov_doc):
    query = 'select * from APPLAUSE_DR2.scan where scan_id = '+ scan_id
    cli = get_client()
    run = submit_query(cli, query, queue='long')
    data = get_data(cli, run, username, password)
    if data is None:
        print('data is none')
        return None
    print(data)
    scan_ident = 'scan:' + str(data['scan_id'][0])
    prov_doc.entity(scan_ident, {'prov:label':data['scan_id'][0],'prov:id':scan_id, 'prov:type':'Scan'})
    plate_ident = 'plate:'+ str(data['plate_id'][0])
    prov_doc.wasDerivedFrom(plate_ident, scan_ident)
    return scan_ident

def get_logbook(logbook_id, prov_doc):
    query = 'select * from APPLAUSE_DR2.logbook where logbook_id = '+ logbook_id
    cli = get_client()
    run = submit_query(cli, query, queue='long')
    data = get_data(cli, run, username, password)
    print(data)
    logbook_ident = 'logbook:' + logbook_id
    print(logbook_ident)
    prov_doc.entity(logbook_ident, {'prov:label':data['logbook_title'][0],'prov:id':logbook_id, 'prov:type':'Collection'})
    archive_ident = 'archive:'+ str(data['archive_id'][0])
    prov_doc.hadMember(archive_ident, logbook_ident)
    return logbook_ident


def get_plate_logpage(logpage_id, prov_doc):
    query = 'select * from APPLAUSE_DR2.plage_logpage where logpage_id = '+ logpage_id
    cli = get_client()
    run = submit_query(cli, query, queue='long')
    data = get_data(cli, run, username, password)
    print(data)
    logpage_ident = 'logpage:' + logpage_id
    print(logpage_ident)
    prov_doc.entity(logpage_ident, {'prov:label':logpage_id,'prov:id':logpage_id, 'prov:type':'Logpage'})
    return logpage_id

def get_logpage(logpage_id, prov_doc):
    query = 'select * from APPLAUSE_DR2.logpage where logpage_id = '+ logpage_id
    cli = get_client()
    run = submit_query(cli, query, queue='long')
    data = get_data(cli, run, username, password)
    print(data)
    logpage_ident = 'logpage:' + logpage_id
    print(logpage_ident)
    prov_doc.entity(logpage_ident, {'prov:label':logpage_id,'prov:id':logpage_id, 'prov:type':'Logpage'})
    print(data['logbook_id'][0])
    if pandas.isnull(data['logbook_id'][0]):
        archive_ident = 'archive:'+ str(data['archive_id'][0])
        prov_doc.hadMember(archive_ident, logpage_ident)
    else:
        logbook_ident = get_logbook(str(data['logbook_id'][0]), prov_doc)
        prov_doc.hadMember(logbook_ident, logpage_ident)

    return logpage_id

def get_source(source_id, prov_doc):
    query = 'select * from APPLAUSE_DR2.source where source_id = '+ source_id
    cli = get_client()
    run = submit_query(cli, query, queue='long')
    data = get_data(cli, run, username, password)
    print(data)
    source_ident = 'source:'+ source_id
    print(source_ident)
    prov_doc.entity(source_ident, {'prov:label':source_id,'prov:id':source_id, 'prov:type':'Source'})
    process_ident = get_process(str(data['process_id'][0]), prov_doc)
    plate_ident = get_plate(str(data['plate_id'][0]), prov_doc)
    prov_doc.wasDerivedFrom(source_ident, plate_ident)
    prov_doc.wasGeneratedBy(process_ident, source_ident)

    return source_ident
    

def get_process(proc_id, prov_doc):
    query = 'select * from APPLAUSE_DR2.process where process_id = '+ proc_id
    cli = get_client()
    run = submit_query(cli, query, queue='long')
    data = get_data(cli, run, username, password)
    print(data)

    proc_ident = 'process:' + str(data['process_id'][0])
    prov_doc.activity(proc_ident)

    plate_ident = 'plate:' + str(data['plate_id'][0])

    for scan in data['scan_id']:
        scan_ident = get_scan(str(scan), prov_doc)
        prov_doc.used(proc_ident, scan_ident)
        #prov_doc.wasGeneratedBy(plate_ident, proc_ident)
        prov_doc.used(proc_ident, plate_ident)
    return proc_ident 
 

def get_lightcurve(ucac4_id, prov_doc):
    query = 'select * from APPLAUSE_DR2.lightcurve where ucac4_id like \'' + ucac4_id +'\''
    cli = get_client()
    run = submit_query(cli, query, queue='long')
    data = get_data(cli, run, username, password)
    print(data)
    lightcurve_ident = 'lighturve:' + '614' # ucac4_id
    print(lightcurve_ident)
    prov_doc.entity('lightcurve:614', {'prov:label':ucac4_id, 'prov:id':ucac4_id,'prov:type':'Lightcurve'})
    for source in data['source_id']:
        try:
            source_ident = get_source(str(source), prov_doc)
            prov_doc.hadMember('lightcurve:614-', source_ident)
        except TypeError:
            print('Source %s could not be retrieved, proceeding...' %  str(source))
    return lightcurve_ident


def get_entity(id, entity_type, prov_doc):
    if entity_type == 'scan':
        return get_scan(id, prov_doc)
    elif entity_type == 'plate':
        return get_plate(id, prov_doc)
    elif entity_type == 'lightcurve':
        return get_lightcurve(id, prov_doc)
    elif (entity_type == 'archive'):
        return get_archive(id, prov_doc)
    else:
        raise ValueError()


def get_plate_prov(plate_id,prov_doc):
    
    #get the plate entity, archives and institutes
    plate_ident = get_plate(plate_id, prov_doc)

    # get the processes
    query = 'select * from APPLAUSE_DR2.process where plate_id = '+ plate_id
    cli = get_client()
    run = submit_query(cli, query, queue='long')
    data = get_data(cli, run, username, password)
    for process in data['process_id']:
        process_ident = get_process(str(process), prov_doc)
    
    # get logbook, pages
    query = 'select * from APPLAUSE_DR2.plate_logpage where plate_id = '+ plate_id
    cli = get_client()
    run = submit_query(cli, query, queue='long')
    data = get_data(cli, run, username, password)
    for logpage in data['logpage_id']:
        logpage_ident = get_logpage(str(logpage), prov_doc)

    return plate_ident


# Create a new provenance document
d1 = ProvDocument()
declare_namespaces(d1)
# get V468Cyg
# get_plate
# process = get_process('2180', d1)
try:
    # scan = get_entity('2462','scan', d1)
    id = '2180'
    prov_type = 'lightcurve'
    # plate_name = get_entity(id,prov_type, d1)
    # process_name = get_process('9804',d1)
    # logpage_name = get_logpage('10085',d1)
    # source_id = get_source('40000001', d1)
    # plate_ident = get_plate_prov(id, d1)
    id = get_lightcurve('614-089373', d1)
except TypeError:
    print('the job is still executing...')



print(d1.get_provn())

filename = 'prov_'+ prov_type + id
d1.serialize(filename + '.xml', format='xml')
dot = prov_to_dot(d1)
dot.write_png(filename + '.png')
