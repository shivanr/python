from elastic_utils import elastic_utils

pem_file=r"C:\installed\your_pem-SB.pem"
host_name='deda1x3263.merckgroup.com'
username='aa-nlp'
password='aa-nlp'
es=elastic_utils(pem_file,host_name,username,password)
elasticSession=es.connect_elasticsearch()
elasticSession
err="""applicationmaster: user class threw exception: java.lang.reflect.invocationtargetexception  java.lang.reflect.invocationtargetexception caused by: java.lang.numberformatexception: for input string: """""
es_response=es.search_kb(elasticSession,err)
solutions = es.parse_response(es_response)
solutions.head(5)
es.insert_error(elasticSession,err)
elasticSession.index(index="unresolved_errors", doc_type="_doc", body={"ERR_MESSAGE":err})

#=======================================================================
import email_util as eu
port = 25 
smtp_server = "smtpgw.merckgroup.com"
log_file= r"C:\Users\your_folders\1application_1568810042014_190726.log"
email_conf = eu.read_cofig(r"C:\Users\your_folder\code\project_email_contact_list.yml")
#email_conf["test_project"]
mail_body   = eu.prepair_email(email_conf["test_project"],"Test Error","test"," Some Solutions")
mail_with_attachment = eu.attach_file(log_file,mail_body)
eu.send_mail(smtp_server,port,mail_with_attachment.as_string())

import os
if os.path.exists(log_file):
    print("File Exists")
else:
    print("Not exists")
    
#======================================================================= 

log_file_path=r"C:\Users\your_folder\data\la_runlog.log"

import logging
logging.basicConfig(filename=log_file_path,level=logging.DEBUG,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.debug('This message should go to the log file')
logging.info('So should this')
logging.warning('And this, too')
logging.error('this is error {0}')