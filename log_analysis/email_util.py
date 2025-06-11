import smtplib
from socket import gaierror
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import yaml
import os

def read_cofig(confFile):
    with open(confFile.split('/')[-1], 'rb') as ConfigFile:
        confData = yaml.load(ConfigFile)
    return confData
       
def prepair_email(email_conf,error_message=None,root_cause="Unknown",solutions=None):
    message = MIMEMultipart("alternative")
    message["From"] = email_conf["From"]
    message["To"] = email_conf["To"]
    message["Subject"] = "LA Notification: Error in running {0} Oozie pipeline.".format(email_conf["projectName"])
    
    if error_message is None:
        text_body = MIMEText(" No error message found. ",'plain')
        html_body = MIMEText(" No error message found. ",'html')
    elif error_message is not None and solutions is None:
        text_template="""Hi Team,
            New error noticed and updated in erro databse.Please resolve and update the solution in knowledge base:
            Error Message: {0}
            """

        html_template="""<p>Hi Team,</p>
                    <p>New error noticed and updated in erro databse.Please resolve and update the solution in knowledge base:</p>
                    <p><strong>Error Message</strong>: {0}&nbsp;</p>
                    <p><br></p>
                    """
        text_body = MIMEText(text_template.format(error_message),'plain')
        html_body = MIMEText(html_template.format(error_message),'html')
               
    elif error_message is not None and solutions is not None:   
        text_template="""Hi Team,
                Please find the error message and the solution below:
                Error Message: {0}
                Root Cause: {1}
                Probable Solutions: 
                {2} """
    
        html_template="""<p>Hi Team,</p>
                    <p>Please find the error message and the solution below:</p>
                    <p><strong>Error Message</strong>: {0}</p>
                    <p><strong>Root Cause</strong>: <span style='color: rgb(0, 0, 0); font-family: "Times New Roman"; font-size: medium; font-style: normal; font-variant-ligatures: normal; font-variant-caps: normal; font-weight: 400; letter-spacing: normal; orphans: 2; text-align: start; text-indent: 0px; text-transform: none; white-space: normal; widows: 2; word-spacing: 0px; -webkit-text-stroke-width: 0px; text-decoration-style: initial; text-decoration-color: initial; display: inline !important; float: none;'>{1}</span></p>
                    <p><strong>Probable Solutions</strong>:&nbsp;</p>
                    <p><span style='color: rgb(0, 0, 0); font-family: "Times New Roman"; font-size: medium; font-style: normal; font-variant-ligatures: normal; font-variant-caps: normal; font-weight: 400; letter-spacing: normal; orphans: 2; text-align: start; text-indent: 0px; text-transform: none; white-space: normal; widows: 2; word-spacing: 0px; -webkit-text-stroke-width: 0px; text-decoration-style: initial; text-decoration-color: initial; display: inline !important; float: none;'>{2}</span>&nbsp;</p>
                    <p><br></p>
                    """        
        text_body = MIMEText(text_template.format(error_message,root_cause,solutions.to_html()),'plain')
        html_body = MIMEText(html_template.format(error_message,root_cause,solutions.to_html()),'html')
    
    message.attach(text_body)
    message.attach(html_body)  
    return message   
        
def attach_file(filePath,message):
        with open(filePath, "rb") as attachment:
            part = MIMEBase('text', 'html')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            param="attachment; filename={0}".format(filePath.split('\\')[-1])
            part.add_header("Content-Disposition",param)
            message.attach(part)        
        return message
    
def send_mail(smtpHost,port,email_conf,errorMessage=None,rootCause=None,solution=None,logFile=None):
    try:
        if errorMessage is not None and solution is not None:
             mail_body = prepair_email(email_conf,errorMessage," ",solution)
        elif errorMessage and solution is None:
            mail_body = prepair_email(email_conf,errorMessage)         
        
        if logFile:
            if os.path.exists(logFile):
                mail_body = attach_file(logFile,mail_body)
        print(mail_body.as_string())    
        with smtplib.SMTP(smtpHost, port) as smtp_connection:
            smtp_connection = smtplib.SMTP('smtpgw.merckgroup.com', 25)
            smtp_connection.starttls()
            smtp_connection.sendmail(email_conf["From"],email_conf["To"], mail_body.as_string()) 
    except (gaierror, ConnectionRefusedError):
        print('Failed to connect to the server. Bad connection settings?')
        raise()
    except smtplib.SMTPServerDisconnected:
        print('Failed to connect to the server. Wrong user/password?')
        raise()
    except smtplib.SMTPException as e:
        print('SMTP error occurred: ' + str(e))
        raise()