import time
import imaplib
import email
# import requests
import re
import datetime
# import sys
import os
# import MySQLdb
# from sqlalchemy import create_engine
import smtplib  
import email.utils
import shutil
import pandas as pd
import numpy as np
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
# from email.mime.application import MIMEApplication
# from email.mime.base import MIMEBase
# from email import encoders
from datetime import timedelta, datetime 
# import pymysql
from bigquery_utils import *
from utils import *

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../bq-secrets.json'

LOG_TABLE = ''
FINAL_LABEL = ''

ist = datetime.now() + timedelta(minutes=330)
start= ist - timedelta(minutes=36) #changed from 38 to 33
end= ist - timedelta(minutes=6) #changed from 8 to 3
start = start.strftime('%d-%m-%Y %H:%M')
end = end.strftime('%d-%m-%Y %H:%M')

RECIPIENT=['vaishali.kulkarni@atidiv.com','yogesh.kumar@atidiv.com','Yelpams@Atidiv.com','Teamlead@Atidiv.com']
dev=['vaishali.kulkarni@atidiv.com']

def sendEmail(subject, body, RECIPIENT, retry):
    # if error retry 3 times
    if(retry <= 3):
        COMMASPACE = ', '
        print("sending email..")
        st = time.time()
        try:
            # Replace sender@example.com with your "From" address.
            # This address must be verified.
            SENDER = 'Splunk Reporting<no-reply@atidiv.com>'
            SENDERNAME = 'Splunk Reporting'

            # Replace smtp_username with your Amazon SES SMTP user name.
            # USERNAME_SMTP = "AKIAJSNHI4BU4UF3NIKA"

            # Replace smtp_password with your Amazon SES SMTP password.
            # PASSWORD_SMTP = "AiZ8yWKZ9VBgPqhQqpbekPcCjKSfI4ci0lTxjBsiMAye"

            # If you're using Amazon SES in an AWS Region other than US West (Oregon),
            # replace email-smtp.us-west-2.amazonaws.com with the Amazon SES SMTP
            # endpoint in the appropriate region.
            # HOST = "email-smtp.us-west-2.amazonaws.com"

            USERNAME_SMTP = 'no-reply@atidiv.com' 
            PASSWORD_SMTP = 'rbEW.A%[Z[Q2Ur>'
            HOST = 'smtp.gmail.com'

            PORT = 587

            # Create the enclosing (outer) message
            outer = MIMEMultipart()
            outer['Subject'] = subject

            text = body
            outer.attach(
                MIMEText(text, 'html'))  # or 'plain'

            outer['To'] = COMMASPACE.join(RECIPIENT)
            #outer['bcc'] = bcc
            outer['From'] = SENDER
            outer.preamble = 'You will not see this in a MIME-aware mail reader.\\n'
            composed = outer.as_string()

            # Send the email
            with smtplib.SMTP(HOST, PORT) as s:
                s.ehlo()
                s.starttls()
                s.ehlo()
                s.login(USERNAME_SMTP, PASSWORD_SMTP)
                s.sendmail(SENDER, RECIPIENT, composed)
                s.close()
            time_in_min = round((time.time()-st)/60, 2)
            print("Email sent! Time taken : %.2f mins" % (time_in_min))
        except Exception as e:
            if hasattr(e, 'message'):
                msg = e.message
            else:
                msg = e
            print("Unable to send the email. Error: ", msg)
            retry = retry+1
            #sendEmail(subject, body, RECIPIENT, retry)


pattern_uid = re.compile('\d+ \(UID (?P<uid>\d+)\)')

def parse_uid(data):
	match = pattern_uid.match(data)
	return match.group('uid')
	
def makecopy(inputfile):
	try:
		# import ipdb; ipdb.set_trace()

		path1='';
		src =  inputfile
		dest = "spam\\" + inputfile;
		shutil.copyfile(src, dest)
		f=pd.read_csv(src)
		keep_col = ['Enc Business ID','business_id','attr','value_submitted','note_by_user','action','moderator_id','Email Address','time_acted','time_acted_readable','reason','time_created','time_created_readable','source.ip']
		fnew = f[keep_col]
		fnew.to_csv(src, index=False)
		fileToUpload = src
		print(fileToUpload)
		return src
	except Exception as e:
		print(str(e))

def read_email_from_gmail():
	fileToUpload=''
	file_name=''
	scriptExecuted=0	
	today_str = datetime.now().strftime('%m-%d-%Y_%H%M%S')

	# path1='C:\\Users\\Syed\\Documents\\internal'
	path1=''

	mail = imaplib.IMAP4_SSL('imap.gmail.com')
	mail.login('splunk.integration@atidiv.com','kboqacnbupfdawoc')
	mail.select(LOG_TABLE)

	_ , data = mail.search(None, 'ALL')
	
	# This flag stops script from moving email to processed label
	# when en error is encountered while moving its data to BQ 
	skipMigration = False 
	
	mail_ids = data[0]
	id_list = mail_ids.split()  
	if id_list:
		 
		first_email_id = int(id_list[0])
		latest_email_id = int(id_list[-1])
		processed_emails = 0
		# for attachments in 1 email flow is -> read, download, process, move to spam_processed label
		for i in range(latest_email_id,first_email_id-1, -1):		 
			_ , data = mail.fetch(str(i).encode("utf-8"), '(RFC822)' )
			if(processed_emails >= 2):
				break
			# processed_emails += 1
			skipMigration = False

			for response_part in data:
				try:
					if isinstance(response_part, tuple):
						msg = email.message_from_string(response_part[1].decode())
						email_subject = msg['subject']
						email_from = msg['from']
						print ('Subject : ' + email_subject)
						if (email_from == 'splunk via Splunk_integration <splunk_integration@atidiv.com>'):
							for part in msg.walk():		
										
								if part.get_content_maintype() == 'multipart':
									continue
								if part.get('Content-Disposition') is None:
									continue

								filename = part.get_filename()
								today_str = (datetime.now() + timedelta(minutes=330)).strftime('%m-%d-%Y_%H%M%S')
								file_name=today_str+"_"+filename

								att_path = os.path.join(path1, file_name)
								fileToUpload= file_name

								#move original file to different location after removing extra biz_enc column	
								# print(fileToUpload) 
								if not os.path.isfile(att_path):
									print('file downloaded')
									fp = open(file_name, 'wb')
									fp.write(part.get_payload(decode=True))
									fp.close()

									df = pd.read_csv(file_name)
									df['pull_date'] = datetime.now()
									if(len(df)>0):
										try:
											print('starting to load file...')
											# import ipdb; ipdb.set_trace()
											df1 = standardize_column(df)

											# df_cols = df1.columns
											# db_data = gcp2df('select * from {}.{} limit 1'.format(bq_dataset, LOG_TABLE))
											# db_cols = db_data.columns
											# import ipdb; ipdb.set_trace()
											# new_cols = list(set(df_cols) - set(db_cols))
											# df1.drop(axis=1, columns=new_cols, inplace=True)
											
											df2gcp(df1, LOG_TABLE, mode='append')
											time.sleep(2)

										except Exception as e:
												print(str(e))
												# skipMigration = True
												time.sleep(2)
																	
								if(len(df)==0):
									body='No spam report received from splunk'
									print(body)
				except Exception as e:
						print(str(e))
						skipMigration = True
						time.sleep(2)

			# if skipMigration: continue
			# import ipdb; ipdb.set_trace()
			# this section moves read email to Report_Processed folder
			try:
				_ , data1 = mail.fetch(str(i).encode("utf-8"), "(UID)")
				msg_uid = parse_uid(data1[0].decode())
				result = mail.uid('COPY', msg_uid, FINAL_LABEL)
				if result[0] == 'OK':
					_ , data1 = mail.uid('STORE', msg_uid , '+FLAGS', '(\Deleted)')
					mail.expunge()
				print("processed emails moved to {} label\n".format(FINAL_LABEL))	
			except Exception as e:
				print(str(e))
				#sendEmail("Error in moving processed emails to Spam_Processed label", str(e), dev, 1)
			# import ipdb; ipdb.set_trace()
		
	if(fileToUpload==''):
		body='No spam report received'
		print(body)
		#sendEmail("Daily Spam Notes not received from Splunk",body,RECIPIENT, 1)

if __name__=="__main__":
	
	labels_to_process = [
		'incorrect_vl', 
		'advertiser_nba', 
		'ROCS_Handled',
		'ROCS-Accepts'
	]

	processed_label = {
		'incorrect_vl': 'incorrect_vl_processed',
		'advertiser_nba': 'advertiser_nba_processed',
		'ROCS_Handled': 'ROCS_Handled_Processed',
		'ROCS-Accepts': 'ROCS-Accepts-Handled'
	}
	# import ipdb; ipdb.set_trace()

	for elm in labels_to_process:

		LOG_TABLE = elm
		FINAL_LABEL = processed_label[elm]

		today_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		today_str_ist = (datetime.now() + timedelta(minutes=330)).strftime('%Y-%m-%d %H:%M:%S')
		print("Current datetime- UTC: {0} and IST: {1}".format(today_str,today_str_ist))
		st_main = time.time()
		read_email_from_gmail()
		print("Total time taken : %.2f mins" %(round((time.time() - st_main)/60,2)))

