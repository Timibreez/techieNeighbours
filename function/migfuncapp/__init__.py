import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    # Get connection to database
    connection = psycopg2.connect(
        host="techconfdbserver.postgres.database.azure.com",
        dbname="techconfdb",
        user="Timibreez@techconfdbserver",
        password="Adegbe01"
        )

    try:
        cursor = connection.cursor()
        # Get notification message and subject from database using the notification_id
        query = cursor.execute("SELECT message, subject FROM notification WHERE id = {};".format(notification_id))

        # Get attendees email and name
        cursor.execute("SELECT first_name, last_name, email FROM attendee;")
        attendees = cursor.fetchall()

        #  Loop through each attendee and send an email with a personalized subject
        for attendee in attendees:
            Mail('{}, {}, {}'.format({'Timibreez@techconf.com'}, {attendee[2]}, {query}))

        # Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        completed_date = datetime.utcnow()
        status = 'Notified {} attendees'.format(len(attendees))
        update_query = cursor.execute("UPDATE notification SET status = '{}', completed_date = '{}' WHERE id = {};".format(status, completed_date, notification_id))
        connection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # Close connection
        cursor.close()
        connection.close()