import os
import smtplib
import time
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv
from plyer import notification
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

load_dotenv()

# Email configuration
SENDER_NAME = os.getenv("SENDER_NAME")
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
RECIPIENT_ADDRESS = os.getenv("RECIPIENT_ADDRESS")
CC_LIST = os.getenv("CC_LIST").split(',')

# Folder to monitor
FOLDER_PATHS = os.getenv("FOLDER_PATHS").split(',')


class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return

        file_path = event.src_path
        file_name = os.path.basename(file_path)
        folder_name = os.path.basename(os.path.dirname(file_path))

        if file_name.endswith('.csv'):
            # Generate new file name with folder name and current date/time
            current_datetime = datetime.now().strftime('%d-%b %I-%M-%S %p')
            new_file_name = f"Bank Statement {folder_name} {current_datetime}.csv"

            # Construct new file path with the renamed file
            new_file_path = os.path.join(os.path.dirname(file_path), new_file_name)

            time.sleep(1)
            # Rename the file
            os.rename(file_path, new_file_path)

            # Call send_email with the new file name and path
            send_email(new_file_name, new_file_path)


# Function to create directory if it doesn't exist
def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")


def send_notification(status, file_name):
    kwargs = {
        'title': f"{status}: Bank Statement Email",
        'message': f"Emailing {status}:'{file_name}'",
        'app_icon': os.path.join(os.path.dirname(os.path.realpath(__file__)), f'icon-{status.lower()}.ico')
    }
    notification.notify(**kwargs)


def send_email(file_name, file_path):
    # Create email message
    message = MIMEMultipart()
    message['From'] = SENDER_NAME
    message['To'] = RECIPIENT_ADDRESS
    message['Cc'] = ', '.join(CC_LIST)
    message['Subject'] = file_name
    body = f'Please find the latest Bank Statement Attachment.'
    message.attach(MIMEText(body, 'plain'))

    # Attach CSV file
    with open(file_path, 'rb') as attachment:
        part = MIMEBase('text', 'csv')
        part.set_payload(attachment.read())

    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename= {file_name}')
    message.attach(part)

    try:
        # Connect to SMTP server and send email
        print(f'Sending mail {file_name}')

        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.send_message(message)

        print(f'Mail sent: {file_name}')
        send_notification('Success', file_name)

    except Exception as e:
        print(f"Error sending email: {e}")
        send_notification('Failed', file_name)


if __name__ == "__main__":
    observers = []

    # Create directories if they don't exist before observing them
    for folder_path in FOLDER_PATHS:
        create_directory(folder_path)

    for folder_path in FOLDER_PATHS:
        print(f'Start watching directory {folder_path}')
        observer = Observer()
        observer.schedule(FileHandler(), folder_path, recursive=True)
        observer.start()
        observers.append(observer)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for observer in observers:
            observer.stop()

    for observer in observers:
        observer.join()
