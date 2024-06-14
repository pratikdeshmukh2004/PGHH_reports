import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import dotenv
import os

# Load environment variables
dotenv.load_dotenv()

# Email Configuration
smtp_server = 'smtp.gmail.com'
smtp_port = 587
sender_email = os.getenv("SENDER_EMAIL")
password = os.getenv("SENDER_PASSWORD")  # Use an App Password for Gmail if 2FA is enabled.

def send_email(subject, receiver, body, files, cc=[]):
    """
    Sends an email with the specified subject, body, and attachments to the receiver, with optional CC.

    :param subject: Subject of the email
    :param receiver: Main recipient's email address
    :param body: Body of the email
    :param files: List of file paths to attach
    :param cc: List of CC email addresses (optional)
    """
    # Create the Email
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = receiver
    message['Subject'] = subject
    if cc:
        message['CC'] = ', '.join(cc)  # Add CC recipients

    # Email body
    message.attach(MIMEText(body, 'plain'))

    # Attach each CSV file
    for file in files:
        try:
            with open(file, 'rb') as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(file)}"')
                message.attach(part)
        except Exception as e:
            print(f"Could not attach file {file}: {e}")

    # Send the Email
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # Secure the connection
            server.login(sender_email, password)
            # Combine all recipients
            to_addresses = [receiver] + cc
            server.sendmail(sender_email, to_addresses, message.as_string())
            print(f"Email sent to {receiver} with CC to {', '.join(cc)}")
            return True
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False
