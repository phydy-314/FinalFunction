from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

SENDGRID_API_KEY = "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"
EMAIL_TO = "vynnp22416c@st.uel.edu.vn"
EMAIL_FROM = "phuongvynguyenngoc97@gmail.com"

message = Mail(
    from_email=EMAIL_FROM,
    to_emails=EMAIL_TO,
    subject="Test SendGrid",
    html_content="Nếu bạn nhận được email này, cấu hình SendGrid đã đúng!"
)

try:
    sg = SendGridAPIClient(SENDGRID_API_KEY)
    response = sg.send(message)
    print(f"✅ SendGrid Response: {response.status_code}")
except Exception as e:
    print(f"❌ Failed to send email: {e}")

