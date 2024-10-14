from postmarker.core import PostmarkClient  # pylint: disable=import-error
import os

POSTMARK_API_KEY = os.getenv('POSTMARK_API_KEY', 'your-postmark-api-key')

# Initialize the Postmark Client
postmark = PostmarkClient(server_token=POSTMARK_API_KEY)
