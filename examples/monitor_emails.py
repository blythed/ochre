# pip install pydantic
from examples.sync_emails import GmailSync
from examples.notify_inbound import AnalyseInbound

from pydantic import BaseModel, Field


class Analysis(BaseModel):
    summary: str = Field(
        ...,
        description="A concise summary of the email content, no more than 20 words."
    )
    score: int = Field(
        ...,
        ge=1,
        le=10,
        description="A score from 1 to 10 indicating the importance of the email, 10 being the most important. Take consideration of the sender's domain and content."
    )
    kind: str = Field(
        ...,
        description="The category that best describes the email.", enum=["promotional", "personal", "business", "meeting", "other"]
    )


main = AnalyseInbound(
    'email_analyser',
    json_schema=Analysis,
    db_name='agentdb',
    collection_name='email_sync',
    condition=lambda analysis: analysis['score'] >= 8,
    cron="*/5 * * * *",  # every 5 minutes
    sync=GmailSync(
        'email_sync',
        token_file='.secrets/google-api-token.json',
        credentials_file='.secrets/google-api-credentials.json',
        db_name='agentdb',
        collection_name='email_sync',
    )
)