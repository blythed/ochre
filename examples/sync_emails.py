import dataclasses as dc
import datetime
import hashlib
import os
import base64
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import typing as t

import tqdm 

from ochre import Component

import pymongo

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


class GmailSync(Component):
    token_file: str
    credentials_file: str
    db_name: str
    collection_name: str
    sensitive_domains: t.List[str] = dc.field(default_factory=list)
    date_analysed: str | None = None 

    def _get_mails(self, query: str):
        results = self.service.users().messages().list(
            userId='me',
            q=query,
            maxResults=1000,
        ).execute()

        messages = results.get('messages', [])

        data = []

        for msg in tqdm.tqdm(messages):

            full_msg = self.service.users().messages().get(userId='me', id=msg['id'], format='full').execute()

            headers = {h['name']: h['value'] for h in full_msg['payload']['headers']}
            subject = headers.get('Subject', '(No Subject)')
            sender = headers.get('From', '(Unknown Sender)')
            date = headers.get('Date', '(No Date)')

            raw_key = f"{sender}|{subject}|{date}"
            unique_id = hashlib.sha256(raw_key.encode('utf-8')).hexdigest()

            analysed = self.collection.distinct('unique_id')

            if unique_id in analysed:
                continue

            body_text = self._get_plain_text(full_msg['payload'])
            body_text = '\n'.join([x for x in body_text.split('\n') if not x.startswith('>') and x.strip() != ''])

            labels = full_msg.get('labelIds', [])
            is_unread = 'UNREAD' in labels

            r = {
                "unique_id": unique_id,
                "sender": sender,
                "subject": subject,
                "date": date,
                "body": body_text,
                "unread": is_unread,
            }

            data.append(r)
        return data

    def create(self):
        data = self._get_mails('in:inbox newer_than:29d')
        for r in data:
            self.collection.insert_one(r)

    @staticmethod
    def _get_plain_text(payload) -> str:
        if payload.get('mimeType') == 'text/plain':
            return base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')
        if 'parts' in payload:
            for part in payload['parts']:
                text = GmailSync._get_plain_text(part)
                if text:
                    return text
        return ''

    def _get_service(self):
        creds = None
        if os.path.exists(self.token_file):
            creds = Credentials.from_authorized_user_file(self.token_file, SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.credentials_file, SCOPES)
                creds = flow.run_local_server(port=0)
            with open(self.token_file, 'w') as token:
                token.write(creds.to_json())
        return build('gmail', 'v1', credentials=creds)

    def read(self):
        self.mongo_client = pymongo.MongoClient('localhost', 27017)
        self.collection = self.mongo_client[self.db_name][self.collection_name]
        self.service = self._get_service()
        self.date_analysed = datetime.datetime.now().isoformat()

    def update(self):
        data = self._get_mails('is:unread in:inbox newer_than:1d')
        for r in data:
            print('Found new email: ', f"\"{r['subject']}\"", ' from: ', r['sender'])
            self.collection.insert_one(r)

    def delete(self):
        self.collection.drop()


main = GmailSync(
    token_file='.secrets/google-api-token.json',
    credentials_file='.secrets/google-api-credentials.json',
    db_name='agentdb',
    identifier='superduper',
    sensitive_domains=['hetz.vc', 'session.vc'],
    collection_name='email_sync',
)