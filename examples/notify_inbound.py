from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import pymongo
import typing as t

from openai import OpenAI
import tqdm
from pydantic import BaseModel

from ochre import Component


class AnalyseInbound(Component):
    json_schema: t.Type[BaseModel]
    filter: t.Callable | None= None
    sensitive: t.Callable | None = None
    condition: t.Callable | None = None
    db_name: str
    collection_name: str
    sync: Component
    model: str = "gpt-4.1"
    max_workers: int = 8
    cron: str = "*/1 * * * *"  # every 1 minute

    def notify(self, msg: str):
        cmd = f"osascript -e 'display dialog \"{msg}\"'"
        print(f"Running command: {cmd}")
        os.system(cmd)

    def analyse(self, text: str) -> dict:
        return self.openai_client.responses.parse(
            model="gpt-4.1",
            input=text,
            text_format=self.json_schema,
        ).output_parsed

    def process_record(self, r):
        if self.filter and not self.filter(r):
            analysis = {'summary': 'Filtered out', 'score': 1, 'kind': 'other'}
        elif r['body'].strip() == '':
            analysis = {'summary': 'Empty email', 'score': 1, 'kind': 'other'}
        elif self.sensitive and self.sensitive(r):
            self.notify(f"New sensitive email from {r['sender']}: {r['subject']}".replace('"', ""))
            analysis = {'summary': 'Sensitive email', 'score': 10, 'kind': 'other'}
        else:
            analysis = json.loads(self.analyse(r['sender'] + '\n' + r['body']).model_dump_json())

        if self.condition and self.condition(analysis):
            self.notify(
                f"New important email from {r['sender']}: {r['subject']}: {analysis['summary']}".replace('"', "")
            )

        self.collection.update_one({'_id': r['_id']}, {'$set': {'analysis': analysis}})
        return r['_id']  # return something for progress tracking

    def create(self):
        # Pull all candidate records upfront
        records = list(self.collection.find({'unread': True, 'analysis': {'$exists': False}}))
        if not records:
            print("No new records to process")
            return

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.process_record, r) for r in records]
            for f in tqdm.tqdm(as_completed(futures), total=len(futures)):
                try:
                    f.result()
                except Exception as e:
                    print(f"Error: {e}")

    def read(self):
        self.collection = pymongo.MongoClient()[self.db_name][self.collection_name]
        self.openai_client = OpenAI()

    def update(self):
        self.create()

    def delete(self):
        self.collection.update_many({}, {'$unset': {'analysis': ""}})