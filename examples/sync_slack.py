# To install
# pip install pymongo slack_sdk
import dataclasses as dc
import datetime
import hashlib
import os

import tqdm
import pymongo

from ochre import Component
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


class SlackSync(Component):
    db_name: str
    collection_name: str
    date_analysed: str | None = None
    channels: list[dict] = dc.field(default_factory=list)  # store id+name dicts
    user_map: dict = dc.field(default_factory=dict)        # user_id -> name

    def _get_messages(self, channel_id: str, channel_name: str, oldest: float = None):
        try:
            history = self.client.conversations_history(
                channel=channel_id,
                oldest=oldest,
                limit=1000,
            )
        except SlackApiError as e:
            print(f"Error fetching history for {channel_name}: {e}")
            return []

        messages = history.get("messages", [])
        data = []

        analysed = set(self.collection.distinct("unique_id"))

        for msg in tqdm.tqdm(messages, desc=f"Syncing #{channel_name}"):
            user_id = msg.get("user", "(unknown user)")
            text = msg.get("text", "")
            ts = msg.get("ts", "")

            # Convert Slack ts → datetime
            try:
                dt = datetime.datetime.fromtimestamp(float(ts))
            except ValueError:
                dt = None

            # Resolve user_id -> name
            user_name = self.user_map.get(user_id, user_id)

            raw_key = f"{user_id}|{text}|{ts}|{channel_id}"
            unique_id = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

            if unique_id in analysed:
                continue

            r = {
                "unique_id": unique_id,
                "user_id": user_id,
                "user_name": user_name,
                "text": text,
                "channel_id": channel_id,
                "channel_name": channel_name,
                "timestamp": dt,
            }
            data.append(r)

        return data

    def _get_service(self):
        token = os.environ.get("SLACK_BOT_TOKEN")
        if not token:
            raise RuntimeError("Environment variable SLACK_BOT_TOKEN must be set")
        return WebClient(token=token)

    def _load_channels(self):
        """Return only channels where the bot is a member, with id+name."""
        channels = []
        cursor = None
        while True:
            resp = self.client.users_conversations(
                types="public_channel,private_channel",
                cursor=cursor,
                limit=200,
            )
            for c in resp["channels"]:
                channels.append({"id": c["id"], "name": c.get("name", "(unknown)")})
            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break
        return channels

    def _load_users(self):
        """Build a map of user_id -> display name."""
        user_map = {}
        cursor = None
        while True:
            resp = self.client.users_list(cursor=cursor, limit=200)
            for u in resp["members"]:
                name = u.get("real_name") or u.get("name") or "(unknown)"
                user_map[u["id"]] = name
            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break
        return user_map

    def create(self):
        """Initial sync: last 30 days of history for all channels."""
        oldest = (datetime.datetime.now() - datetime.timedelta(days=30)).timestamp()
        for c in self.channels:
            data = self._get_messages(c["id"], c["name"], oldest=oldest)
            if data:
                self.collection.insert_many(data)

    def read(self):
        """Prepare DB + Slack client + channel list + user map."""
        self.mongo_client = pymongo.MongoClient("localhost", 27017)
        self.collection = self.mongo_client[self.db_name][self.collection_name]
        self.client = self._get_service()
        self.channels = self._load_channels()
        self.user_map = self._load_users()
        self.date_analysed = datetime.datetime.now().isoformat()

    def update(self):
        """Fetch last 24h of messages and insert new ones."""
        oldest = (datetime.datetime.now() - datetime.timedelta(hours=24)).timestamp()
        for c in self.channels:
            data = self._get_messages(c["id"], c["name"], oldest=oldest)
            for r in data:
                print(
                    f"Found new Slack message in #{r['channel_name']}:",
                    f"\"{r['text']}\"",
                    "from:",
                    r["user_name"],
                )
                self.collection.insert_one(r)

    def delete(self):
        """Drop the collection."""
        self.collection.drop()


main = SlackSync(
    db_name="agentdb",
    identifier="superduper",
    collection_name="slack_sync",
)
