import dataclasses as dc
import datetime
import hashlib
import os
from concurrent.futures import ThreadPoolExecutor

import tqdm
import pymongo
from pymongo import UpdateOne

from ochre import Component
from github import Github


class GitHubSync(Component):
    db_name: str
    collection_name: str
    repo_name: str  # e.g. "owner/repo"
    date_analysed: str | None = None

    # ---------- Helpers ----------
    def _hash_key(self, parts: list[str]) -> str:
        raw_key = "|".join(parts)
        return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    def _process_issue(self, issue):
        uid = self._hash_key([str(issue.id), "issue"])
        return {
            "unique_id": uid,
            "type": "issue",
            "number": issue.number,
            "title": issue.title,
            "body": issue.body or "",
            "user": issue.user.login if issue.user else None,
            "state": issue.state,
            "created_at": issue.created_at,
            "updated_at": issue.updated_at,
            "closed_at": issue.closed_at,
        }

    def _process_pr(self, pr):
        uid = self._hash_key([str(pr.id), "pr"])
        return {
            "unique_id": uid,
            "type": "pr",
            "number": pr.number,
            "title": pr.title,
            "body": pr.body or "",
            "user": pr.user.login if pr.user else None,
            "state": pr.state,
            "created_at": pr.created_at,
            "updated_at": pr.updated_at,
            "closed_at": pr.closed_at,
            "merged_at": pr.merged_at,
        }

    def _process_comment(self, comment, parent_type: str, parent_number: int):
        uid = self._hash_key([str(comment.id), "comment"])
        return {
            "unique_id": uid,
            "type": "comment",
            "parent_type": parent_type,
            "parent_number": parent_number,
            "body": comment.body or "",
            "user": comment.user.login if comment.user else None,
            "created_at": comment.created_at,
            "updated_at": comment.updated_at,
        }

    # ---------- Main sync ----------
    def create(self):
        """Initial sync: fetch issues, PRs, comments."""
        existing = set(self.collection.distinct("unique_id"))
        ops = []

        # Issues
        for issue in tqdm.tqdm(self.repo.get_issues(state="open"), desc="Syncing issues"):
            doc = self._process_issue(issue)
            if doc["unique_id"] not in existing:
                ops.append(UpdateOne({"unique_id": doc["unique_id"]}, {"$set": doc}, upsert=True))
            for c in issue.get_comments():
                cdoc = self._process_comment(c, "issue", issue.number)
                if cdoc["unique_id"] not in existing:
                    ops.append(UpdateOne({"unique_id": cdoc["unique_id"]}, {"$set": cdoc}, upsert=True))

        # PRs
        for pr in tqdm.tqdm(self.repo.get_pulls(state="open"), desc="Syncing PRs"):
            doc = self._process_pr(pr)
            if doc["unique_id"] not in existing:
                ops.append(UpdateOne({"unique_id": doc["unique_id"]}, {"$set": doc}, upsert=True))
            for c in pr.get_comments():
                cdoc = self._process_comment(c, "pr", pr.number)
                if cdoc["unique_id"] not in existing:
                    ops.append(UpdateOne({"unique_id": cdoc["unique_id"]}, {"$set": cdoc}, upsert=True))
            for c in pr.get_issue_comments():
                cdoc = self._process_comment(c, "pr", pr.number)
                if cdoc["unique_id"] not in existing:
                    ops.append(UpdateOne({"unique_id": cdoc["unique_id"]}, {"$set": cdoc}, upsert=True))

        if ops:
            self.collection.bulk_write(ops)

    def update(self):
        """Fetch only recent updates (last 24h)."""
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=24)
        ops = []

        # Issues (fast, supports `since`)
        issues = list(self.repo.get_issues(state="all", since=cutoff))
        for issue in tqdm.tqdm(issues, desc="Updating issues"):
            doc = self._process_issue(issue)
            ops.append(UpdateOne({"unique_id": doc["unique_id"]}, {"$set": doc}, upsert=True))
            for c in issue.get_comments(since=cutoff):
                cdoc = self._process_comment(c, "issue", issue.number)
                ops.append(UpdateOne({"unique_id": cdoc["unique_id"]}, {"$set": cdoc}, upsert=True))

        # PRs (use issues → as_pull_request)
        prs = [i.as_pull_request() for i in issues if i.pull_request is not None]

        def process_pr(pr):
            pr_ops = []
            if pr.updated_at < cutoff:
                return pr_ops
            doc = self._process_pr(pr)
            pr_ops.append(UpdateOne({"unique_id": doc["unique_id"]}, {"$set": doc}, upsert=True))
            for c in pr.get_comments():
                if c.created_at >= cutoff:
                    cdoc = self._process_comment(c, "pr", pr.number)
                    pr_ops.append(UpdateOne({"unique_id": cdoc["unique_id"]}, {"$set": cdoc}, upsert=True))
            for c in pr.get_issue_comments():
                if c.created_at >= cutoff:
                    cdoc = self._process_comment(c, "pr", pr.number)
                    pr_ops.append(UpdateOne({"unique_id": cdoc["unique_id"]}, {"$set": cdoc}, upsert=True))
            return pr_ops

        # Parallel fetch PR comments
        with ThreadPoolExecutor(max_workers=8) as ex:
            for result in tqdm.tqdm(ex.map(process_pr, prs), total=len(prs), desc="Updating PRs"):
                ops.extend(result)

        if ops:
            self.collection.bulk_write(ops)

    # ---------- Setup ----------
    def _get_service(self):
        token = os.environ.get("GITHUB_TOKEN")
        if not token:
            raise RuntimeError("Environment variable GITHUB_TOKEN must be set")
        return Github(token)

    def read(self):
        self.mongo_client = pymongo.MongoClient("localhost", 27017)
        self.collection = self.mongo_client[self.db_name][self.collection_name]
        self.client = self._get_service()
        self.repo = self.client.get_repo(self.repo_name)
        self.date_analysed = datetime.datetime.now().isoformat()

    def delete(self):
        self.collection.drop()


main = GitHubSync(
    db_name="agentdb",
    identifier="superduper",
    collection_name="github_sync",
    repo_name="superduper-io/superduper",  # replace with actual repo
)
