import os, requests
import dotenv

dotenv.load_dotenv()

REPO = "superduper-io/superduper"
TOKEN = os.environ["GITHUB_TOKEN"]
headers = {"Accept":"application/vnd.github+json"}
if TOKEN:
    headers["Authorization"] = f"Bearer {TOKEN}"

def get_json(url, params=None, headers=headers):
    r = requests.get(url, headers=headers, params=params); r.raise_for_status(); return r.json(), r

# Stars & forks
repo, _ = get_json(f"https://api.github.com/repos/{REPO}")
stars = repo["stargazers_count"]
forks = repo["forks_count"]

# Open issues (exclude PRs)
search, _ = get_json("https://api.github.com/search/issues",
                     params={"q": f"repo:{REPO} type:issue state:open"})
open_issues = search["total_count"]

# Contributors (paginate, include anonymous)
contributors = set()
url = f"https://api.github.com/repos/{REPO}/contributors"
params = {"per_page": 100, "anon": "1"}
while url:
    data, resp = get_json(url, params=params)
    for c in data:
        # anonymous contributors have no "login"; fall back to name/id string
        contributors.add(c.get("login") or c.get("name") or str(c))
    # pagination
    url = None
    if "Link" in resp.headers:
        for part in resp.headers["Link"].split(","):
            if 'rel="next"' in part:
                url = part[part.find("<")+1:part.find(">")]
                params = None  # url already has params
                break

print({
    "stars": stars,
    "forks": forks,
    "open_issues": open_issues,
    "contributors": len(contributors)
})
