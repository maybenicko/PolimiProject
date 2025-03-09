import praw
from datetime import datetime
import pytz
import json
from db_manager import SQLManager


class RedditMonitor:
    def __init__(self):
        self.credentials = self.get_creds()
        self.subdirectories = ["POST/", "WORDS/"]
        self.reddit = praw.Reddit(
            client_id=self.credentials[0],
            client_secret=self.credentials[1],
            user_agent="web app"
        )
        self.subreddits = self.load_subreddits()
        self.rome_tz = pytz.timezone("Europe/Rome")

    @staticmethod
    def get_creds():
        try:
            with open("creds/creds.json", "r", encoding="utf-8") as file:
                j = json.load(file)
                return [j["client_id"], j["client_secret"]]
        except (FileNotFoundError, json.JSONDecodeError):
            print("Error: creds.json not found or invalid JSON format.")
            return {}

    @staticmethod
    def load_subreddits():
        try:
            with open("subreddits.json", "r", encoding="utf-8") as file:
                content = json.load(file)
                return content
        except (FileNotFoundError, json.JSONDecodeError):
            print("Error: subreddits.json not found or invalid JSON format.")
            return {}

    def fetch_posts(self, limit=6):
        for db_id, sub_name_list in self.subreddits.items():
            if db_id != 'Politics':
                continue
            for sub_name in sub_name_list:
                subreddit = self.reddit.subreddit(sub_name)
                print(f"[ Fetching latest {limit} posts from r/{sub_name}... ]")

                for submission in subreddit.new(limit=limit):
                    post_id = submission.id

                    if not SQLManager(f'FILTER/{db_id}', post_id, '').check_post_processed():
                        SQLManager(f'FILTER/{db_id}', post_id, '').store_filter()

                        data = {
                            "title": submission.title,
                            "score": submission.score,
                            "num_comments": submission.num_comments,
                            "upvote_ratio": submission.upvote_ratio,
                            "date": submission.created_utc,
                            "subreddit_name": sub_name
                        }
                        print(data)
                        for subdirectory in self.subdirectories:
                            SQLManager(db_id, post_id, subdirectory, data).update_db()
                    else:
                        print(f"Post {post_id} has already been processed. Skipping...")
                break


if __name__ == "__main__":
    reddit_monitor = RedditMonitor()
    reddit_monitor.fetch_posts()

