import time

from dateutil.relativedelta import relativedelta
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
    def get_time():
        return datetime.now().strftime("[ %Y-%m-%d ] [ %H:%M:%S ]")

    def get_creds(self):
        try:
            with open("creds/creds.json", "r", encoding="utf-8") as file:
                j = json.load(file)
                return [j["client_id"], j["client_secret"]]
        except (FileNotFoundError, json.JSONDecodeError):
            print(f"{self.get_time} [ ERROR IN creds.json - WRONG JSON FORMAT ]")

            return {}

    def load_subreddits(self):
        try:
            with open("subreddits.json", "r", encoding="utf-8") as file:
                content = json.load(file)
                return content
        except (FileNotFoundError, json.JSONDecodeError):
            print(f"{self.get_time} [ ERROR IN subreddits.json - WRONG JSON FORMAT ]")
            return {}

    def fetch_posts(self):
        for db_id, sub_name_list in self.subreddits.items():
            for sub_name in sub_name_list:
                subreddit = self.reddit.subreddit(sub_name)
                print(f"{self.get_time()} [ GETTING DATA FROM r/{sub_name} ]")

                after = None
                fetch = True

                while fetch:
                    new_posts = list(subreddit.new(limit=100, params={"after": after}))
                    if not new_posts:
                        print(f"{self.get_time()} [ No more posts available. Stopping. ]")
                        fetch = False
                        break

                    for submission in new_posts:
                        post_timestamp = datetime.fromtimestamp(submission.created_utc)
                        date_post = post_timestamp.strftime("%Y-%m-%d")
                        timestamp_month = datetime.now() - relativedelta(months=1)

                        if post_timestamp < timestamp_month:
                            print(f'{self.get_time()} [ POSTS TOO OLD - STOPPING FETCHING ] [ {date_post} ]')
                            fetch = False
                            break

                        post_id = submission.id

                        if not SQLManager(f'FILTER/{db_id}', post_id, '').check_post_processed():
                            SQLManager(f'FILTER/{db_id}', post_id, '').store_filter()

                            data = {
                                "title": submission.title,
                                "score": submission.score,
                                "num_comments": submission.num_comments,
                                "upvote_ratio": submission.upvote_ratio,
                                "timestamp": submission.created_utc,
                                "date": date_post,
                                "subreddit_name": sub_name
                            }

                            for subdirectory in self.subdirectories:
                                print(f'{self.get_time()} [ ADDING {post_id} TO THE DATABASE ] [ {date_post} ]')
                                SQLManager(db_id, post_id, subdirectory, data).update_db()

                        else:
                            print(f"[ POST {post_id} ALREADY IN THE DATABASE ]")
                            fetch = False
                            break
                    after = new_posts[-1].fullname


if __name__ == "__main__":
    reddit_monitor = RedditMonitor()
    while True:
        reddit_monitor.fetch_posts()
        time.sleep(1800)

