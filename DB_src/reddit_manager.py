from dateutil.relativedelta import relativedelta
import praw
from datetime import datetime
import pytz
import json
from DB_src.db_manager import DatabaseManagement


class RedditMonitor:
    def __init__(self):
        self.credentials = self.get_creds()
        self.reddit = praw.Reddit(
            client_id=self.credentials[0],
            client_secret=self.credentials[1],
            user_agent="web app"
        )
        self.subreddits = self.load_subreddits()
        self.rome_tz = pytz.timezone("Europe/Rome")

    @staticmethod
    def get_time():
        return datetime.now().strftime("[ %H:%M:%S ]")

    def get_creds(self):
        try:
            with open("DB_src/creds/creds.json", "r", encoding="utf-8") as file:
                j = json.load(file)
                return [j["client_id"], j["client_secret"]]
        except (FileNotFoundError, json.JSONDecodeError):
            print(f"{self.get_time} [ ERROR IN creds.json - WRONG JSON FORMAT ]")

            return {}

    def load_subreddits(self):
        try:
            with open("DB_src/subreddits.json", "r", encoding="utf-8") as file:
                content = json.load(file)
                return content
        except (FileNotFoundError, json.JSONDecodeError):
            print(f"{self.get_time} [ ERROR IN subreddits.json - WRONG JSON FORMAT ]")
            return {}

    def fetch_posts(self):
        for db_name, sub_name_list in self.subreddits.items():
            for sub_name in sub_name_list:
                subreddit = self.reddit.subreddit(sub_name)
                print(f"{self.get_time()} [ GETTING DATA FROM r/{sub_name} ]")
                after = None
                fetch = True

                while fetch:
                    new_posts = list(subreddit.new(limit=100, params={"after": after}))
                    if not new_posts:
                        print(f"{self.get_time()} [ No more posts available. Stopping. ]")
                        break

                    for submission in new_posts:
                        post_timestamp = datetime.fromtimestamp(submission.created_utc)
                        date_post = post_timestamp.strftime("%Y-%m-%d")
                        timestamp_week = datetime.now() - relativedelta(weeks=1)

                        if post_timestamp < timestamp_week:
                            print(f'{self.get_time()} [ POSTS TOO OLD - STOPPING FETCHING ]')
                            fetch = False
                            break

                        post_id = submission.id
                        data = (submission.title, submission.selftext, submission.score,
                                submission.num_comments, submission.upvote_ratio, date_post, sub_name)
                        if submission.num_comments < 40 or submission.upvote_ratio < 0.8 or submission.score < 100:
                            continue

                        DatabaseManagement(db_name, data, post_id).update_db()
                    after = new_posts[-1].fullname

