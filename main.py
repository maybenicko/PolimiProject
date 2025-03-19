from DB_src.reddit_manager import RedditMonitor
from DB_src.db_manager import DatabaseManagement
import time


if __name__ == "__main__":
    reddit_monitor = RedditMonitor()
    while True:
        delay = 600
        reddit_monitor.fetch_posts()
        DatabaseManagement().generate_top()
        print(f'[ SLEEPING FOR {delay}s... ]')
        time.sleep(delay)
