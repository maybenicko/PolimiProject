from DB_src.reddit_manager import RedditMonitor
import time


if __name__ == "__main__":
    reddit_monitor = RedditMonitor()
    while True:
        delay = 1800
        reddit_monitor.fetch_posts()
        print(f'[ SLEEPING FOR {delay}s... ]')
        time.sleep(delay)
