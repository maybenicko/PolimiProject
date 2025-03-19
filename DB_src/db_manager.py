import sqlite3
import time
from datetime import datetime, timedelta
from post_ranking import ranked
from coin_options_db import MemecoinGenerator


# data = (title, selftext, score, num_comments, upvote_ratio, date_post, sub_name)
class DatabaseManagement:
    def __init__(self, db_name, data=None, post_id=None):
        self.db_name = db_name
        self.directories = ['Crypto', 'Politics', 'Activity']
        self.data = data
        self.post_id = post_id

    def update_db(self):
        directory = 'DB_src/DB/' + self.db_name + '.db'
        with sqlite3.connect(directory) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                        CREATE TABLE IF NOT EXISTS post_data (
                            post_id TEXT PRIMARY KEY,
                            title TEXT,
                            description TEXT,
                            score INTEGER,
                            num_comments INTEGER,
                            upvote_ratio REAL,
                            date TEXT,
                            subreddit_name TEXT
                        )
                    ''')
            conn.commit()

            cursor.execute("SELECT * FROM post_data WHERE post_id = ?", (self.post_id,))
            post_data = cursor.fetchone()

            if not post_data:
                cursor.execute('''
                                    INSERT INTO post_data (
                                        post_id, title, description, score, num_comments, upvote_ratio, date, subreddit_name
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                ''', (self.post_id, *self.data))
                conn.commit()
                print(f'[ ADDED {self.post_id} ]')
                return True

            current_post_id, current_title, current_description, current_score, \
                current_num_comments, current_upvote_ratio, current_date, current_subreddit = post_data

            if (current_score == self.data[2] and
                    current_num_comments == self.data[3] and
                    current_upvote_ratio == self.data[4]):
                print(f'[ SKIPPING {self.post_id} ]')
                return False

            # update post data
            cursor.execute('''
                        UPDATE post_data
                        SET title = ?, description = ?, score = ?, num_comments = ?, 
                            upvote_ratio = ?, date = ?, subreddit_name = ?
                        WHERE post_id = ?
                    ''', self.data + (self.post_id,))
            conn.commit()

            # delete bad posts
            one_week_ago = datetime.now() - timedelta(weeks=1)
            formatted_date = one_week_ago.strftime('%Y-%m-%d')
            query = """
                        DELETE FROM post_data
                        WHERE date < ?
                        OR upvote_ratio < 0.8
                        OR num_comments < 40
                        OR score < 100
                    """
            cursor.execute(query, (formatted_date,))
            conn.commit()
            print(f'[ MODIFIED {self.post_id} ]')
            return True

    def generate_top(self):
        for db_name in self.directories:
            directory = 'DB/' + db_name + '.db'
            with sqlite3.connect(directory) as conn:
                cursor = conn.cursor()

                query = """
                    SELECT * 
                    FROM post_data
                    ORDER BY score DESC
                    LIMIT 100;
                """
                cursor.execute(query)
                data_score = cursor.fetchall()

                query = """
                    SELECT * 
                    FROM post_data
                    ORDER BY num_comments DESC
                    LIMIT 100;
                """
                cursor.execute(query)
                data_comments = cursor.fetchall()

                query = """
                    SELECT * 
                    FROM post_data
                    ORDER BY upvote_ratio DESC
                    LIMIT 100;
                """
                cursor.execute(query)
                data_upvote = cursor.fetchall()
                top = ranked(data_score, data_comments, data_upvote)
                self.check_top(top, db_name)

    def check_top(self, top, db_name):
        post_id_latest = []
        for item_tuple in top:
            post_id_latest.append(item_tuple[0])

        directory = 'DB/TOP/' + db_name + '.db'

        post_id_old = []
        with sqlite3.connect(directory) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT post_id FROM posts")
            post_id_old = [row[0] for row in cursor.fetchall()]
        conn.close()

        # common to keep elements
        keep_post_id = list(set(post_id_old) & set(post_id_latest))

        # posts to keep as they are in db
        add_post_id = []
        for post in post_id_latest:
            if post not in keep_post_id:
                add_post_id.append(post)

        # posts to remove from db
        remove_post_id = []
        for post in post_id_old:
            if post not in keep_post_id:
                remove_post_id.append(post)

        directory = 'DB/TOP/' + db_name + '.db'
        with sqlite3.connect(directory) as conn:
            cursor = conn.cursor()

            for post_id in add_post_id:

                for item in top:
                    if item[0] == post_id:
                        desc = item[1] + '. ' + item[2]
                        metadata = MemecoinGenerator(desc, item[7]).gen_metadata()
                        continue

                cursor.execute('''
                    INSERT INTO posts (post_id, metadata) 
                    VALUES (?, ?);
                ''', (post_id, str(metadata)))
                conn.commit()

            for post_id in remove_post_id:
                cursor.execute('''
                        DELETE FROM posts 
                        WHERE post_id = ?;
                    ''', (post_id,))
                conn.commit()


bot = DatabaseManagement('')
bot.generate_top()
