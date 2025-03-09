import sqlite3
import re
import nltk
from nltk.corpus import stopwords


class SQLManager:
    def __init__(self, db_id, post_id, subdirectory, data=None):
        self.subdirectory = subdirectory
        self.db_id = db_id
        self.post_id = post_id
        self.data = data
        self.db_connection = sqlite3.connect(f'DB/{self.subdirectory}{self.db_id}.db')
        self.db_cursor = self.db_connection.cursor()
        self.initialize_filter_db()

    @staticmethod
    def filter_stopwords(words_list_base):
        stop_words = set(stopwords.words('english'))
        words_list = []

        for word in words_list_base:
            if len(word) < 3:
                continue
            if word not in stop_words:
                words_list.append(word)
        return words_list

    def initialize_filter_db(self):
        self.db_cursor.execute('''
            CREATE TABLE IF NOT EXISTS posts (
                post_id TEXT PRIMARY KEY
            )
        ''')
        self.db_connection.commit()

    def check_post_processed(self):
        self.db_cursor.execute('SELECT 1 FROM posts WHERE post_id = ?', (self.post_id,))
        result = self.db_cursor.fetchone()
        return result is not None

    def store_filter(self):
        self.db_cursor = self.db_connection.cursor()
        self.initialize_filter_db()
        self.db_cursor.execute('INSERT OR REPLACE INTO posts (post_id) VALUES (?)', (self.post_id,))
        self.db_connection.commit()

    def update_db(self):
        if self.subdirectory is None:
            print('Error subdirectory')
            return
        if self.subdirectory == 'POST/':
            self.db_cursor.execute('''
                            CREATE TABLE IF NOT EXISTS post_data (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                title TEXT,
                                score INTEGER,
                                num_comments INTEGER,
                                upvote_ratio REAL,
                                date INTEGER,
                                subreddit_name TEXT
                            )
                        ''')
            self.db_connection.commit()
            self.db_cursor.execute('''
                            INSERT INTO post_data (title, score, num_comments, upvote_ratio, date, subreddit_name)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (self.data["title"], self.data["score"], self.data["num_comments"],
                              self.data["upvote_ratio"], self.data["date"], self.data["subreddit_name"]))
            self.db_connection.commit()

        elif self.subdirectory == 'WORDS/':
            words_list_base = re.findall(r'\b\w+\b', self.data["title"].lower())

            words_list = self.filter_stopwords(words_list_base)

            words_set = list(set(words_list))
            words_list_add_count = [word for word in words_set if word not in words_list]

            self.db_cursor.execute('''
                            CREATE TABLE IF NOT EXISTS word_data (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                word TEXT UNIQUE,
                                count INTEGER DEFAULT 0,
                                score INTEGER DEFAULT 0,
                                num_comments INTEGER DEFAULT 0,
                                upvote_ratio INTEGER DEFAULT 0,
                                post_count INTEGER DEFAULT 0
                            )
                        ''')
            self.db_connection.commit()

            # get the list of the set to change all data once
            for word in words_set:
                self.db_cursor.execute('SELECT count, score, num_comments, upvote_ratio, post_count FROM word_data WHERE word = ?', (word,))
                result = self.db_cursor.fetchone()

                if result:
                    count, score, num_comments, upvote_ratio, post_count = result
                    self.db_cursor.execute('UPDATE word_data SET count = count + 1 WHERE word = ?', (word,))
                    self.db_cursor.execute('UPDATE word_data SET post_count = post_count + 1 WHERE word = ?', (word,))

                    new_score = score + self.data['score']
                    new_num_comments = num_comments + self.data['num_comments']

                    weight_old = post_count / (post_count + 1)
                    weight_new = 1 - weight_old
                    new_upvote_ratio = (upvote_ratio * weight_old) + (self.data["upvote_ratio"] * weight_new)

                    self.db_cursor.execute('''
                                        UPDATE word_data SET score = ?, num_comments = ?, upvote_ratio = ?
                                        WHERE word = ?
                                    ''', (
                        new_score, new_num_comments, new_upvote_ratio, word))
                    self.db_connection.commit()
                else:
                    # Insert new word with default values
                    self.db_cursor.execute('''
                                INSERT INTO word_data (word, count, score, num_comments, upvote_ratio, post_count)
                                VALUES (?, 1, ?, ?, ?, 1)
                            ''', (word, self.data['score'], self.data['num_comments'], self.data['upvote_ratio']))
                    self.db_connection.commit()

            for word in words_list_add_count:
                self.db_cursor.execute('SELECT count FROM word_data WHERE word = ?', (word,))
            self.db_connection.commit()

