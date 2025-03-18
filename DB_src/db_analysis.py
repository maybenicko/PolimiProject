import sqlite3
import time


class HadilHandler:
    def __init__(self, directory, top_activity, top_crypto, top_politics):
        self.directory = directory
        self.db_connection = sqlite3.connect(f'DB/POST/{directory}.db')
        self.db_cursor = self.db_connection.cursor()
        self.top_activity = top_activity
        self.top_crypto = top_crypto
        self.top_politics = top_politics

    def idk(self):
        try:
            query = """
                SELECT * 
                FROM post_data
                WHERE score > (SELECT AVG(score) FROM post_data)
                AND num_comments > (SELECT AVG(num_comments) FROM post_data)
                ORDER BY score DESC;
            """
            self.db_cursor.execute(query)
            data_score = self.db_cursor.fetchall()

            query = """
                SELECT * 
                FROM post_data
                WHERE score > (SELECT AVG(score) FROM post_data)
                AND num_comments > (SELECT AVG(num_comments) FROM post_data)
                ORDER BY num_comments DESC;
            """
            self.db_cursor.execute(query)
            data_comments = self.db_cursor.fetchall()

            query = """
                SELECT * 
                FROM post_data
                WHERE score > (SELECT AVG(score) FROM post_data)
                AND num_comments > (SELECT AVG(num_comments) FROM post_data)
                ORDER BY upvote_ratio DESC;
            """
            self.db_cursor.execute(query)
            data_upvote = self.db_cursor.fetchall()

            result = self.ranked(data_score, data_comments, data_upvote)

            if self.directory == 'Activity':
                return [result, [], []]
            elif self.directory == 'Crypto':
                return [[], result, []]
            elif self.directory == 'Politics':
                return [[], [], result]
        except Exception as e:
            print(f'Error: {e}')

    def ranked(self, score, comments, upvote):
        ranks = {}

        for rank, elem in enumerate(score):
            ranks[elem] = [rank + 1, None, None]

        for rank, elem in enumerate(comments):
            if elem in ranks:
                ranks[elem][1] = rank + 1
            else:
                ranks[elem] = [None, rank + 1, None]

        for rank, elem in enumerate(upvote):
            if elem in ranks:
                ranks[elem][2] = rank + 1
            else:
                ranks[elem] = [None, None, rank + 1]

        combined_ranks = []
        for elem, (rank1, rank2, rank3) in ranks.items():
            total_rank = (rank1 if rank1 is not None else float('inf')) + \
                         (rank2 if rank2 is not None else float('inf')) + \
                         (rank3 if rank3 is not None else float('inf'))
            combined_ranks.append((elem, total_rank))

        combined_ranks.sort(key=lambda x: x[1])
        return [elem for elem, _ in combined_ranks]

    def top(self):
        if len(self.top_activity) == 0 or len(self.top_crypto) == 0 or len(self.top_politics) == 0:
            return
        tops = [self.top_activity, self.top_crypto, self.top_politics]
        for array in tops:
            _ = 0
            for item in array:
                if _ < 10:
                    print(item)
                    _ += 1
            print('\n\n')


top_activity = []
top_crypto = []
top_politics = []

directories = ['Activity', 'Crypto', 'Politics']
for directory in directories:
    bot = HadilHandler(directory, top_activity, top_crypto, top_politics)
    result = bot.idk()
    top_activity += result[0]
    top_crypto += result[1]
    top_politics += result[2]
    bot.top()
