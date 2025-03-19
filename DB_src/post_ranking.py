def ranked(score, comments, upvote):
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
    top = [elem for elem, _ in combined_ranks]
    return top[:10]
