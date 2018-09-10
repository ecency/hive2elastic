def chunks(l, n):
    """Yield successive n-sized chunks from l.


    >>> list(chunks(['a', 'b', 'c', 'd'], 2))
    [['a', 'b'], ['c', 'd']]

    >>> list(chunks(list(range(11)), 3))
    [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]]

    >>> list(chunks(list(range(10)), 30))
    [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]]


    """

    for i in range(0, len(l), n):
        yield l[i:i + n]
