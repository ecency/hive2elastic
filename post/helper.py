import math


def reputation_to_score(rep):
    """
    Converts the raw reputation value into the reputation score.
    If the input is already a 2-digit score, it returns it directly.
    """
    if isinstance(rep, str):
        rep = int(rep)

    # If rep is already in 2-digit format, return it as a float
    if 10 <= rep < 100:
        return float(rep)

    if rep == 0:
        return 25.0

    score = max([math.log10(abs(rep)) - 9, 0])
    if rep < 0:
        score *= -1
    score = (score * 9.0) + 25.0
    return score


def calc_flag_weight(rshares, abs_rshares):
    neg_rshares = (rshares - abs_rshares) // 2  # effectively sum of all negative rshares
    # take negative rshares, divide by 2, truncate 10 digits (plus neg sign),
    #   and count digits. creates a cheap log10, stake-based flag weight.
    #   result: 1 = approx $400 of downvoting stake; 2 = $4,000; etc

    return max((len(str(int(neg_rshares / 2))) - 11, 0))
