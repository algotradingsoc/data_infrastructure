import pandas as pd


def ListingUpdate(day1, day2, method="symbol"):
    # allternative method: by 'symbol'

    id1 = pd.read_csv(day1 + ".csv")[method]
    id2 = pd.read_csv(day2 + ".csv")[method]

    delisting_id = id1[id1.isin(id2) == False]
    listing_id = id2[id2.isin(id1) == False]

    return delisting_id, listing_id


if __name__ == "__main__":
    print(ListingUpdate("19920615", "19920619"))
