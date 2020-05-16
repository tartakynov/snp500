#!/usr/bin/env python

# updates market cap values

import csv
from pandas_datareader import data


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def read_companies(file_name):
    with open(file_name) as csv_file:
        return list(csv.DictReader(csv_file))


def write_companies(file_name, rows, append=False):
    with open(file_name, 'a' if append else 'w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=rows[0].keys())
        if not append:
            writer.writeheader()
        writer.writerows(rows)


def main():
    companies = read_companies('companies.csv')
    append = False

    for chunk in chunks(companies, 100):
        tickers = [row["Symbol"] for row in chunk]
        market_cap_data = data.get_quote_yahoo(tickers)['marketCap']
        for market_cap, company in zip(market_cap_data, chunk):
            company["Market cap"] = market_cap

        write_companies('companies.csv', chunk, append)
        append = True


if __name__ == "__main__":
    main()
