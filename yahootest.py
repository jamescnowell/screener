import StringIO
import csv
import json
import requests
import urllib2
import sys
import datetime
import time


def get_symbols():
    '''
    This function grabs symbols from the NASDAQ ftp server. Symbols don't necessarily match Yahoo
    :return:
    '''
    nas_listed = 'ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt'
    other_listed = 'ftp://ftp.nasdaqtrader.com/symboldirectory/otherlisted.txt'

    sources = [nas_listed, other_listed]
    syms = []
    # TODO: Handle special symbol types, e.g. BRK-A
    for source in sources:
        sym_req = urllib2.Request(source)
        sym_resp = urllib2.urlopen(sym_req)
        page = sym_resp.read()
        sym_reader = csv.reader(StringIO.StringIO(page), delimiter='|')
        for i, row in enumerate(sym_reader):
            if i == 0:
                continue
            syms.append(row[0])
        syms.pop(-1)
    return syms


def new_get_symbols():
    # CSV File built from data found here: http://investexcel.net/all-yahoo-finance-stock-tickers/
    syms = []
    with open('symbols.csv') as f:
        sym_reader = csv.DictReader(f.readlines())
        for row in sym_reader:
            syms.append(row)

    return syms


def main():
    # map of column names to Yahoo CSV API codes
    # see here: http://www.jarloo.com/yahoo_finance/
    cols = [
        ('sym','s'),
        ('book','b4'),
        ('divshare','d'),
        ('divyield','y'),
        ('eps','e'),
        ('price','l1'),  # using last trade
        ('p/e','r'),
        ('peg','r5'),
        ('rev','s6')
    ]

    # Grab all symbol information from the CSV
    query_syms = new_get_symbols()
    chunk_size = 100
    syms = []
    # File to store the filtered symbol data
    good_out = open('goodout.json', 'w')
    # File to store all of the symbol data
    with open('out.json', 'wb') as out:
        # If anything breaks, close the filtered file and re-raise the exception
        try:
            # Grab the data in batches of chunk_size (limited by the url length/csv api, not sure upper limit)
            for i in range(0, len(query_syms), chunk_size):
                batch_syms = query_syms[i:i + chunk_size]
                # This is an inefficient way of being able to match the return data with the symbol information
                sym_dict = dict(map(lambda x: (x['Ticker'], x), batch_syms))

                # grab the CSV
                query = 'http://download.finance.yahoo.com/d/quotes.csv?s=%s&f=%s' % (','.join(map(lambda x: x['Ticker'], batch_syms)), ''.join(map(lambda x: x[1], cols)))
                r = requests.get(query)
                # Make sure to print failure information for debugging
                if r.status_code > 299:
                    print r.status_code
                    print r.reason
                    sys.exit(1)
                # Clean up the data and open it as a CSV
                clean = r.text
                reader = csv.reader(StringIO.StringIO(clean), delimiter=",", quotechar='"')
                for row in reader:
                    # Turn the data into a dict with the column information (api returns data in the order requested
                    sym = dict(zip(map(lambda x: x[0], cols), row))
                    # Join the CSV data with the known symbol data
                    sym.update(sym_dict.get(sym['sym'], {}))
                    # Add to the list of all symbols, for downstream processing?
                    syms.append(sym)
                    # Turn the data into a string
                    j = json.dumps(sym, encoding="ISO-8859-1")
                    # Write out to the master file
                    out.write(j+'\n')
                    # Only screen symbols where all data is available for now
                    if all(map(lambda v: v != 'N/A', sym.itervalues())):
                        # Convert everything to a float
                        price = float(sym['price'])
                        book = float(sym['book'])
                        eps = float(sym['eps'])
                        divshare = float(sym['divshare'])
                        divyield = float(sym['divyield'])
                        peg = float(sym['peg'])

                        # TODO: Find a way to make this more configurable
                        # If the symbol matches the screen, write it to the good file, and print it
                        if price < book*1.5 and eps > divshare*2 and divyield > 3 and 0 < peg < 1.1:
                            good_out.write(j+'\n')
                            print j
                # Just to be nice to the Yahoo API, max 2 requests per second
                time.sleep(.5)
        except:
            good_out.close()
            raise
    good_out.close()
    return syms


if __name__ == '__main__':
    start = datetime.datetime.now()
    print start
    main()
    end = datetime.datetime.now()
    print end
    print end-start
