import StringIO
import csv
import json
import requests
import urllib2
import sys
import datetime
import time


def get_symbols():
    nas_listed = 'ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt'
    other_listed = 'ftp://ftp.nasdaqtrader.com/symboldirectory/otherlisted.txt'

    sources = [nas_listed, other_listed]
    syms = []
    # TODO: Handle special symbol types
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
    syms = []
    with open('symbols.csv') as f:
        sym_reader = csv.DictReader(f.readlines())
        for row in sym_reader:
            syms.append(row)

    return syms


def main():
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

    query_syms = new_get_symbols()
    chunk_size = 100
    syms = []
    good_out = open('goodout.json', 'w')
    with open('out.json', 'wb') as out:
        try:
            for i in range(0, len(query_syms), chunk_size):
                batch_syms = query_syms[i:i + chunk_size]
                sym_dict = dict(map(lambda x: (x['Ticker'], x), batch_syms))

                query = 'http://download.finance.yahoo.com/d/quotes.csv?s=%s&f=%s' % (','.join(map(lambda x: x['Ticker'], batch_syms)), ''.join(map(lambda x: x[1], cols)))
                r = requests.get(query)
                if r.status_code > 299:
                    print r.status_code
                    print r.reason
                    sys.exit(1)
                clean = r.text
                reader = csv.reader(StringIO.StringIO(clean), delimiter=",", quotechar='"')
                for row in reader:
                    sym = dict(zip(map(lambda x: x[0], cols), row))
                    sym.update(sym_dict.get(sym['sym'], {}))
                    syms.append(sym)
                    j = json.dumps(sym, encoding="ISO-8859-1")
                    out.write(j+'\n')
                    if all(map(lambda v: v != 'N/A', sym.itervalues())):
                        price = float(sym['price'])
                        book = float(sym['book'])
                        eps = float(sym['eps'])
                        divshare = float(sym['divshare'])
                        divyield = float(sym['divyield'])
                        peg = float(sym['peg'])

                        if price < book*1.5 and eps > divshare*2 and divyield > 3 and 0 < peg < 1.1:
                            good_out.write(j+'\n')
                            print j

                time.sleep(.5)
        except:
            good_out.close()
            raise
    good_out.close()


if __name__ == '__main__':
    start = datetime.datetime.now()
    print start
    main()
    end = datetime.datetime.now()
    print end
    print end-start
