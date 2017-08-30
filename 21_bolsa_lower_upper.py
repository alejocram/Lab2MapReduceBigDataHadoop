from mrjob.job import MRJob

class BolsaLowerUpper(MRJob):

    def mapper(self, _, line):
        for w in line.split():
            company, stock, date = line.split(',')
            yield company,float(stock)

    def reducer(self, key, values):
        stocks = []
        for stock in values:
            stocks.append(stock)
        minimum = min(stocks)
        maximum = max(stocks)
        yield "Company " + key + " (min, max)", (minimum, maximum)

if __name__ == '__main__':
    BolsaLowerUpper.run()