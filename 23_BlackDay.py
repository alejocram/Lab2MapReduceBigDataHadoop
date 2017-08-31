from mrjob.job import MRJob
# Se importa para poder definir 2 reduces
from mrjob.job import MRStep
import statistics
from collections import Counter

class BlackDay(MRJob):

    # Metodo donde se definen los pasos
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_negative_stock_dates),
            MRStep(reducer=self.reducer_common_day)
        ]

    def mapper(self, _, line):
        company, stock, date = line.split(',')
        yield company, (float(stock), date)


    def reducer_negative_stock_dates(self, key, values):
        negative_stock_dates = []
        stocks = []
        for stock, date in values:
            stocks.append((stock, date))
        # Se organiza por fecha
        sorted_stocks = sorted(stocks, key=lambda x: x[1])

        i = 0
        for stock, date in sorted_stocks:
            # Verifica si es el primer valor
            if i == 0:
                i =+ 1
                # Continua con la siguiente acción
                continue
            difference = stock - (sorted_stocks[i-1])[0]
            # print(key, stock, (sorted_stocks[i-1])[0], "=", difference)
            # Verifica si la diferencia es negativa
            if difference < 0:
                negative_stock_dates.append(date)
                break
            i =+ 1
        # Verifica si la acción tuvo alguna caida algún día
        if len(negative_stock_dates) > 0:
            yield key, negative_stock_dates

    def reducer_common_day(self, key, values):
        dates = []
        for date in values:
            dates.append(date)

        negative_stock_dates = Counter()
        for negative_stock_date in dates:
            negative_stock_dates[negative_stock_date[0]] += 1

        # Se presenta la fecha que mas veces aparecio
        yield "Black day ", negative_stock_dates.most_common()

if __name__ == '__main__':
    BlackDay.run()