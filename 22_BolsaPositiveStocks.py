from mrjob.job import MRJob

class BolsaPositiveStocks(MRJob):

    def mapper(self, _, line):
        for w in line.split():
            company, stock, date = line.split(',')
            yield company,(float(stock), date)

    def reducer(self, key, values):
        diferencias = []
        stocks = []
        possitive_stocks = []
        always_possitive = True
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
            diferencia = stock - (sorted_stocks[i-1])[0]
            print(key, stock, (sorted_stocks[i-1])[0], "=", diferencia)
            # Verifica si la diferencia es negativa
            if diferencia < 0:
                # Marca la accion como no apta
                always_possitive = False
                break
            i =+ 1
        # Verifica si la accion siempre se mantuvo estable o creciendo
        if always_possitive:
            yield "Company " + key, 0

if __name__ == '__main__':
    BolsaPositiveStocks.run()