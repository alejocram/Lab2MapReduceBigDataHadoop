from mrjob.job import MRJob

class AverageDIAN(MRJob):

    def mapper(self, _, line):
        for w in line.split():
            sector, employee, _, _ = line.split(',')
            yield employee,sector

    def reducer(self, key, values):
        sectors = []
        for sector in values:
            sectors.append(sector)
        yield "Employee " + key + " in count sector ", + len(sectors)

if __name__ == '__main__':
    AverageDIAN.run()