from mrjob.job import MRJob

class AverageDIAN(MRJob):

    def mapper(self, _, line):
        for w in line.split():
            sector, _, salary, _ = line.split(',')
            print(sector, "$ ", salary)
            yield sector,int(salary)

    def reducer(self, key, values):
        salaries = []
        for salary in values:
            salaries.append(salary)
        yield key, sum(salaries)/len(salaries)

if __name__ == '__main__':
    AverageDIAN.run()