from mrjob.job import MRJob


class MaxTempEstacao(MRJob):
    def mapper(self, _, line):
        try:

            if ';' in line:
                campos = line.split(';')
            else:
                campos = line.split(',')

            if len(campos) >= 2:
                estacao = campos[0].strip()
                temp = float(campos[1].strip())
                yield estacao, temp
        except ValueError:
            pass

    def reducer(self, estacao, temps):

        yield estacao, max(temps)


if __name__ == '__main__':
    MaxTempEstacao.run()

