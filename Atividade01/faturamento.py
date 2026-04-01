from mrjob.job import MRJob


class FaturamentoPorCategoria(MRJob):

    def mapper(self, _, linha):
       
        colunas = linha.split(';')


        if len(colunas) < 3:
            colunas = linha.split(',')

        if len(colunas) >= 3:
            try:

                categoria = colunas[1].strip().replace('"', '')
                valor = float(colunas[2].strip().replace('"', '').replace(',', '.'))

                yield (categoria, valor)
            except (ValueError, IndexError):
                pass

    def reducer(self, categoria, valores):
        yield (categoria, sum(valores))


if __name__ == '__main__':
    FaturamentoPorCategoria.run()
