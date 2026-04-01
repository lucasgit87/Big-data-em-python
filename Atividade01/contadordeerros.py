from mrjob.job import MRJob

class ContadorErros(MRJob):


    def mapper(self, _, line):

        if "[error]" in line.lower():

            yield "erros_encontrados", 1


    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':

    log_data = """2026-03-31 10:00:01[info] sistema iniciado
2026-03-31 10:05:12[error] falha na conexao com banco
2026-03-31 10:10:45[info] usuario logado
2026-03-31 10:15:20[error] timeout na requisição
2026-03-31 10:20:00[warn] memoria alta
2026-03-31 10:25:00[error] erro fatal no core"""

    with open("servidor.log", "w", encoding="utf-8") as f:
        f.write(log_data)


    ContadorErros.run()