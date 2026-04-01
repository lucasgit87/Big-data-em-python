from mrjob.job import MRJob


class MediaAlunos(MRJob):

    def mapper(self, _, line):

        try:

            if ';' in line:
                campos = line.split(';')
            else:
                campos = line.split(',')

            if len(campos) >= 3:
                aluno = campos[0].strip()
                nota = float(campos[2].strip())

                yield aluno, nota
        except ValueError:

            pass

    def reducer(self, aluno, notas):
        soma_notas = 0
        quantidade = 0


        for nota in notas:
            soma_notas += nota
            quantidade += 1


        if quantidade > 0:
            media = soma_notas / quantidade
            yield aluno, media


if __name__ == '__main__':
    MediaAlunos.run()
