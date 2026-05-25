from api.repositories import views_repository


def filmes_ranking():
    return views_repository.filmes_ranking()


def faturamento_mensal():
    return views_repository.faturamento_mensal()
