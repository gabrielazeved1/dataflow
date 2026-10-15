import os

# define antes de qualquer import da api — Settings() usa localhost em vez de "postgres"
# "postgres" é o hostname dentro do Docker; fora do Docker a porta é mapeada para localhost
os.environ["POSTGRES_DSN"] = (
    "postgresql+psycopg2://dataflow:changeme@localhost:5432/dataflow"
)
