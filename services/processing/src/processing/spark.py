from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # barra de progresso no console embaralha as linhas de log
        .config("spark.ui.showConsoleProgress", "false")
        # ninguem abre a ui do spark: so gastava threads (~240 no worker)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
