class PreconditionError(Exception):
    """Execution de entrada não está num estado processável.

    Levantada pelos run(). Não está em autoretry_for da task, então falha
    sem retry. Só os main() (CLI) convertem em SystemExit: levantar
    SystemExit dentro de uma task Celery derruba o processo filho do pool.
    """
