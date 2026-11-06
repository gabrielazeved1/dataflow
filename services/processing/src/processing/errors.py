class PreconditionError(Exception):
    """Execution de entrada não está num estado processável.

    Levantada pelos run(). Não está em autoretry_for da task, então falha
    sem retry. Só os main() (CLI) convertem em SystemExit: levantar
    SystemExit dentro de uma task Celery derruba o processo filho do pool.
    """


class ConcurrentExecutionError(Exception):
    """Outra task já está processando a mesma execução pai (filha RUNNING).

    Acontece quando a mesma Bronze é enfileirada duas vezes (ex.: pelo
    reconciler enquanto a original ainda estava na fila). O índice único
    parcial impede a segunda filha; a task duplicada só desiste.
    """
