# Execution de entrada nao processavel. fora do autoretry_for
# so a CLI converte em SystemExit: dentro da task derrubaria o processo filho
class PreconditionError(Exception):
    pass


# outra task ja processa o mesmo pai (mensagem duplicada)
class ConcurrentExecutionError(Exception):
    pass
