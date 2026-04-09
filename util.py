from datetime import date

# Função aceita data (dia do ano) e retorna string (estação correspondente)
def calcular_estacao_periodo(data: date) -> str:
    mes = data.month
    ano = data.year

    match mes:
        case 1 | 2:
            return f"1-Verao-{ano}"
        case 3 | 4 | 5:
            return f"2-Outono-{ano}"
        case 6 | 7 | 8:
            return f"3-Inverno-{ano}"
        case 9 | 10 | 11:
            return f"4-Primavera-{ano}"
        case 12:
            return f"1-Verao-{ano + 1}"
        case _:
            return "null"
        