import downloader
import matplotlib.pyplot as plt
import parquet_manager as pm
import polars

PASTA_PARQUETS = "C:/Users/Administrador/Documents/Developer/Python/Projeto Eventos Climaticos/parquets"

# Checar se existem novos dados desde a última atualização, e baixá-los se for o caso
downloader.extrair_dados()

# Gerar DF unificado
df = pm.gerar_df_unificado()

print(df.describe())



# 5. Feature Engineering

# 6. Modeling (if prediction is your next step)

# 7. Versioning & Pipeline Prep


