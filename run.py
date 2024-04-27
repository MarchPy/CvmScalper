from src.main import Cvm

app = Cvm()
app.colect_data()
app.extract_files()
df_cvm = app.concat_files()
app.remove_files()
app.database(df=df_cvm)
app.create_new_file()