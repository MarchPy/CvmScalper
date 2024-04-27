from src.main import Cvm

app = Cvm()
app.colect_data()
app.extract_files()
app.remove_files()
app.concat_files()