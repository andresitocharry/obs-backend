import pandas as pd
import sys

def analyze_excel(file_path):
    try:
        # Usamos openpyxl para .xlsm
        xl = pd.ExcelFile(file_path, engine='openpyxl')
        print(f"Hojas encontradas: {xl.sheet_names}")
        
        for sheet in xl.sheet_names:
            df = xl.parse(sheet, nrows=5)
            print(f"\n--- Hoja: {sheet} ---")
            print(f"Columnas ({len(df.columns)}): {df.columns.tolist()}")
            print("Primeras 2 filas:")
            print(df.head(2))
            
    except Exception as e:
        print(f"Error analizando el archivo: {e}")

if __name__ == "__main__":
    path = r"C:\Users\PIPEC\Desktop\tesis\dqg-frontend\Herramienta_seguimiento_monitoreo_pacientes_canguro.xlsm"
    analyze_excel(path)
