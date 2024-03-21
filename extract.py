import csv
import psycopg2

#first part: extraction
print("Initializing extraction fase")

def insert_data_from_csvs(csv_files, table_names):

    conn = psycopg2.connect(database = "origem_pizzaria", 
                        user = "postgres", 
                        host= 'localhost',
                        password = "postgres",
                        port = 5432)
    cursor = conn.cursor()

    for csv_file, table_name in zip(csv_files, table_names):
        with open(csv_file, 'r', encoding='iso-8859-1') as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                placeholders = ', '.join(['%s'] * len(row))
                cursor.execute(f"INSERT INTO {table_name} values ({placeholders})", row)

    conn.commit()
    conn.close()

csv_files = ['data_source/order_details.csv', 'data_source/orders.csv', 'data_source/pizza_types.csv', 'data_source/pizzas.csv']  # List of CSV file paths
table_names = ['orders_details', 'orders', 'pizza_types', 'pizzas']          # List of corresponding table names
insert_data_from_csvs(csv_files, table_names)