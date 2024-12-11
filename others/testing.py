# import csv
#
# def generate_schema(csv_file):
#     with open(csv_file, 'r') as file:
#         reader = csv.reader(file)
#         headers = next(reader)
#         schema = {}
#         for header in headers:
#             schema[header] = 'TEXT'  # Defaulting to TEXT, you can add logic to infer types
#     return schema
#
# def save_schema_to_txt(schema, txt_file):
#     with open(txt_file, 'w') as file:
#         for column, data_type in schema.items():
#             file.write(f'{column} {data_type}\n')
#
# def generate_sql_query(schema, table_name):
#     columns = ', '.join([f'{column} {data_type}' for column, data_type in schema.items()])
#     sql_query = f'CREATE TABLE {table_name} ({columns});'
#     return sql_query
#
# def main():
#     csv_file = 'MOCK_DATA.csv'  # Replace with your CSV file path
#     txt_file = 'schema.txt'
#     table_name = 'tableName'
#
#     schema = generate_schema(csv_file)
#     save_schema_to_txt(schema, txt_file)
#     sql_query = generate_sql_query(schema, table_name)
#
#     print(f'Schema saved to {txt_file}')
#     print(f'SQL Query: {sql_query}')
#
# if __name__ == '__main__':
#     main()



word="programming"
vowels = "aeiou"
count  = 0
for i in list(word(0,len(word)-1)):
    print(i)
