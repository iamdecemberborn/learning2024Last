# import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq
# from cryptography.fernet import Fernet
# import os
#
# # Function to generate a key and save it to a file
# def generate_key(key_file):
#     key = Fernet.generate_key()
#     with open(key_file, 'wb') as key_out:
#         key_out.write(key)
#     return key
#
# # Function to load the encryption key from a file
# def load_key(key_file):
#     with open(key_file, 'rb') as key_in:
#         return key_in.read()
#
# # Function to encrypt a file
# def encrypt_file(file_path, key):
#     fernet = Fernet(key)
#     with open(file_path, 'rb') as file_in:
#         data = file_in.read()
#     encrypted_data = fernet.encrypt(data)
#     with open(file_path, 'wb') as file_out:
#         file_out.write(encrypted_data)
#
# def convert_csv_to_parquet(csv_file, parquet_file):
#     df = pd.read_csv(csv_file)
#     table = pa.Table.from_pandas(df)
#     pq.write_table(table, parquet_file)
#
# def main():
#     csv_file = 'MOCK_DATA.csv'
#     parquet_file = 'MOCK_DATA.parquet'
#     key_file = 'encryption_key.key'
#
#     # Convert CSV to Parquet
#     convert_csv_to_parquet(csv_file, parquet_file)
#     print(f'Converted {csv_file} to {parquet_file}')
#
#     # Generate encryption key
#     if not os.path.exists(key_file):
#         generate_key(key_file)
#         print(f'Generated encryption key and saved to {key_file}')
#
#     # Load the encryption key
#     key = load_key(key_file)
#
#     # Encrypt the Parquet file
#     encrypt_file(parquet_file, key)
#     print(f'Encrypted {parquet_file} with key from {key_file}')
#
# if __name__ == '__main__':
#     main()




#
#
# import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq
# from cryptography.fernet import Fernet
# import os
# # Function to generate a key and save it to a file
# def generate_key(key_file):
#     key = Fernet.generate_key()
#     with open(key_file, 'wb') as key_out:
#         key_out.write(key)
#     return key
# # Function to load the encryption key from a file
# def load_key(key_file):
#     with open(key_file, 'rb') as key_in:
#         return key_in.read()
# # Function to encrypt a file
# def encrypt_file(file_path, key):
#     fernet = Fernet(key)
#     with open(file_path, 'rb') as file_in:
#         data = file_in.read()
#     encrypted_data = fernet.encrypt(data)
#     with open(file_path, 'wb') as file_out:
#         file_out.write(encrypted_data)
# # Function to decrypt a file
# def decrypt_file(file_path, key):
#     fernet = Fernet(key)
#     with open(file_path, 'rb') as file_in:
#         encrypted_data = file_in.read()
#     decrypted_data = fernet.decrypt(encrypted_data)
#     with open(file_path, 'wb') as file_out:
#         file_out.write(decrypted_data)
# # Function to convert CSV to Parquet
# def convert_csv_to_parquet(csv_file, parquet_file):
#     df = pd.read_csv(csv_file)
#     table = pa.Table.from_pandas(df)
#     pq.write_table(table, parquet_file)
# # Function to convert Parquet to CSV
# def convert_parquet_to_csv(parquet_file, csv_file):
#     table = pq.read_table(parquet_file)
#     df = table.to_pandas()
#     df.to_csv(csv_file, index=False)
# def main():
#     csv_file = 'MOCK_DATA.csv'
#     parquet_file = 'MOCK_DATA.parquet'
#     decrypted_parquet_file = 'decrypted_MOCK_DATA.parquet'
#     restored_csv_file = 'restored_MOCK_DATA.csv'
#     key_file = 'encryption_key.key'
#     # Convert CSV to Parquet
#     convert_csv_to_parquet(csv_file, parquet_file)
#     print(f'Converted {csv_file} to {parquet_file}')
#     # Generate encryption key
#     if not os.path.exists(key_file):
#         generate_key(key_file)
#         print(f'Generated encryption key and saved to {key_file}')
#     # Load the encryption key
#     key = load_key(key_file)
#     # Encrypt the Parquet file
#     encrypt_file(parquet_file, key)
#     print(f'Encrypted {parquet_file} with key from {key_file}')
#     # Decrypt the Parquet file
#     decrypt_file(parquet_file, key)
#     os.rename(parquet_file, decrypted_parquet_file)
#     print(f'Decrypted {parquet_file} to {decrypted_parquet_file}')
#     # Convert decrypted Parquet back to CSV
#     convert_parquet_to_csv(decrypted_parquet_file, restored_csv_file)
#     print(f'Converted {decrypted_parquet_file} back to {restored_csv_file}')
# if __name__ == '__main__':
#     main()
#





#
# import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq
# from cryptography.fernet import Fernet
# import os
# # Function to generate a key and save it to a file
# def generate_key(key_file):
#     key = Fernet.generate_key()
#     with open(key_file, 'wb') as key_out:
#         key_out.write(key)
#     return key
# # Function to load the encryption key from a file
# def load_key(key_file):
#     with open(key_file, 'rb') as key_in:
#         return key_in.read()
# # Function to encrypt a file
# def encrypt_file(file_path, key):
#     fernet = Fernet(key)
#     with open(file_path, 'rb') as file_in:
#         data = file_in.read()
#     encrypted_data = fernet.encrypt(data)
#     with open(file_path, 'wb') as file_out:
#         file_out.write(encrypted_data)
# # Function to decrypt a file
# def decrypt_file(file_path, key):
#     fernet = Fernet(key)
#     with open(file_path, 'rb') as file_in:
#         encrypted_data = file_in.read()
#     decrypted_data = fernet.decrypt(encrypted_data)
#     with open(file_path, 'wb') as file_out:
#         file_out.write(decrypted_data)
# # Function to convert CSV to Parquet
# def convert_csv_to_parquet(csv_file, parquet_file):
#     df = pd.read_csv(csv_file)
#     table = pa.Table.from_pandas(df)
#     pq.write_table(table, parquet_file)
# # Function to convert Parquet to CSV
# def convert_parquet_to_csv(parquet_file, csv_file):
#     table = pq.read_table(parquet_file)
#     df = table.to_pandas()
#     df.to_csv(csv_file, index=False)
# def main():
#     csv_file = 'MOCK_DATA.csv'
#     parquet_file = 'MOCK_DATA.parquet'
#     decrypted_parquet_file = 'decrypted_MOCK_DATA.parquet'
#     restored_csv_file = 'restored_MOCK_DATA.csv'
#     key_file = 'encryption_key.key'
#     # Convert CSV to Parquet
#     convert_csv_to_parquet(csv_file, parquet_file)
#     print(f'Converted {csv_file} to {parquet_file}')
#     # Generate encryption key
#     if not os.path.exists(key_file):
#         generate_key(key_file)
#         print(f'Generated encryption key and saved to {key_file}')
#     # Load the encryption key
#     key = load_key(key_file)
#     # Encrypt the Parquet file
#     encrypt_file(parquet_file, key)
#     print(f'Encrypted {parquet_file} with key from {key_file}')
#     # Decrypt the Parquet file
#     decrypt_file(parquet_file, key)
#     os.rename(parquet_file, decrypted_parquet_file)
#     print(f'Decrypted {parquet_file} to {decrypted_parquet_file}')
#     # Convert decrypted Parquet back to CSV
#     convert_parquet_to_csv(decrypted_parquet_file, restored_csv_file)
#     print(f'Converted {decrypted_parquet_file} back to {restored_csv_file}')
# if __name__ == '__main__':
#     main()
#
#
#



#
# import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq
# from cryptography.fernet import Fernet
# import os
# # Function to generate a key and save it to a file
# def generate_key(key_file):
#     key = Fernet.generate_key()
#     with open(key_file, 'wb') as key_out:
#         key_out.write(key)
#     return key
# # Function to load the encryption key from a file
# def load_key(key_file):
#     with open(key_file, 'rb') as key_in:
#         return key_in.read()
# # Function to encrypt a file
# def encrypt_file(file_path, key):
#     fernet = Fernet(key)
#     with open(file_path, 'rb') as file_in:
#         data = file_in.read()
#     encrypted_data = fernet.encrypt(data)
#     with open(file_path, 'wb') as file_out:
#         file_out.write(encrypted_data)
# # Function to decrypt a file
# def decrypt_file(file_path, key):
#     fernet = Fernet(key)
#     with open(file_path, 'rb') as file_in:
#         encrypted_data = file_in.read()
#     decrypted_data = fernet.decrypt(encrypted_data)
#     with open(file_path, 'wb') as file_out:
#         file_out.write(decrypted_data)
# # Function to convert CSV to Parquet
# def convert_csv_to_parquet(csv_file, parquet_file):
#     df = pd.read_csv(csv_file)
#     table = pa.Table.from_pandas(df)
#     pq.write_table(table, parquet_file)
# # Function to convert Parquet to CSV with normalization
# def convert_parquet_to_csv(parquet_file, csv_file):
#     table = pq.read_table(parquet_file)
#     df = table.to_pandas()
#     df.to_csv(csv_file, index=False, line_terminator='\n', encoding='utf-8')
# def main():
#     csv_file = 'MOCK_DATA.csv'
#     parquet_file = 'MOCK_DATA.parquet'
#     decrypted_parquet_file = 'decrypted_MOCK_DATA.parquet'
#     restored_csv_file = 'restored_MOCK_DATA.csv'
#     key_file = 'encryption_key.key'
#     # Convert CSV to Parquet
#     convert_csv_to_parquet(csv_file, parquet_file)
#     print(f'Converted {csv_file} to {parquet_file}')
#     # Generate encryption key
#     if not os.path.exists(key_file):
#         generate_key(key_file)
#         print(f'Generated encryption key and saved to {key_file}')
#     # Load the encryption key
#     key = load_key(key_file)
#     # Encrypt the Parquet file
#     encrypt_file(parquet_file, key)
#     print(f'Encrypted {parquet_file} with key from {key_file}')
#     # Decrypt the Parquet file
#     decrypt_file(parquet_file, key)
#     os.rename(parquet_file, decrypted_parquet_file)
#     print(f'Decrypted {parquet_file} to {decrypted_parquet_file}')
#     # Convert decrypted Parquet back to CSV with normalization
#     convert_parquet_to_csv(decrypted_parquet_file, restored_csv_file)
#     print(f'Converted {decrypted_parquet_file} back to {restored_csv_file}')
#     # Compare file sizes
#     original_size = os.path.getsize(csv_file)
#     restored_size = os.path.getsize(restored_csv_file)
#     print(f'Original CSV size: {original_size} bytes')
#     print(f'Restored CSV size: {restored_size} bytes')
# if __name__ == '__main__':
#     main()
#fass raha tha






import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from cryptography.fernet import Fernet
import os

# Function to generate a key and save it to a file
def generate_key(key_file):
    key = Fernet.generate_key()
    with open(key_file, 'wb') as key_out:
        key_out.write(key)
    return key

# Function to load the encryption key from a file
def load_key(key_file):
    with open(key_file, 'rb') as key_in:
        return key_in.read()

# Function to encrypt a file
def encrypt_file(file_path, key):
    fernet = Fernet(key)
    with open(file_path, 'rb') as file_in:
        data = file_in.read()
    encrypted_data = fernet.encrypt(data)
    with open(file_path, 'wb') as file_out:
        file_out.write(encrypted_data)

# Function to decrypt a file
def decrypt_file(file_path, key):
    fernet = Fernet(key)
    with open(file_path, 'rb') as file_in:
        encrypted_data = file_in.read()
    decrypted_data = fernet.decrypt(encrypted_data)
    with open(file_path, 'wb') as file_out:
        file_out.write(decrypted_data)

# Function to convert CSV to Parquet
def convert_csv_to_parquet(csv_file, parquet_file):
    df = pd.read_csv(csv_file)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_file)

# Function to convert Parquet to CSV with normalization
def convert_parquet_to_csv(parquet_file, csv_file):
    table = pq.read_table(parquet_file)
    df = table.to_pandas()
    df.to_csv(csv_file, index=False, lineterminator='\n', encoding='utf-8')

def main():
    csv_file = 'MOCK_DATA.csv'
    parquet_file = 'MOCK_DATA.parquet'
    decrypted_parquet_file = 'decrypted_MOCK_DATA.parquet'
    restored_csv_file = 'restored_MOCK_DATA.csv'
    key_file = 'encryption_key.key'

    # Convert CSV to Parquet to
    convert_csv_to_parquet(csv_file, parquet_file)
    print(f'Converted {csv_file} to {parquet_file}')

    # Generate encryption key
    if not os.path.exists(key_file):
        generate_key(key_file)
        print(f'Generated encryption key and saved to {key_file}')

    # Load the encryption key
    key = load_key(key_file)

    # Encrypt the Parquet file
    encrypt_file(parquet_file, key)
    print(f'Encrypted {parquet_file} with key from {key_file}')

    # Decrypt the Parquet file
    decrypt_file(parquet_file, key)
    os.rename(parquet_file, decrypted_parquet_file)
    print(f'Decrypted {parquet_file} to {decrypted_parquet_file}')

    # Convert decrypted Parquet back to CSV with normalization
    convert_parquet_to_csv(decrypted_parquet_file, restored_csv_file)
    print(f'Converted {decrypted_parquet_file} back to {restored_csv_file}')

    # Compare file sizes
    original_size = os.path.getsize(csv_file)
    restored_size = os.path.getsize(restored_csv_file)
    print(f'Original CSV size: {original_size} bytes')
    print(f'Restored CSV size: {restored_size} bytes')

if __name__ == '__main__':
    main()



