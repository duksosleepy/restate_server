import pandas as pd

def analyze_row_differences(file1_path, file2_path):
    # Read both files
    df1 = pd.read_excel(file1_path)
    df2 = pd.read_excel(file2_path)

    print(f"=== ROW DIFFERENCE ANALYSIS ===")
    print(f"File 1 ({file1_path}): {len(df1)} rows")
    print(f"File 2 ({file2_path}): {len(df2)} rows")
    print(f"Difference: {abs(len(df1) - len(df2))} rows")
    print()

    # Check duplicates by phone number
    phone_col = "Số điện thoại"

    if phone_col in df1.columns and phone_col in df2.columns:
        print("=== PHONE NUMBER FREQUENCY ANALYSIS ===")

        # Count occurrences of each phone number
        phone_counts_1 = df1[phone_col].value_counts()
        phone_counts_2 = df2[phone_col].value_counts()

        print(f"Unique phone numbers in file 1: {len(phone_counts_1)}")
        print(f"Unique phone numbers in file 2: {len(phone_counts_2)}")
        print()

        # Find phones with different frequencies
        all_phones = set(phone_counts_1.index) | set(phone_counts_2.index)
        differences = []

        for phone in all_phones:
            count1 = phone_counts_1.get(phone, 0)
            count2 = phone_counts_2.get(phone, 0)
            if count1 != count2:
                differences.append({
                    'phone': phone,
                    'file1_count': count1,
                    'file2_count': count2,
                    'difference': count1 - count2
                })

        if differences:
            print("=== PHONES WITH DIFFERENT FREQUENCIES ===")
            for diff in sorted(differences, key=lambda x: abs(x['difference']), reverse=True):
                print(f"Phone: {diff['phone']}")
                print(f"  File 1: {diff['file1_count']} occurrences")
                print(f"  File 2: {diff['file2_count']} occurrences")
                print(f"  Difference: {diff['difference']}")
                print()
        else:
            print("All phone numbers have same frequency in both files")

    # Check for empty/null rows
    print("=== NULL/EMPTY DATA ANALYSIS ===")

    # Check for completely empty rows
    empty_rows_1 = df1.isnull().all(axis=1).sum()
    empty_rows_2 = df2.isnull().all(axis=1).sum()

    print(f"Completely empty rows in file 1: {empty_rows_1}")
    print(f"Completely empty rows in file 2: {empty_rows_2}")

    # Check for null phone numbers
    if phone_col in df1.columns and phone_col in df2.columns:
        null_phones_1 = df1[phone_col].isnull().sum()
        null_phones_2 = df2[phone_col].isnull().sum()

        print(f"Null phone numbers in file 1: {null_phones_1}")
        print(f"Null phone numbers in file 2: {null_phones_2}")

    # Show column comparison
    print("\n=== COLUMN STRUCTURE COMPARISON ===")
    cols1 = set(df1.columns)
    cols2 = set(df2.columns)

    print(f"Columns in file 1: {len(cols1)}")
    print(f"Columns in file 2: {len(cols2)}")

    only_in_1 = cols1 - cols2
    only_in_2 = cols2 - cols1

    if only_in_1:
        print(f"Columns only in file 1: {list(only_in_1)}")
    if only_in_2:
        print(f"Columns only in file 2: {list(only_in_2)}")

    # Sample rows for manual inspection
    print("\n=== SAMPLE DATA PREVIEW ===")
    print("First 3 rows of file 1:")
    print(df1.head(3))
    print("\nFirst 3 rows of file 2:")
    print(df2.head(3))

if __name__ == "__main__":
    file1 = "CRM_Offline_Data_20250921.xlsx"
    file2 = "ke-toan-offline-15-9.xlsx"

    analyze_row_differences(file1, file2)