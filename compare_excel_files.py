import pandas as pd
import numpy as np
from pathlib import Path

def compare_excel_files(file1_path, file2_path, key_column=None):
    """
    Compare two Excel files and show differences
    """
    print(f"Comparing {file1_path} vs {file2_path}")
    print("=" * 80)

    try:
        df1 = pd.read_excel(file1_path)
        df2 = pd.read_excel(file2_path)

        file1_name = Path(file1_path).stem
        file2_name = Path(file2_path).stem

        print(f"\n📊 FILE STRUCTURES:")
        print(f"{file1_name}: {df1.shape[0]} rows × {df1.shape[1]} columns")
        print(f"{file2_name}: {df2.shape[0]} rows × {df2.shape[1]} columns")

        print(f"\n📋 COLUMNS:")
        print(f"{file1_name} columns: {list(df1.columns)}")
        print(f"{file2_name} columns: {list(df2.columns)}")

        # Find common columns
        common_cols = set(df1.columns) & set(df2.columns)
        file1_only_cols = set(df1.columns) - set(df2.columns)
        file2_only_cols = set(df2.columns) - set(df1.columns)

        print(f"\n🔗 COLUMN ANALYSIS:")
        print(f"Common columns ({len(common_cols)}): {list(common_cols)}")
        if file1_only_cols:
            print(f"Only in {file1_name} ({len(file1_only_cols)}): {list(file1_only_cols)}")
        if file2_only_cols:
            print(f"Only in {file2_name} ({len(file2_only_cols)}): {list(file2_only_cols)}")

        # Auto-detect key column if not provided
        if key_column is None:
            possible_keys = ["Số điện thoại", "Phone", "ID", "Code", "Mã"]
            for col in possible_keys:
                if col in common_cols:
                    key_column = col
                    break

            if key_column is None and common_cols:
                key_column = list(common_cols)[0]

        if key_column and key_column in common_cols:
            print(f"\n🔑 Using '{key_column}' as comparison key")

            # Get records using key column
            df1_keys = set(df1[key_column].dropna().astype(str))
            df2_keys = set(df2[key_column].dropna().astype(str))

            only_in_file1 = df1_keys - df2_keys
            only_in_file2 = df2_keys - df1_keys
            common_keys = df1_keys & df2_keys

            print(f"\n📊 DATA COMPARISON:")
            print(f"Records only in {file1_name}: {len(only_in_file1)}")
            print(f"Records only in {file2_name}: {len(only_in_file2)}")
            print(f"Common records: {len(common_keys)}")

            # Save differences to CSV files
            if only_in_file1:
                only_file1_df = df1[df1[key_column].astype(str).isin(only_in_file1)]
                only_file1_df.to_csv(f"only_in_{file1_name}.csv", index=False)
                print(f"✅ Saved records only in {file1_name} to: only_in_{file1_name}.csv")

                print(f"\n📋 Sample records only in {file1_name}:")
                for i, key in enumerate(sorted(only_in_file1)[:5]):
                    print(f"  {i+1}. {key}")
                if len(only_in_file1) > 5:
                    print(f"  ... and {len(only_in_file1) - 5} more")

            if only_in_file2:
                only_file2_df = df2[df2[key_column].astype(str).isin(only_in_file2)]
                only_file2_df.to_csv(f"only_in_{file2_name}.csv", index=False)
                print(f"✅ Saved records only in {file2_name} to: only_in_{file2_name}.csv")

                print(f"\n📋 Sample records only in {file2_name}:")
                for i, key in enumerate(sorted(only_in_file2)[:5]):
                    print(f"  {i+1}. {key}")
                if len(only_in_file2) > 5:
                    print(f"  ... and {len(only_in_file2) - 5} more")

            # Check for value differences in common records
            if common_keys and len(common_cols) > 1:
                print(f"\n🔍 CHECKING VALUE DIFFERENCES IN COMMON RECORDS...")
                differences = []

                for key in list(common_keys)[:100]:  # Limit to first 100 for performance
                    row1 = df1[df1[key_column].astype(str) == key]
                    row2 = df2[df2[key_column].astype(str) == key]

                    if len(row1) == 1 and len(row2) == 1:
                        for col in common_cols:
                            if col != key_column:
                                val1 = row1[col].iloc[0]
                                val2 = row2[col].iloc[0]

                                if pd.isna(val1) and pd.isna(val2):
                                    continue
                                elif pd.isna(val1) or pd.isna(val2) or str(val1) != str(val2):
                                    differences.append({
                                        'Key': key,
                                        'Column': col,
                                        f'{file1_name}_Value': val1,
                                        f'{file2_name}_Value': val2
                                    })

                if differences:
                    diff_df = pd.DataFrame(differences)
                    diff_df.to_csv(f"value_differences_{file1_name}_vs_{file2_name}.csv", index=False)
                    print(f"✅ Found {len(differences)} value differences")
                    print(f"✅ Saved to: value_differences_{file1_name}_vs_{file2_name}.csv")
                else:
                    print("✅ No value differences found in common records")

        print(f"\n📈 SUMMARY:")
        print(f"• {file1_name}: {df1.shape[0]} rows, {df1.shape[1]} columns")
        print(f"• {file2_name}: {df2.shape[0]} rows, {df2.shape[1]} columns")
        print(f"• Column differences: {len(file1_only_cols) + len(file2_only_cols)}")
        if key_column and key_column in common_cols:
            print(f"• Unique record differences: {len(only_in_file1) + len(only_in_file2)}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    file1 = "CRM_Online_Data_20250921.xlsx"
    file2 = "ke-toan-online-15-9.xlsx"

    compare_excel_files(file1, file2)
