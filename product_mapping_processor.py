import io
import re
from pathlib import Path
from typing import Dict, List, Tuple, Union

import pandas as pd

# Configure basic logging
import logging
logger = logging.getLogger(__name__)


class ProductMappingProcessor:
    def __init__(
        self,
        data_file: Union[str, io.BytesIO],
        mapping_file: Union[str, io.BytesIO],
    ):
        if isinstance(data_file, (str, Path)):
            self.data_file = Path(data_file)
            if not self.data_file.exists():
                raise FileNotFoundError(
                    f"Không tìm thấy file dữ liệu {data_file}"
                )
        else:
            self.data_file = data_file

        if isinstance(mapping_file, (str, Path)):
            self.mapping_file = Path(mapping_file)
            if not self.mapping_file.exists():
                raise FileNotFoundError(
                    f"Không tìm thấy file mapping {mapping_file}"
                )
        else:
            self.mapping_file = mapping_file

        self.old_code_patterns = [
            r"mã\s*gốc",
            r"ma\s*goc",
            r"old.*code",
            r"code.*old",
            r"mã\s*cũ",
            r"ma\s*cu",
        ]
        self.old_name_patterns = [
            r"tên\s*gốc",
            r"ten\s*goc",
            r"old.*name",
            r"name.*old",
            r"tên\s*cũ",
            r"ten\s*cu",
        ]
        self.new_code_patterns = [
            r"mã\s*mới",
            r"ma\s*moi",
            r"new.*code",
            r"code.*new",
            r"mã\s*thay",
            r"ma\s*thay",
        ]
        self.new_name_patterns = [
            r"tên\s*mới",
            r"ten\s*moi",
            r"new.*name",
            r"name.*new",
            r"tên\s*thay",
            r"ten\s*thay",
        ]

    def _normalize_column_name(self, name: str) -> str:
        normalized = name.lower()
        normalized = (
            normalized.replace("đ", "d")
            .replace("ê", "e")
            .replace("ô", "o")
            .replace("ư", "u")
        )
        normalized = re.sub(r"[áàảãạâấầẩẫậăắằẳẵặ]", "a", normalized)
        normalized = re.sub(r"[éèẻẽẹêếềểễệ]", "e", normalized)
        normalized = re.sub(r"[íìỉĩị]", "i", normalized)
        normalized = re.sub(r"[óòỏõọôốồổỗộơớờởỡợ]", "o", normalized)
        normalized = re.sub(r"[úùủũụưứừửữự]", "u", normalized)
        normalized = re.sub(r"[ýỳỷỹỵ]", "y", normalized)
        normalized = re.sub(r"[^a-z0-9]", "", normalized)
        return normalized

    def _find_column_by_pattern(
        self, df: pd.DataFrame, patterns: List[str]
    ) -> str:
        normalized_columns = {
            self._normalize_column_name(col): col for col in df.columns
        }

        for pattern in patterns:
            pattern = pattern.lower()
            for norm_col, original_col in normalized_columns.items():
                if re.search(pattern, norm_col):
                    return original_col

        for pattern in patterns:
            for col in df.columns:
                if pattern.lower() in col.lower():
                    return col

        return None

    def _identify_mapping_columns(self, df: pd.DataFrame) -> Dict[str, str]:
        logger.info(f"Các cột trong file mapping: {list(df.columns)}")

        old_code_col = self._find_column_by_pattern(df, self.old_code_patterns)
        old_name_col = self._find_column_by_pattern(df, self.old_name_patterns)
        new_code_col = self._find_column_by_pattern(df, self.new_code_patterns)
        new_name_col = self._find_column_by_pattern(df, self.new_name_patterns)

        missing_cols = []
        if not old_code_col:
            missing_cols.append("Mã gốc")
        if not old_name_col:
            missing_cols.append("Tên gốc")
        if not new_code_col:
            missing_cols.append("Mã mới")
        if not new_name_col:
            missing_cols.append("Tên mới")

        if missing_cols:
            if len(df.columns) >= 4 and not missing_cols:
                logger.warning(
                    f"Không tìm thấy các cột: {missing_cols}. Sử dụng 4 cột đầu tiên..."
                )
                return {
                    "old_code": df.columns[0],
                    "old_name": df.columns[1],
                    "new_code": df.columns[2],
                    "new_name": df.columns[3],
                }
            else:
                error_msg = f"File mapping thiếu các cột bắt buộc: {', '.join(missing_cols)}. "
                error_msg += f"Các cột hiện có: {', '.join(df.columns)}"
                raise ValueError(error_msg)

        return {
            "old_code": old_code_col,
            "old_name": old_name_col,
            "new_code": new_code_col,
            "new_name": new_name_col,
        }

    def _load_mapping_data(self) -> Tuple[pd.DataFrame, Dict[str, str]]:
        try:
            df_mapping = pd.read_excel(
                self.mapping_file, sheet_name=0, dtype=str, engine="openpyxl"
            )

            df_mapping = df_mapping.fillna("")

            column_mapping = self._identify_mapping_columns(df_mapping)

            logger.info(f"Đã xác định các cột mapping: {column_mapping}")

            return df_mapping, column_mapping

        except Exception as e:
            logger.error(
                f"Lỗi khi đọc dữ liệu mapping: {str(e)}", exc_info=True
            )
            raise

    def _load_data_file(self) -> pd.DataFrame:
        try:
            df_data = pd.read_excel(
                self.data_file, sheet_name=0, dtype=str, engine="openpyxl"
            )

            if "Mã hàng" not in df_data.columns:
                logger.warning(
                    "Không tìm thấy cột 'Mã hàng' trong file dữ liệu!"
                )
                code_col = self._find_column_by_pattern(
                    df_data, self.old_code_patterns + ["ma.*hang", "code"]
                )
                if code_col:
                    logger.info(f"Sử dụng cột '{code_col}' làm cột mã hàng.")
                    df_data = df_data.rename(columns={code_col: "Mã hàng"})
                else:
                    raise ValueError(
                        "File dữ liệu phải chứa cột 'Mã hàng' hoặc cột tương tự"
                    )

            if "Tên hàng" not in df_data.columns:
                name_col = self._find_column_by_pattern(
                    df_data, self.old_name_patterns + ["ten.*hang", "name"]
                )
                if name_col:
                    logger.info(f"Sử dụng cột '{name_col}' làm cột tên hàng.")
                    df_data = df_data.rename(columns={name_col: "Tên hàng"})

            logger.info(f"Các cột trong file dữ liệu: {list(df_data.columns)}")

            return df_data

        except Exception as e:
            logger.error(f"Lỗi khi đọc file dữ liệu: {str(e)}", exc_info=True)
            raise

    def _levenshtein_distance(self, str1, str2):
        try:
            # Try to use a specialized library
            import Levenshtein

            return Levenshtein.distance(str1 or "", str2 or "")
        except ImportError:
            # Fall back to a more efficient custom implementation
            if not str1:
                return len(str2) if str2 else 0
            if not str2:
                return len(str1)

            # Use a single row for dynamic programming (reduced memory)
            prev_row = range(len(str2) + 1)
            for i, c1 in enumerate(str1):
                curr_row = [i + 1]
                for j, c2 in enumerate(str2):
                    cost = 0 if c1 == c2 else 1
                    curr_row.append(
                        min(
                            curr_row[j] + 1,
                            prev_row[j + 1] + 1,
                            prev_row[j] + cost,
                        )
                    )
                prev_row = curr_row

            return prev_row[-1]

    def _find_closest_match(
        self, code, mapping_df, new_code_col, old_code_col, threshold=0.70
    ):
        if not code or code == "":
            return None, 0

        valid_mapping = mapping_df[
            mapping_df[new_code_col].notna() & (mapping_df[new_code_col] != "")
        ]

        if valid_mapping.empty:
            return None, 0

        prefix_length = min(3, len(code))
        if prefix_length > 0:
            prefix = code[:prefix_length]
            prefix_matches = valid_mapping[
                valid_mapping[new_code_col].str.startswith(prefix)
            ]

            if not prefix_matches.empty:
                valid_mapping = prefix_matches

        best_match = None
        highest_similarity = 0

        new_codes = valid_mapping[new_code_col].values
        old_codes = valid_mapping[old_code_col].values

        for i, new_code in enumerate(new_codes):
            if not new_code or new_code == "":
                continue

            distance = self._levenshtein_distance(code, new_code)

            max_len = max(len(code), len(new_code))
            similarity = 1 - (distance / max_len) if max_len > 0 else 0

            if similarity > highest_similarity:
                highest_similarity = similarity
                best_match = old_codes[i]

        if highest_similarity >= threshold:
            return best_match, highest_similarity
        else:
            return None, 0

    def process_to_buffer(self, output_buffer: io.BytesIO) -> dict:
        try:
            mapping_df, column_mapping = self._load_mapping_data()
            data_df = self._load_data_file()

            old_code_col = column_mapping["old_code"]
            new_code_col = column_mapping["new_code"]
            new_name_col = column_mapping["new_name"]

            code_mapping = dict(
                zip(mapping_df[old_code_col], mapping_df[new_code_col])
            )
            name_mapping = dict(
                zip(mapping_df[old_code_col], mapping_df[new_name_col])
            )

            logger.info(f"Đã tạo mapping cho {len(code_mapping)} mã hàng")

            total_count = len(data_df)
            exact_matched_count = 0
            reverse_matched_count = 0
            fuzzy_matched_count = 0

            if "Mã hàng" in data_df.columns:
                # 1. Phương pháp 1: Ánh xạ trực tiếp từ mã cũ sang mã mới
                data_df["Mã hàng_mới"] = data_df["Mã hàng"].map(code_mapping)
                data_df["Tên hàng_mới"] = data_df["Mã hàng"].map(name_mapping)

                exact_matched_count = data_df["Mã hàng_mới"].notna().sum()
                logger.info(
                    f"Số lượng bản ghi được ánh xạ trực tiếp: {exact_matched_count}/{total_count}"
                )

                # 2. Phương pháp 2: Tìm kiếm ngược - kiểm tra nếu mã hàng đã tồn tại trong cột mã mới
                reverse_code_mapping = {}
                for idx, row in mapping_df.iterrows():
                    new_code = row[new_code_col]
                    old_code = row[old_code_col]
                    if pd.notna(new_code) and new_code != "":
                        reverse_code_mapping[new_code] = old_code

                unmatched_mask = data_df["Mã hàng_mới"].isna() | (
                    data_df["Mã hàng_mới"] == ""
                )
                unmatched_indices = data_df[unmatched_mask].index

                for idx in unmatched_indices:
                    current_code = data_df.at[idx, "Mã hàng"]

                    if pd.isna(current_code) or current_code == "":
                        continue

                    if current_code in reverse_code_mapping:
                        old_code = reverse_code_mapping[current_code]
                        data_df.at[idx, "Mã hàng_mới"] = code_mapping.get(
                            old_code, ""
                        )
                        data_df.at[idx, "Tên hàng_mới"] = name_mapping.get(
                            old_code, ""
                        )
                        reverse_matched_count += 1

                logger.info(
                    f"Số lượng bản ghi được ánh xạ qua tìm kiếm ngược: {reverse_matched_count}"
                )

                # 3. Phương pháp 3: Ánh xạ mờ dựa trên độ tương đồng
                still_unmatched_mask = data_df["Mã hàng_mới"].isna() | (
                    data_df["Mã hàng_mới"] == ""
                )
                still_unmatched_indices = data_df[still_unmatched_mask].index

                for idx in still_unmatched_indices:
                    current_code = data_df.at[idx, "Mã hàng"]

                    if pd.isna(current_code) or current_code == "":
                        continue

                    best_match, similarity = self._find_closest_match(
                        current_code,
                        mapping_df,
                        new_code_col,
                        old_code_col,
                        threshold=0.70,
                    )

                    if best_match:
                        data_df.at[idx, "Mã hàng_mới"] = code_mapping.get(
                            best_match, ""
                        )
                        data_df.at[idx, "Tên hàng_mới"] = name_mapping.get(
                            best_match, ""
                        )
                        fuzzy_matched_count += 1

                logger.info(
                    f"Số lượng bản ghi được ánh xạ qua ánh xạ mờ: {fuzzy_matched_count}"
                )

                matched_count = (
                    exact_matched_count
                    + reverse_matched_count
                    + fuzzy_matched_count
                )
                logger.info(
                    f"Tổng số bản ghi được ánh xạ: {matched_count}/{total_count}"
                )

                data_df.loc[
                    data_df["Mã hàng_mới"].notna()
                    & (data_df["Mã hàng_mới"] != ""),
                    "Mã hàng",
                ] = data_df["Mã hàng_mới"]

                if "Tên hàng" in data_df.columns:
                    data_df.loc[
                        data_df["Tên hàng_mới"].notna()
                        & (data_df["Tên hàng_mới"] != ""),
                        "Tên hàng",
                    ] = data_df["Tên hàng_mới"]

                data_df = data_df.drop(columns=["Mã hàng_mới", "Tên hàng_mới"])

            with pd.ExcelWriter(output_buffer, engine="openpyxl") as writer:
                data_df.to_excel(writer, index=False)

            output_buffer.seek(0)
            logger.info("Đã hoàn thành xử lý và ghi vào buffer")

            return {
                "matched_count": int(matched_count),
                "total_count": total_count,
                "exact_matched_count": int(exact_matched_count),
                "reverse_matched_count": int(reverse_matched_count),
                "fuzzy_matched_count": int(fuzzy_matched_count),
            }

        except Exception as e:
            logger.error(f"Lỗi khi xử lý dữ liệu: {str(e)}", exc_info=True)
            raise