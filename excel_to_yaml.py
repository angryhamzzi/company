import pandas as pd
import yaml
import math
from collections import OrderedDict
import re


class ExcelToYaml:
    def __init__(self):
        self.oper_col_start = 17
        self.oper_col_end = 38
        self.index_col_start = 8
        self.index_col_end = 13
        self.ne = None
        self.engine_family_name = None
        self.system_name_loc = 5
        self.type_name_loc = 13
        self.type_unit_loc = 14

    def read_excel(self, file_path, sheet_name):
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        return df


    def create_yaml(self, data, output_path):
        with open(output_path, 'w', encoding='utf-8') as file:
            yaml.dump(data, file, default_flow_style=False)

    def collect_ne(self, ne):
        if 'ADPF' in ne:
            return 'adpf'
        elif 'ACPF' in ne:
            return 'acpf'
        elif 'ENB' in ne:
            return 'enb'
        else:
            return None

    def convert_ordered_dict_to_dict(self, ordered_dict):
        if isinstance(ordered_dict, OrderedDict):
            ordered_dict = dict(ordered_dict)
            return {key: self.convert_ordered_dict_to_dict(value) for key, value in ordered_dict.items()}
        elif isinstance(ordered_dict, dict):
            return {key: self.convert_ordered_dict_to_dict(value) for key, value in ordered_dict.items()}
        else:
            return ordered_dict

    def extract_number_after_string(self, text, target_string):
        pattern = rf'(.*)(?={re.escape(target_string)}){re.escape(target_string)}(\d{{2}})'
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip(), int(match.group(2))
        return None, None

    def cnt_fields(self, index, df):
        type_value = []
        type_unit = []
        merge_cnt = 1
        while index + merge_cnt < len(df) and not isinstance(df.iloc[index + merge_cnt, self.system_name_loc], str):
            merge_cnt += 1
        for merge_idx in range(merge_cnt):
            if isinstance(df.iloc[index + merge_idx, self.type_name_loc], str):
                field_str, field_num = self.extract_number_after_string(df.iloc[index + merge_idx, self.type_name_loc], '0~')
                if field_num:
                    for i in range(field_num+1):
                        type_value.append(field_str+str(i))
                        type_unit.append(df.iloc[index + merge_idx, self.type_unit_loc])
                else:
                    type_value.append(df.iloc[index + merge_idx, self.type_name_loc])
                    type_unit.append(df.iloc[index + merge_idx, self.type_unit_loc])
        return type_value, type_unit

    def convert_to_yaml(self, df):
        yaml_data = OrderedDict({'pm':OrderedDict({})})

        operators = df.columns[self.oper_col_start-1:self.oper_col_end]
        for oper_index, operator in enumerate(operators):
            yaml_data['pm'][operator] = OrderedDict({'counters': OrderedDict({})})
            for index, row in df.iterrows():

                excel_ne_name = row['NE']
                if isinstance(excel_ne_name, str):
                    self.ne_name = self.collect_ne(excel_ne_name)
                engine_family_name = row['Engine Family Name']
                if isinstance(engine_family_name, str):
                    self.engine_family_name = engine_family_name
                system_family_name = row['System Family Name']
                if not isinstance(system_family_name, str):
                    continue
                indexes = row[self.index_col_start - 1:self.index_col_end]
                index_value = [index for index in indexes if isinstance(index, str)]
                additional_params = {'granularity': '1h'}
                type_value, type_unit = self.cnt_fields(index, df)

                if row[operator] == 'O':
                    if self.ne_name not in yaml_data['pm'][operator]['counters']:
                        yaml_data['pm'][operator]['counters'][self.ne_name] = OrderedDict({})
                    if self.engine_family_name not in yaml_data['pm'][operator]['counters'][self.ne_name]:
                        yaml_data['pm'][operator]['counters'][self.ne_name][self.engine_family_name] = OrderedDict({
                            'system_family_name': system_family_name,
                            'index': {},
                            'fields': {},
                            'additional_tags': {'dataGroup': {}},
                            'additional_params': additional_params
                        })
                        for index in index_value:
                            yaml_data['pm'][operator]['counters'][self.ne_name][self.engine_family_name]['index'][index] = index
                        for idx, type in enumerate(type_value):
                            yaml_data['pm'][operator]['counters'][self.ne_name][self.engine_family_name]['fields'][f"{type}_{type_unit[idx]}_"] = f"{type}({type_unit[idx]})"
        yaml_data = self.convert_ordered_dict_to_dict(yaml_data)
        return yaml_data


def main():
    excel_to_yaml = ExcelToYaml()
    file_path = './SVR25B_NR_LBM_interface_engine_name.xlsx'
    sheet_name = 'PM Counter'
    output_path = 'data_list2.yaml'

    df = excel_to_yaml.read_excel(file_path, sheet_name)

    yaml_data = excel_to_yaml.convert_to_yaml(df)

    excel_to_yaml.create_yaml(yaml_data, output_path)

if __name__ == "__main__":
    main()