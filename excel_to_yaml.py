import pandas as pd
import math
from collections import OrderedDict
import re
from ruamel.yaml import YAML


class ExcelToYaml:
    def __init__(self):
        self.ne = None
        self.engine_family_name = None


    def read_excel(self, file_path, sheet_name):
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        return df

    def set_parameters(self, df):
        oper_col_start = df.columns.get_loc('VZW')
        oper_col_end = df.columns.get_loc('UTD_NVGNB')
        index_col_start = df.columns.get_loc('Index0')
        index_col_end = df.columns.get_loc('Index5')
        system_name_loc = df.columns.get_loc('System Family Name')
        type_name_loc = df.columns.get_loc('Type Name')
        type_unit_loc = df.columns.get_loc('Unit')
        return oper_col_start, oper_col_end, index_col_start, index_col_end, system_name_loc, type_name_loc, type_unit_loc

    def create_yaml(self, data, output_path):
        yaml = YAML()
        yaml.indent(mapping=2, sequence=4, offset=2)
        with open(output_path, 'w', encoding='utf-8') as file:
            yaml.dump(data, file)

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

    def cnt_fields(self, index, df, system_name_loc, type_name_loc, type_unit_loc):
        type_value = []
        type_unit = []
        merge_cnt = 1
        while index + merge_cnt < len(df) and not isinstance(df.iloc[index + merge_cnt, system_name_loc], str):
            merge_cnt += 1
        for merge_idx in range(merge_cnt):
            if isinstance(df.iloc[index + merge_idx, type_name_loc], str):
                field_str, field_num = self.extract_number_after_string(df.iloc[index + merge_idx, type_name_loc], '0~')
                if field_num:
                    for i in range(field_num+1):
                        type_value.append(field_str+str(i))
                        type_unit.append(df.iloc[index + merge_idx, type_unit_loc])
                else:
                    type_value.append(df.iloc[index + merge_idx, type_name_loc])
                    type_unit.append(df.iloc[index + merge_idx, type_unit_loc])
        return type_value, type_unit

    def convert_to_yaml(self, df, oper_col_start, oper_col_end, index_col_start, index_col_end, system_name_loc, type_name_loc, type_unit_loc):
        yaml_data = OrderedDict({'pm':OrderedDict({})})

        operators = df.columns[oper_col_start:oper_col_end+1]
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
                indexes = row[index_col_start:index_col_end+1]
                index_value = [index for index in indexes if isinstance(index, str)]
                additional_params = {'granularity': '1h'}
                type_value, type_unit = self.cnt_fields(index, df, system_name_loc, type_name_loc, type_unit_loc)

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
    oper_col_start, oper_col_end, index_col_start, index_col_end, system_name_loc, type_name_loc, type_unit_loc = excel_to_yaml.set_parameters(df)
    yaml_data = excel_to_yaml.convert_to_yaml(df, oper_col_start, oper_col_end, index_col_start, index_col_end, system_name_loc, type_name_loc, type_unit_loc)

    excel_to_yaml.create_yaml(yaml_data, output_path)

if __name__ == "__main__":
    main()
