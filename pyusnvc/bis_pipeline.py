"""
This script wraps the usnvc logic for use in the bis data pipline project.
It can be run locally following the instructions below. It is not part of the usnvc
analysis and core scientific functionality of this package.
see https://github.com/bgotthold-usgs/bis_pipeline/blob/master/README.md for more details 
"""

import os
import json
import time
from pyusnvc.usnvc import *

version = 2.03
json_schema = None
try:
    with open(os.path.join(os.path.dirname(__file__), './usnvc_unit_schema_2.03.json'), 'r') as json_schema_file:
        json_schema = json.load(json_schema_file)
except FileNotFoundError as e:
    pass

# # # # # # # # TO RUN THIS BIS PIPELINE FILE LOCALLY UNCOMMENT BELOW # # # # # # # # #

# # file should exist here
# get_source_item()
# path = './'
# file_name = 'NVC v2.03 2019-03.zip'
# version = 2.03
# # with open(os.path.join(os.path.dirname(__file__), './resources/usnvc_unit_schema_2.03.json'), 'r') as json_schema_file:
# #     json_schema = json.load(json_schema_file)


# def send_final_result(obj):
#     print(json.dumps(obj))


# def send_to_stage(obj, stage):
#     globals()['process_{}'.format(stage)](path, file_name,
#                                           ch_ledger(), send_final_result,
#                                           send_to_stage, obj)


# class ch_ledger:
#     def log_change_event(self, change_id, file_name, function_name, change_name, change_description,
#                          source, result):
#         print('\n\n\n', change_id, change_name, file_name, function_name, change_description,
#               source, result, '\n\n\n')


# def main():
#     process_1(path, file_name, ch_ledger(),
#               send_final_result, send_to_stage, None)
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


# The first processing stage.
# It creates 1 final result and many other results that it sends to the next
#  stage for further processing.
# It returns count which is the number of rows created by this stage.
def process_1(path, file_name, ch_ledger, send_final_result,
              send_to_stage, previous_stage_result):

    file_name = file_name.replace('.zip', '.db')
    count = 0
    for element_global_id in all_keys(path + file_name):
        send_to_stage({'element_global_id': element_global_id}, 2)
        time.sleep(0.02)  # 2 ms
        count += 1
    return count


# The second processing stage used the previous_stage_result and sends
#  a singe document to be handled as a final result.
# It returns 1
def process_2(path, file_name, ch_ledger, send_final_result,
              send_to_stage, previous_stage_result):

    file_name = file_name.replace('.zip', '.db')
    element_global_id = previous_stage_result['element_global_id']
    process_result = build_unit(
        element_global_id, source_data_filename=path + file_name, version_number=version, change_log_function=ch_ledger.log_change_event)

    final_result = {'data': process_result,
                    'row_id': str(element_global_id)}
    send_final_result(final_result)
    return 1


if __name__ == "__main__":
    main()
