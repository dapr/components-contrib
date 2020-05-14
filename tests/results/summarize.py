import os
import json

root_dir = os.path.dirname(os.path.abspath(__file__))
file_abs = os.path.join(root_dir, os.path.abspath(__file__))
file_basename = os.path.basename(__file__)


state_components = {}
for root,d_names,f_names in os.walk(root_dir):
  for f in f_names:
    if f == file_basename:
      continue

    f_name = os.path.join(root, f)
    with open(f_name) as json_file:
      report = json.load(json_file)
      
      if report['component_type'] == "state":
        state_components[report['component_name']] = report

print("State Stores")
print("------------")
for key in state_components:
  v = state_components[key]
  print(f"{v['component_name']}: {v['conformant']}")
  funcs = v['functions']
  for fk in funcs:
    f = funcs[fk]
    if f['included']:
      print(f"\t{f['function_name']}: {f['execution_duration_in_μs']}μs")

