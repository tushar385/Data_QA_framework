import json

def print_decorated_json(results):
    # Print each item in a formatted way
    formatted_output = json.dumps(results, indent=2, ensure_ascii=False)
    print(formatted_output)
