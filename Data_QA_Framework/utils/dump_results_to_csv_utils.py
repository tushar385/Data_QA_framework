import csv
import os

def dump_results_to_csv(results, function_name):
    # Create artifacts directory if it doesn't exist
    os.makedirs('artifacts', exist_ok=True)
    
    # Define the CSV file path based on the function name
    csv_file_path = os.path.join('artifacts', f'{function_name}_results.csv')

    # Write results to CSV
    with open(csv_file_path, mode='w', newline='') as csv_file:
        fieldnames = results[0].keys() if results else []
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()
        for result in results:
            writer.writerow(result)

    print(f"Results have been dumped to {csv_file_path}")