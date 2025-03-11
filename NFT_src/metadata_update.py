import os
import json


def update_json_files(directory):
    json_files = [f for f in os.listdir(directory) if f.endswith('.json')]

    for index, file_name in enumerate(json_files):
        file_path = os.path.join(directory, file_name)

        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        data["image"] = f"ipfs://bafybeibuiftzhds2jyz3cpcnmvymprn5q6neuy6iy5fspf6jg2xy26yy2q/{index}"

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)

    print(f"Updated {len(json_files)} files in '{directory}'.")


if __name__ == "__main__":
    update_json_files("test_metadata")
