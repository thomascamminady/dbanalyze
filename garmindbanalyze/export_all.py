import glob
import os
import subprocess


ACTIVITIES = "/home/thomas/HealthData/FitFiles/Activities"
EXPORT = "/home/thomas/HealthData/FitFiles/Export"

import subprocess


def run_garmindb_cli(activity_id):
    # Construct the command with the varying ID
    command = [
        f"/home/thomas/Repos/garmindb/.venv/bin/garmindb_cli.py --export-activity {str(activity_id)}"
    ]

    # Run the command
    try:
        print(command)
        result = subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        print("Command output:", result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error:", e.stderr)


if __name__ == "__main__":
    files = sorted(glob.glob(f"{ACTIVITIES}/activity_details_*.json"))

    for file in files:
        id = os.path.basename(file).replace(".json", "").split("_")[-1]
        out = os.path.join(EXPORT, f"{id}.tcx")

        if not os.path.exists(out):
            run_garmindb_cli(id)
