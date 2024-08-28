import glob
import os
import subprocess
from fit2parquets import fit2parquets


ACTIVITIES = "/home/thomas/HealthData/FitFiles/Activities"
EXPORT = "/home/thomas/HealthData/FitFiles/Parsed"


if __name__ == "__main__":
    files = sorted(glob.glob(f"{ACTIVITIES}/*_ACTIVITY.fit"))

    for file in files:
        id = os.path.basename(file).replace("_ACTIVITY.fit", "")
        out_dir = os.path.join(EXPORT, f"id={id}")
        try:
            if not os.path.exists(out_dir):
                fit2parquets(
                    file,
                    write_to_folder_in_which_fit_file_lives=False,
                    alternate_folder_path=out_dir,
                )
        except Exception as e:
            print(e)
