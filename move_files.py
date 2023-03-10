#!/usr/bin/env python3
import os
import pandas
from tqdm import tqdm

# To change according to version and language
# ---------
lang = "ab"
clip_path = f"/home/vaibhav_huggingface_co/common_voice_dataset_generator/data/{lang}/cv-corpus-12.0-2022-12-07/{lang}/clips"
# ---------

splits = ("test", "dev", "train", "other", "invalidated", "validated")
for split in splits:
    data = pandas.read_csv(f"/home/vaibhav_huggingface_co/common_voice_dataset_generator/data/{lang}/cv-corpus-12.0-2022-12-07/{lang}/{split}.tsv", sep='\t')
    all_files = [os.path.join(clip_path, f) for f in list(data["path"])]

    # nums = [f.split("_")[-1].split(".mp3")[0] for f in all_files]
    # nums = [int(s) for s in nums]

    num_files = len(all_files)
    files_per_archive = 4_000

    print(f"Moving {num_files} files...")

    # max_num = max([int(s) for s in nums])
    # num_per_dir = 1_000_000
    dir_path = "{lang}_{split}_{idx}"
    new_clip_path = f"./audio/{lang}/{split}"

    for start_idx in tqdm(range(0, num_files, files_per_archive), desc="moving files"):
        target_dir = os.path.join(new_clip_path, dir_path.format(lang=lang, split=split, idx=start_idx))
        command = f"mkdir -p {target_dir}"
        print(command)
        os.system(command)
        curr_archive_files = all_files[start_idx:start_idx+files_per_archive]
        for file in curr_archive_files:
            command = f"mv {os.path.join(clip_path, file)} {os.path.join(target_dir, file)}"
            os.system(command)

    all_dirs = [d for d in os.listdir(new_clip_path) if os.path.isdir(os.path.join(new_clip_path, d))]
    for directory in tqdm(all_dirs, desc="taring files"):
        command = f"tar -cvf {new_clip_path}/{directory}.tar {new_clip_path}/{directory}"
        print(command)
        os.system(command)
