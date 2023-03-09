#!/usr/bin/env python3
import os
import logging
import pandas
from tqdm import tqdm
import tarfile
import logging
import os
from pathlib import Path
import csv
from functools import partial
from multiprocessing import Pool


logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)


files_per_archive = 40_000


def make_archive(archive_index_with_files, output_dir, lang, split):
    archive_index, files = archive_index_with_files
    archive_dir = f"{lang}_{split}_{archive_index}"
    archive_path = os.path.join(output_dir, f"{archive_dir}.tar")
    with tarfile.open(archive_path, "w") as tar:
        for file in files:
            _, filename = os.path.split(file)
            tar.add(file, arcname=os.path.join(archive_dir, filename))


def extract_archive(archive_path, target_dir):
    with tarfile.open(archive_path, 'r:gz') as f:
        f.extractall(path=target_dir)


def main():
    langs = 
    for lang in tqdm(langs, desc="languages"):
        extract = True
        logging.info(f"Language: {lang.upper()}, files per tar: {files_per_archive}")
        orig_archive_path = f"/home/vaibhav_huggingface_co/data/cv11/{lang}.tar.gz"
        if extract:
            logging.info("Extracting original archive...")
            extract_archive(orig_archive_path, target_dir="/home/vaibhav_huggingface_co/data/cv11/")
            logging.info("Extracted.")

        clip_path = f"/home/vaibhav_huggingface_co/data/cv11/cv-corpus-11.0-2022-09-21/{lang}/clips"
        # data_size = sum(f.stat().st_size for f in Path(clip_path).glob('*.mp3') if f.is_file())
        # data_size_gb = data_size / 1024 ** 3
        # num_procs = min(int(data_size_gb) + 2, 28)

        splits = ("test", "dev", "train", "other", "invalidated")

        for split in splits:
            meta_path = f"/home/vaibhav_huggingface_co/data/cv11/cv-corpus-11.0-2022-09-21/{lang}/{split}.tsv"
            new_meta_dir = f"repos/common_voice_11_0/transcript/{lang}/"
            Path(new_meta_dir).mkdir(parents=True, exist_ok=True)

            data = pandas.read_csv(meta_path, sep='\t', quoting=csv.QUOTE_NONE, low_memory=False)
            copy_command = f"cp {meta_path} {new_meta_dir}"
            os.system(copy_command)

            all_files = [os.path.join(clip_path, filename) for filename in list(data["path"])]

            num_files = len(all_files)
            if num_files == 0:
                continue

            logging.info(f"split: {split.upper()}, num_files: {num_files}")

            new_clip_path = f"repos/common_voice_11_0/audio/{lang}/{split}"
            Path(new_clip_path).mkdir(parents=True, exist_ok=True)

            file_groups = [
                (arch_index_in_dir, all_files[start_index:start_index + files_per_archive])
                for arch_index_in_dir, start_index in enumerate(range(0, num_files, files_per_archive))
            ]

            n_file_groups = len(file_groups)
            num_procs = max(1, min(n_file_groups, 26))
            logging.info(f"N groups: {n_file_groups}, num procs: {num_procs}")

            if n_file_groups > 1:
                pool = Pool(num_procs)
                pool.map(
                    partial(
                        make_archive,
                        output_dir=new_clip_path,
                        lang=lang,
                        split=split,
                    ),
                    tqdm(file_groups, desc=f"Taring {split} subset...", position=0),
                )
            else:
                make_archive(
                    file_groups[0],
                    output_dir=new_clip_path,
                    lang=lang,
                    split=split,
                )


if __name__ == "__main__":
    main()
