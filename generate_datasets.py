import json
import os
import shutil

import requests

RELEASE_STATS_URL = "https://commonvoice.mozilla.org/dist/releases/{}.json"
RELEASE_STATS_GH_URL = "https://raw.githubusercontent.com/common-voice/cv-dataset/main/datasets/{}.json"
VERSIONS = [
    {"semver": "1.0.0", "name": "common_voice_1_0", "release": "cv-corpus-1"},
    {"semver": "2.0.0", "name": "common_voice_2_0", "release": "cv-corpus-2"},
    {"semver": "3.0.0", "name": "common_voice_3_0", "release": "cv-corpus-3"},
    {
        "semver": "4.0.0",
        "name": "common_voice_4_0",
        "release": "cv-corpus-4-2019-12-10",
    },
    {
        "semver": "5.0.0",
        "name": "common_voice_5_0",
        "release": "cv-corpus-5-2020-06-22",
    },
    {
        "semver": "5.1.0",
        "name": "common_voice_5_1",
        "release": "cv-corpus-5.1-2020-06-22",
    },
    {
        "semver": "6.0.0",
        "name": "common_voice_6_0",
        "release": "cv-corpus-6.0-2020-12-11",
    },
    {
        "semver": "6.1.0",
        "name": "common_voice_6_1",
        "release": "cv-corpus-6.1-2020-12-11",
    },
    {
        "semver": "7.0.0",
        "name": "common_voice_7_0",
        "release": "cv-corpus-7.0-2021-07-21",
    },
    {
        "semver": "8.0.0",
        "name": "common_voice_8_0",
        "release": "cv-corpus-8.0-2022-01-19",
    },
    {
        "semver": "9.0.0",
        "name": "common_voice_9_0",
        "release": "cv-corpus-9.0-2022-04-27",
    },
    {
        "semver": "10.0.0",
        "name": "common_voice_10_0",
        "release": "cv-corpus-10.0-2022-07-04",
    },
    {
        "semver": "11.0.0",
        "name": "common_voice_11_0",
        "release": "cv-corpus-11.0-2022-09-21",
    },
    {
        "semver": "12.0.0",
        "name": "common_voice_12_0",
        "release": "cv-corpus-12.0-2022-12-07",
        "release_name": "Common Voice Corpus 12",
        "date": "2022-12-07",
    },
    {
        "semver": "13.0.0",
        "name": "common_voice_13_0",
        "release": "cv-corpus-13.0-2023-03-09",
        "release_name": "Common Voice Corpus 13",
        "date": "2022-03-15",
    },
]


def num_to_size(num: int):
    if num < 1000:
        return "n<1K"
    elif num < 10_000:
        return "1K<n<10K"
    elif num < 100_000:
        return "10K<n<100K"
    elif num < 1_000_000:
        return "100K<n<1M"
    elif num < 10_000_000:
        return "1M<n<10M"
    elif num < 100_000_000:
        return "10M<n<100M"
    elif num < 1_000_000_000:
        return "100M<n<1B"


def get_language_names():
    # source: https://github.com/common-voice/common-voice/blob/release-v1.71.0/web/locales/en/messages.ftl
    languages = {}
    with open("languages.ftl") as fin:
        for line in fin:
            lang_code, lang_name = line.strip().split(" = ")
            languages[lang_code] = lang_name

    return languages


def main():
    language_names = get_language_names()

    for version in VERSIONS[-1:]:
        print(version)
        stats_url = RELEASE_STATS_GH_URL.format(version["release"])
        release_stats = requests.get(stats_url).text
        release_stats = json.loads(release_stats)
        release_stats["version"] = version["semver"]
        release_stats["date"] = version["date"]
        release_stats["name"] = version["release_name"]
        release_stats["multilingual"] = True

        dataset_path = version["name"]
        os.makedirs(dataset_path, exist_ok=True)
        with open(f"{dataset_path}/release_stats.py", "w") as fout:
            fout.write("STATS = " + str(release_stats))

        with open(f"README.template.md", "r") as fin:
            readme = fin.read()
            readme = readme.replace("{{NAME}}", version["release_name"])
            readme = readme.replace("{{DATASET_PATH}}", version["name"])

            locales = sorted(release_stats["locales"].keys())
            languages = [f"- {loc}" for loc in locales]
            readme = readme.replace("{{LANGUAGES}}", "\n".join(languages))

            sizes = [f"  {loc}:\n  - {num_to_size(release_stats['locales'][loc]['clips'])}" for loc in locales]
            readme = readme.replace("{{SIZES}}", "\n".join(sizes))

            languages_human = sorted([language_names[loc] for loc in locales])
            readme = readme.replace("{{LANGUAGES_HUMAN}}", ", ".join(languages_human))

            readme = readme.replace("{{TOTAL_HRS}}", str(release_stats["totalHrs"]))
            readme = readme.replace("{{VAL_HRS}}", str(release_stats["totalValidHrs"]))
            readme = readme.replace("{{NUM_LANGS}}", str(len(locales)))

        with open(f"{dataset_path}/README.md", "w") as fout:
            fout.write(readme)
        with open(f"{dataset_path}/languages.py", "w") as fout:
            fout.write("LANGUAGES = " + str(language_names))

        shutil.copy("dataset_script.py", f"{dataset_path}/{dataset_path}.py")


if __name__ == "__main__":
    main()
