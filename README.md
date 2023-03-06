---
duplicated_from: anton-l/common_voice_generator
---
## Common voice release generator

1. Copy the latest release id from the `RELEASES` dict in https://github.com/common-voice/common-voice/blob/main/web/src/components/pages/datasets/releases.ts 
to the `VERSIONS` variable in `generate_datasets.py`.
2. Copy the languages from https://github.com/common-voice/common-voice/blob/release-v1.78.0/web/locales/en/messages.ftl
   (replacing `release-v1.78.0` with the latest version tag) to the `languages.ftl` file.
3. Run `python generate_datasets.py` to generate the dataset repos.
4. `cd ..`
5. `huggingface-cli repo create --type dataset --organization mozilla-foundation common_voice_11_0`
6. `git clone https://huggingface.co/datasets/mozilla-foundation/common_voice_11_0`
7. `cd common_voice_11_0`
8. `cp ../common_voice_generator/common_voice_11_0/* ./`
9. `git add . && git commit -m "Release" && git push`