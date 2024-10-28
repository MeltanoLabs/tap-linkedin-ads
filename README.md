# `tap-linkedin-ads`

LinkedInAds tap class.

Built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`
* `batch`

## Settings

| Setting | Required | Default | Description |
|:--------|:--------:|:-------:|:------------|
| access_token | False    | None    | The token to authenticate against the API service |
| oauth_credentials | False    | None    | LinkedIn Ads OAuth Credentials |
| oauth_credentials.refresh_token | False    | None    | LinkedIn Ads Refresh Token |
| oauth_credentials.client_id | False    | None    | LinkedIn Ads Client ID |
| oauth_credentials.client_secret | False    | None    | LinkedIn Ads Client Secret |
| start_date | True     | None    | The earliest record date to sync |
| end_date | False    | 2024-10-23T22:57:56.958248+00:00 | The latest record date to sync |
| user_agent | False    | tap-linkedin-ads <api_user_email@your_company.com> | API ID      |
| stream_maps | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config | False    | None    | User-defined config values to be used within map expressions. |
| faker_config | False    | None    | Config for the [`Faker`](https://faker.readthedocs.io/en/master/) instance variable `fake` used within map expressions. Only applicable if the plugin specifies `faker` as an addtional dependency (through the `singer-sdk` `faker` extra or directly). |
| faker_config.seed | False    | None    | Value to seed the Faker generator for deterministic output: https://faker.readthedocs.io/en/master/#seeding-the-generator |
| faker_config.locale | False    | None    | One or more LCID locale strings to produce localized output for: https://faker.readthedocs.io/en/master/#localization |
| flattening_enabled | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth | False    | None    | The max depth to flatten schemas. |
| batch_config | False    | None    |             |
| batch_config.encoding | False    | None    | Specifies the format and compression of the batch files. |
| batch_config.encoding.format | False    | None    | Format to use for batch files. |
| batch_config.encoding.compression | False    | None    | Compression format to use for batch files. |
| batch_config.storage | False    | None    | Defines the storage layer to use when writing batch files |
| batch_config.storage.root | False    | None    | Root path to use when writing batch files. |
| batch_config.storage.prefix | False    | None    | Prefix to use when writing batch files. |

A full list of supported settings and capabilities is available by running: `tap-linkedin-ads --about`


### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization


The tap requires a LinkedInAds OAuth 2.0 access token to make API requests

The access token requires the following permissions:

`r_ads`: read ads
`rw_ads`: read-write ads
`r_ads_reporting`: read ads reporting

Access tokens expire after 60 days and require a user to manually authenticate
again. See the [LinkedInAds API docs](https://learn.microsoft.com/en-us/linkedin/shared/authentication/postman-getting-started) for more info.

## Usage

### AdAnalytics API Column Limitation

The AdAnalytics endpoint in the LinkedInAds API can call up to 20 columns at a time, we can create child classes which have 20 columns in them, we can merge their output with get records function.

### Elastic License 2.0

The licensor grants you a non-exclusive, royalty-free, worldwide, non-sublicensable, non-transferable license to use, copy, distribute, make available, and prepare derivative works of the software.

### Executing the Tap Directly

```bash
tap-linkedin-ads --version
tap-linkedin-ads --help
tap-linkedin-ads --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

This project uses parent-child streams. Learn more about them [here](https://gitlab.com/meltano/sdk/-/blob/main/docs/parent_streams.md).

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-linkedin-ads` CLI interface directly using `poetry run`:

```bash
poetry run tap-linkedin-ads --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-linkedin-ads
meltano install tap-linkedin-ads
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-linkedin-ads --version
# OR run a test `elt` pipeline:
meltano elt tap-linkedin-ads target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
