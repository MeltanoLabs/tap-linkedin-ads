# `tap-linkedin-sdk`

LinkedIn tap class.

Built with the [Meltano SDK](https://sdk.meltano.com) for Singer Taps and Targets.

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

```bash
pipx install git+https://github.com/MeltanoLabs/tap-linkedin-sdk.git@main
```

-->

## Configuration

### Accepted Config Options

This tap requires the following environmental variables to be set in ```.env```

- [ ] `TAP_LINKEDIN_ACCOUNTS:` linkedin account id
- [ ] `TAP_LINKEDIN_ACCESS_TOKEN:` linkedin access token
- [ ] `TAP_LINKEDIN_REFRESH_TOKEN:` refresh token
- [ ] `TAP_LINKEDIN_CLIENT_ID:` client id
- [ ] `TAP_LINKEDIN_OWNER:` owner id
- [ ] `TAP_LINKEDIN_CAMPAIGN:` campaign id
- [ ] `TAP_LINKEDIN_CLIENT_SECRET:` client secret

## Meltano Variables

The following config values need to be set in order to use with Meltano. These can be set in `meltano.yml`, via
```meltano config tap-linkedin set --interactive```, or via the env var mappings shown above.

- [ ] `account_id:` linkedin account id
- [ ] `access_token:` linkedin access token
- [ ] `refresh_token:` linkedin api refresh token
- [ ] `client_id:` client id
- [ ] `owner_id:` owner id
- [ ] `campaign_id:` campaign id
- [ ] `client_secret:` client secret
- [ ] `user_agent:` user agent
- [ ] `linkedin_version:` linkedin api version
- [ ] `start_date:` start date
- [ ] `end_date:` end_date

A full list of supported settings and capabilities is available by running: `tap-linkedin-sdk --about`


## Usage

### API Limitation - Pagination

The AdAnalytics endpoint in the LinkedIn API does not support pagination, and the response size is
limited to 15,000 elements. The only way to circumvent missing elements is to shorten the dateRange 
in a single ELT.

You can easily run `tap-linkedin-sdk` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-linkedin-sdk --version
tap-linkedin-sdk --help
tap-linkedin-sdk --config CONFIG --discover > ./catalog.json
```

## Contributing

This project uses parent-child streams. Learn more about them [here](https://gitlab.com/meltano/sdk/-/blob/main/docs/parent_streams.md).

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
``` 

### Create and Run Tests

Create tests within the `lib_tap_linkedin_sdk/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-linkedin-sdk` CLI interface directly using `poetry run`:

```bash
poetry run tap-linkedin --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-linkedin-sdk
meltano install tap-linkedin
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-linkedin --version
# OR run a test `elt` pipeline:
meltano elt tap-linkedin target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
